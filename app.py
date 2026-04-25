"""
app.py — Top Engagers API Backend
Matches frontend: top_engagers_tool.html

Endpoints:
  GET  /api/csrf-token          → {token}
  POST /api/engagers            → {cached, result} | {job_id, queued} | {ip_rate_limited}
  GET  /api/job/<job_id>        → {status, result|error, queue_position, queue_eta_minutes}

Architecture:
  - Max 50 workers (pool). Each profile gets exactly 3 reply-workers.
  - Accounts DBs (accounts1.db…accounts50.db) assigned round-robin per worker thread.
  - If >50 workers needed, new jobs wait in a queue (max 5 min timeout).
  - IP rate limit: max 3 concurrent twscrape-bound fetches per real IP (X-Forwarded-For).
  - DB is free (no IP limit for reads).
  - Tweets limited to 50 (as requested).

DB schema matches scrape_repliers.py exactly:
  tweets, replies, top_repliers (no users table).
  Cache lookup is done via top_repliers (owner_username column).
  Main user pfp is served from the separate pfp table (see build_pfp_table.py).
"""

import json
import logging
import os
import secrets
import sqlite3
import subprocess
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import urllib.request as _urllib_req

from flask import Flask, jsonify, request, session, Response
from flask_limiter import Limiter

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("engagers_api")

# ── Config ─────────────────────────────────────────────────────────────────────

OUTPUT_DB    = Path(os.environ.get("REPLIERS_DB",  "repliers.db"))
OUTPUT_DB2   = Path(os.environ.get("REPLIERS_DB2", "repliers2.db"))
TWEET_LIMIT  = 50
TOTAL_WORKERS = 50
REPLY_WORKERS_PER_PROFILE = 3
MAX_IP_CONCURRENT = 3
JOB_TIMEOUT  = 300          # 5 min
ACCOUNTS_DBS = [f"accounts{i}.db" for i in range(1, 51)]
OVERLOADED_USERS_FILE = Path(os.environ.get("OVERLOADED_USERS_FILE", "overloaded_users.txt"))
_overloaded_file_lock = threading.Lock()

# ── Flask app ──────────────────────────────────────────────────────────────────

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"]   = True

# ── Real IP helper (behind nginx) ─────────────────────────────────────────────

def get_real_ip() -> str:
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        for part in xff.split(","):
            ip = part.strip()
            if ip:
                return ip
    return request.remote_addr or "unknown"

# ── CSRF ───────────────────────────────────────────────────────────────────────

_csrf_tokens: dict[str, float] = {}
_csrf_lock = threading.Lock()

def _csrf_clean():
    now = time.time()
    with _csrf_lock:
        expired = [t for t, exp in _csrf_tokens.items() if now > exp]
        for t in expired:
            del _csrf_tokens[t]

def issue_csrf() -> str:
    token = secrets.token_hex(24)
    with _csrf_lock:
        _csrf_tokens[token] = time.time() + 3600
    return token

def validate_csrf(token: str) -> bool:
    if not token:
        return False
    with _csrf_lock:
        exp = _csrf_tokens.get(token)
        if exp and time.time() < exp:
            return True
    return False

# ── IP rate limiting ───────────────────────────────────────────────────────────

_ip_active: dict[str, int] = defaultdict(int)
_ip_lock = threading.Lock()

def _ip_can_fetch(ip: str) -> bool:
    with _ip_lock:
        return _ip_active[ip] < MAX_IP_CONCURRENT

def _ip_acquire(ip: str) -> bool:
    with _ip_lock:
        if _ip_active[ip] >= MAX_IP_CONCURRENT:
            return False
        _ip_active[ip] += 1
        return True

def _ip_release(ip: str):
    with _ip_lock:
        if _ip_active[ip] > 0:
            _ip_active[ip] -= 1

# ── Overloaded users log ───────────────────────────────────────────────────────

def _save_overloaded_user(username: str):
    """Append username to overloaded_users.txt (one per line, no duplicates within a run)."""
    with _overloaded_file_lock:
        try:
            # Read existing usernames to avoid duplicates
            existing: set[str] = set()
            if OVERLOADED_USERS_FILE.exists():
                existing = {
                    line.strip().lower()
                    for line in OVERLOADED_USERS_FILE.read_text().splitlines()
                    if line.strip()
                }
            if username.lower() not in existing:
                with OVERLOADED_USERS_FILE.open("a") as f:
                    f.write(username + "\n")
                log.info(f"[overloaded] saved @{username} to {OVERLOADED_USERS_FILE}")
        except Exception as e:
            log.warning(f"[overloaded] failed to save @{username}: {e}")

# ── Worker pool ────────────────────────────────────────────────────────────────

_executor = ThreadPoolExecutor(max_workers=TOTAL_WORKERS, thread_name_prefix="eng")
_active_slots = 0
_slots_lock   = threading.Lock()
MAX_USABLE_SLOTS = TOTAL_WORKERS

def _slots_available(n: int) -> bool:
    with _slots_lock:
        return (_active_slots + n) <= MAX_USABLE_SLOTS

def _acquire_slots(n: int) -> bool:
    global _active_slots
    with _slots_lock:
        if (_active_slots + n) > MAX_USABLE_SLOTS:
            return False
        _active_slots += n
        return True

def _release_slots(n: int):
    global _active_slots
    with _slots_lock:
        _active_slots = max(0, _active_slots - n)

# ── Job store ──────────────────────────────────────────────────────────────────

class Job:
    def __init__(self, job_id: str, username: str, ip: str):
        self.job_id   = job_id
        self.username = username
        self.ip       = ip
        self.status   = "queued"
        self.result   = None
        self.error    = None
        self.created  = time.time()
        self.lock     = threading.Lock()

_jobs: dict[str, Job] = {}
_jobs_lock = threading.Lock()
_queue: list[Job] = []
_queue_lock = threading.Lock()

def _enqueue(job: Job):
    with _queue_lock:
        _queue.append(job)
    log.info(f"[queue] job {job.job_id} ({job.username}) enqueued — queue depth={len(_queue)}")

def _dequeue_next() -> Job | None:
    with _queue_lock:
        while _queue:
            job = _queue[0]
            if time.time() - job.created > JOB_TIMEOUT:
                _queue.pop(0)
                with job.lock:
                    job.status = "error"
                    job.error  = "Timed out waiting in queue."
                log.warning(f"[queue] job {job.job_id} timed out in queue")
                continue
            if _acquire_slots(1 + REPLY_WORKERS_PER_PROFILE):
                return _queue.pop(0)
            break
    return None

def _queue_drainer():
    while True:
        job = _dequeue_next()
        if job:
            job.status = "running"
            _executor.submit(_run_job, job)
        time.sleep(1)

threading.Thread(target=_queue_drainer, daemon=True, name="queue-drainer").start()

# ── DB round-robin per thread ──────────────────────────────────────────────────

_thread_local     = threading.local()
_db_claim_counter = 0
_db_claim_lock    = threading.Lock()

def my_db() -> str:
    if not hasattr(_thread_local, "db"):
        global _db_claim_counter
        with _db_claim_lock:
            slot = _db_claim_counter % len(ACCOUNTS_DBS)
            _db_claim_counter += 1
        _thread_local.db = ACCOUNTS_DBS[slot]
    return _thread_local.db

# ── Output DB — schema identical to scrape_repliers.py ───────────────────────

_db_write_lock = threading.Lock()

DDL = """
-- ── tweets ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tweets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id        TEXT NOT NULL,
    owner_username  TEXT,
    tweet_id        TEXT NOT NULL,
    tweet_url       TEXT,
    raw_content     TEXT,
    lang            TEXT,
    likes           INTEGER,
    retweets        INTEGER,
    replies         INTEGER,
    quotes          INTEGER,
    bookmarks       INTEGER,
    views           INTEGER,
    posted_date     TEXT,
    posted_time     TEXT,
    scraped_at      TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tweet ON tweets(tweet_id);

-- ── replies ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS replies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_tweet_id TEXT NOT NULL,
    source_owner_id TEXT NOT NULL,
    reply_id        TEXT NOT NULL,
    reply_url       TEXT,
    reply_content   TEXT,
    reply_lang      TEXT,
    reply_date      TEXT,
    reply_time      TEXT,
    replier_id      TEXT NOT NULL,
    replier_username TEXT,
    replier_name    TEXT,
    replier_pfp     TEXT,
    likes           INTEGER,
    retweets        INTEGER,
    replies_count   INTEGER,
    views           INTEGER,
    scraped_at      TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_reply ON replies(reply_id);
CREATE INDEX IF NOT EXISTS ix_replies_owner ON replies(source_owner_id);

-- ── top_repliers ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS top_repliers (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id            TEXT NOT NULL,
    owner_username      TEXT,
    rank                INTEGER NOT NULL,
    replier_id          TEXT NOT NULL,
    replier_username    TEXT,
    replier_name        TEXT,
    replier_pfp         TEXT,
    reply_count         INTEGER NOT NULL,
    earliest_reply_date TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_top ON top_repliers(owner_id, rank);

-- ── pfp — main-user profile pictures (populated by build_pfp_table.py) ───────
CREATE TABLE IF NOT EXISTS pfp (
    twitter_id   TEXT PRIMARY KEY,
    username     TEXT,
    display_name TEXT,
    pfp_url      TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
-- Add display_name column if it doesn't exist yet (migration for existing DBs)
-- handled in get_conn() below
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(OUTPUT_DB), check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.executescript(DDL)
    # Migration: add display_name to pfp table if it doesn't exist yet
    try:
        conn.execute("ALTER TABLE pfp ADD COLUMN display_name TEXT")
        conn.commit()
    except Exception:
        pass  # Column already exists
    conn.commit()
    return conn


_main_conn: sqlite3.Connection | None = None
_main_conn_lock = threading.Lock()


def db() -> sqlite3.Connection:
    global _main_conn
    with _main_conn_lock:
        if _main_conn is None:
            _main_conn = get_conn()
        return _main_conn


# ── repliers2.db connection (top_repliers + pfp from scrape_overloaded.py) ────

def _open_repliers2() -> sqlite3.Connection | None:
    """Open repliers2.db if it exists. Returns None silently if not present."""
    if not OUTPUT_DB2.exists():
        return None
    try:
        conn = sqlite3.connect(str(OUTPUT_DB2), check_same_thread=False, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn
    except Exception as e:
        log.warning(f"[db2] could not open {OUTPUT_DB2}: {e}")
        return None

_db2_conn: sqlite3.Connection | None = None
_db2_conn_lock = threading.Lock()

def db2() -> sqlite3.Connection | None:
    """Return cached repliers2.db connection, or open it if not yet opened.
    Does NOT cache None — retries on every call until the file appears."""
    global _db2_conn
    with _db2_conn_lock:
        if _db2_conn is None:
            _db2_conn = _open_repliers2()
            if _db2_conn:
                log.info(f"[db2] opened {OUTPUT_DB2}")
            else:
                log.debug(f"[db2] {OUTPUT_DB2} not available yet")
        return _db2_conn


def get_cached_owner_db2(username: str) -> dict | None:
    """Look up owner in repliers2.db. Tries top_repliers first, then pfp table."""
    conn = db2()
    if conn is None:
        return None
    try:
        # ── Try top_repliers.owner_username ──────────────────────────────────
        row = conn.execute("""
            SELECT owner_id, owner_username
            FROM top_repliers
            WHERE LOWER(owner_username) = LOWER(?)
            LIMIT 1
        """, (username,)).fetchone()

        if not row:
            # ── Fallback: pfp.username (scrape_overloaded saves here too) ────
            pfp_only = conn.execute("""
                SELECT twitter_id, username, display_name, pfp_url
                FROM pfp
                WHERE LOWER(username) = LOWER(?)
                LIMIT 1
            """, (username,)).fetchone()
            if not pfp_only:
                return None
            return {
                "owner_id":       pfp_only[0],
                "owner_username": pfp_only[1],
                "display_name":   pfp_only[2] or pfp_only[1],
                "pfp_url":        pfp_only[3] or "",
            }

        owner_id, owner_username = row
        pfp_row = conn.execute(
            "SELECT pfp_url, display_name FROM pfp WHERE twitter_id=?", (owner_id,)
        ).fetchone()
        return {
            "owner_id":       owner_id,
            "owner_username": owner_username,
            "display_name":   pfp_row[1] if pfp_row and pfp_row[1] else owner_username,
            "pfp_url":        pfp_row[0] if pfp_row else "",
        }
    except Exception as e:
        log.warning(f"[db2] get_cached_owner_db2 error for @{username}: {e}")
        return None


def get_top_repliers_db2(owner_id: str) -> list[dict]:
    """Same as get_top_repliers but queries repliers2.db."""
    conn = db2()
    if conn is None:
        return []
    try:
        rows = conn.execute("""
            SELECT rank, replier_id, replier_username, replier_name, replier_pfp,
                   reply_count, earliest_reply_date
            FROM top_repliers WHERE owner_id=? ORDER BY rank
        """, (owner_id,)).fetchall()
        return [dict(zip([
            "rank", "replier_id", "replier_username", "replier_name", "replier_pfp",
            "reply_count", "earliest_reply_date",
        ], r)) for r in rows]
    except Exception as e:
        log.warning(f"[db2] get_top_repliers_db2 error for owner {owner_id}: {e}")
        return []


# ── twscrape wrapper ──────────────────────────────────────────────────────────

# NOTE: twscrape calls are disabled. Live scraping is not available.
# All requests that are not served from the DB cache will return a load error.
def run_twscrape(args: list[str], accounts_db: str, timeout: int = 120) -> str:
    raise RuntimeError("twscrape disabled")
    # result = subprocess.run(
    #     ["twscrape", "--db", accounts_db] + args,
    #     capture_output=True, text=True, timeout=timeout,
    # )
    # if result.returncode != 0:
    #     raise RuntimeError(result.stderr.strip() or "twscrape non-zero exit")
    # return result.stdout.strip()


# ── datetime parser ──────────────────────────────────────────────────────────

def _parse_dt(s: str) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


# ── DB helpers ─────────────────────────────────────────────────────────────────

def insert_tweets(conn: sqlite3.Connection, rows: list[dict]):
    sql = """
    INSERT OR IGNORE INTO tweets
        (owner_id, owner_username, tweet_id, tweet_url, raw_content, lang,
         likes, retweets, replies, quotes, bookmarks, views,
         posted_date, posted_time, scraped_at)
    VALUES
        (:owner_id, :owner_username, :tweet_id, :tweet_url, :raw_content, :lang,
         :likes, :retweets, :replies, :quotes, :bookmarks, :views,
         :posted_date, :posted_time, :scraped_at)
    """
    with _db_write_lock:
        conn.executemany(sql, rows)
        conn.commit()


def insert_replies(conn: sqlite3.Connection, rows: list[dict]):
    sql = """
    INSERT OR IGNORE INTO replies
        (source_tweet_id, source_owner_id,
         reply_id, reply_url, reply_content, reply_lang, reply_date, reply_time,
         replier_id, replier_username, replier_name, replier_pfp,
         likes, retweets, replies_count, views, scraped_at)
    VALUES
        (:source_tweet_id, :source_owner_id,
         :reply_id, :reply_url, :reply_content, :reply_lang, :reply_date, :reply_time,
         :replier_id, :replier_username, :replier_name, :replier_pfp,
         :likes, :retweets, :replies_count, :views, :scraped_at)
    """
    with _db_write_lock:
        conn.executemany(sql, rows)
        conn.commit()


def upsert_top_repliers(conn: sqlite3.Connection, owner_id: str, owner_username: str,
                        ranked: list[dict]):
    sql = """
    INSERT OR REPLACE INTO top_repliers
        (owner_id, owner_username, rank,
         replier_id, replier_username, replier_name, replier_pfp,
         reply_count, earliest_reply_date)
    VALUES
        (:owner_id, :owner_username, :rank,
         :replier_id, :replier_username, :replier_name, :replier_pfp,
         :reply_count, :earliest_reply_date)
    """
    rows = [{"owner_id": owner_id, "owner_username": owner_username, **r} for r in ranked]
    with _db_write_lock:
        conn.executemany(sql, rows)
        conn.commit()


def get_top_repliers(conn: sqlite3.Connection, owner_id: str) -> list[dict]:
    rows = conn.execute("""
        SELECT rank, replier_id, replier_username, replier_name, replier_pfp,
               reply_count, earliest_reply_date
        FROM top_repliers WHERE owner_id=? ORDER BY rank
    """, (owner_id,)).fetchall()
    return [dict(zip([
        "rank", "replier_id", "replier_username", "replier_name", "replier_pfp",
        "reply_count", "earliest_reply_date",
    ], r)) for r in rows]


def get_cached_owner(conn: sqlite3.Connection, username: str) -> dict | None:
    """
    Cache lookup via top_repliers table (same approach as scrape_repliers).
    Returns {owner_id, owner_username, display_name, pfp_url} if a cached entry exists, else None.
    """
    row = conn.execute("""
        SELECT owner_id, owner_username
        FROM top_repliers
        WHERE LOWER(owner_username) = LOWER(?)
        LIMIT 1
    """, (username,)).fetchone()
    if not row:
        return None
    owner_id, owner_username = row
    pfp_row = conn.execute(
        "SELECT pfp_url, display_name FROM pfp WHERE twitter_id=?", (owner_id,)
    ).fetchone()
    return {
        "owner_id":       owner_id,
        "owner_username": owner_username,
        "display_name":   pfp_row[1] if pfp_row and pfp_row[1] else owner_username,
        "pfp_url":        pfp_row[0] if pfp_row else "",
    }


def get_pfp(conn: sqlite3.Connection, twitter_id: str) -> tuple[str, str]:
    """Look up main user pfp and display_name from pfp table. Returns (pfp_url, display_name)."""
    row = conn.execute(
        "SELECT pfp_url, display_name FROM pfp WHERE twitter_id=?", (twitter_id,)
    ).fetchone()
    if row:
        return row[0] or "", row[1] or ""
    return "", ""


def lookup_cache(username: str) -> tuple[dict | None, list[dict]]:
    """Check repliers.db first, then repliers2.db. Returns (cached_owner, top_repliers)."""
    conn = db()

    # ── Primary: repliers.db ──────────────────────────────────────────────────
    cached = get_cached_owner(conn, username)
    if cached:
        top = get_top_repliers(conn, cached["owner_id"])
        if top:
            log.info(f"[cache] hit in repliers.db for @{username}")
            return cached, top

    # ── Fallback: repliers2.db ────────────────────────────────────────────────
    cached2 = get_cached_owner_db2(username)
    if cached2:
        top2 = get_top_repliers_db2(cached2["owner_id"])
        if top2:
            log.info(f"[cache] hit in repliers2.db for @{username}")
            return cached2, top2

    return None, []


# ── Resolve username → (twitter_id, username, meta) ──────────────────────────

def resolve_username(username: str, accounts_db: str) -> tuple[str | None, str | None, dict | None]:
    """Call twscrape user_by_login and return (twitter_id, username, meta_dict)."""
    try:
        raw = run_twscrape(["user_by_login", username], accounts_db=accounts_db, timeout=60)
    except Exception as e:
        log.warning(f"[resolve] user_by_login '{username}' failed: {e}")
        return None, None, None

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        uid = (obj.get("id_str") or str(obj.get("id", ""))).strip()
        if not uid:
            continue
        uname = obj.get("username", username)
        meta = {
            "twitter_id":   uid,
            "username":     uname,
            "display_name": obj.get("displayname") or obj.get("name") or uname,
            "pfp_url":      obj.get("profileImageUrl", ""),
            "followers":    obj.get("followersCount", 0),
            "following":    obj.get("friendsCount", 0),
        }
        log.info(f"[resolve] @{username} -> id={uid} (@{uname})")
        return uid, uname, meta
    log.warning(f"[resolve] no user found for '{username}'")
    return None, None, None


# ── Fetch tweets (Phase 1) ────────────────────────────────────────────────────

def fetch_qualifying_tweets(twitter_id: str, username: str, accounts_db: str) -> list[dict]:
    log.info(f"[tweets] fetching up to {TWEET_LIMIT} tweets for id={twitter_id} db={accounts_db}")
    try:
        raw = run_twscrape(
            ["user_tweets", twitter_id, "--limit", str(TWEET_LIMIT)],
            accounts_db=accounts_db, timeout=120,
        )
    except Exception as e:
        log.warning(f"[tweets] fetch failed for {twitter_id}: {e}")
        return []

    scraped_at = datetime.now(timezone.utc).isoformat()
    seen: set[str] = set()
    qualifying: list[dict] = []
    resolved_username = username
    pinned_ids: set[str] = set()

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            tweet = json.loads(line)
        except json.JSONDecodeError:
            continue

        user_obj = tweet.get("user") or {}
        tweet_user_id = str(user_obj.get("id", ""))
        if tweet_user_id != twitter_id:
            continue

        if not pinned_ids:
            pinned_ids = {str(pid) for pid in (user_obj.get("pinnedIds") or [])}
            if pinned_ids:
                log.info(f"[tweets] ignoring pinned tweet(s): {pinned_ids}")

        tid = tweet.get("id_str") or str(tweet.get("id", ""))
        if not tid or tid in seen:
            continue
        seen.add(tid)

        if tid in pinned_ids:
            log.info(f"[tweets] skipping pinned tweet {tid}")
            continue

        uname = user_obj.get("username", "")
        if uname:
            resolved_username = uname

        if tweet.get("retweetedTweet"):
            continue

        in_reply_to_user = tweet.get("inReplyToUser")
        if in_reply_to_user:
            reply_target_id = str(in_reply_to_user.get("id", ""))
            if reply_target_id and reply_target_id != twitter_id:
                continue

        dt = _parse_dt(tweet.get("date", ""))
        qualifying.append({
            "owner_id":       twitter_id,
            "owner_username": resolved_username,
            "tweet_id":       tid,
            "tweet_url":      tweet.get("url", ""),
            "raw_content":    tweet.get("rawContent", ""),
            "lang":           tweet.get("lang"),
            "likes":          tweet.get("likeCount"),
            "retweets":       tweet.get("retweetCount"),
            "replies":        tweet.get("replyCount"),
            "quotes":         tweet.get("quoteCount"),
            "bookmarks":      tweet.get("bookmarkedCount"),
            "views":          tweet.get("viewCount"),
            "posted_date":    dt.strftime("%Y-%m-%d") if dt else None,
            "posted_time":    dt.strftime("%H:%M:%S") if dt else None,
            "scraped_at":     scraped_at,
        })

    log.info(f"[tweets] @{resolved_username}: {len(qualifying)} qualifying tweets "
             f"(from {len(seen)} total, {len(seen)-len(qualifying)} filtered)")
    return qualifying


# ── Fetch replies for one tweet (Phase 2) ────────────────────────────────────

def fetch_replies_for_tweet(tweet_id: str, owner_id: str, accounts_db: str) -> list[dict]:
    try:
        raw = run_twscrape(["tweet_replies", tweet_id], accounts_db=accounts_db, timeout=300)
    except Exception as e:
        log.warning(f"[replies] tweet_replies {tweet_id} failed: {e}")
        return []

    scraped_at = datetime.now(timezone.utc).isoformat()
    seen: set[str] = set()
    rows: list[dict] = []

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            reply = json.loads(line)
        except json.JSONDecodeError:
            continue

        rid = reply.get("id_str") or str(reply.get("id", ""))
        if not rid or rid in seen:
            continue
        seen.add(rid)

        if str(reply.get("conversationId", "")) != tweet_id:
            continue
        if not reply.get("inReplyToTweetId"):
            continue

        user = reply.get("user") or {}
        replier_id = str(user.get("id", ""))
        if not replier_id:
            continue

        # Skip tweet-owner self-replies
        if replier_id == owner_id:
            continue

        dt = _parse_dt(reply.get("date", ""))
        rows.append({
            "source_tweet_id":  tweet_id,
            "source_owner_id":  owner_id,
            "reply_id":         rid,
            "reply_url":        reply.get("url", ""),
            "reply_content":    reply.get("rawContent", ""),
            "reply_lang":       reply.get("lang"),
            "reply_date":       dt.strftime("%Y-%m-%d") if dt else None,
            "reply_time":       dt.strftime("%H:%M:%S") if dt else None,
            "replier_id":       replier_id,
            "replier_username": user.get("username"),
            "replier_name":     user.get("displayname"),
            "replier_pfp":      user.get("profileImageUrl"),
            "likes":            reply.get("likeCount"),
            "retweets":         reply.get("retweetCount"),
            "replies_count":    reply.get("replyCount"),
            "views":            reply.get("viewCount"),
            "scraped_at":       scraped_at,
        })

    return rows


# ── Compute top repliers (Phase 3) ────────────────────────────────────────────

def compute_top_repliers(conn: sqlite3.Connection, owner_id: str,
                         owner_username: str) -> list[dict]:
    with _db_write_lock:
        bottom5_rows = conn.execute("""
            SELECT source_tweet_id
            FROM replies
            WHERE source_owner_id = ?
              AND replier_id != ?
            GROUP BY source_tweet_id
            ORDER BY COUNT(*) ASC
            LIMIT 5
        """, (owner_id, owner_id)).fetchall()
        bottom5_ids = {r[0] for r in bottom5_rows}

        count_rows = conn.execute("""
            SELECT replier_id, replier_username, replier_name, replier_pfp,
                   COUNT(*) AS reply_count
            FROM replies
            WHERE source_owner_id = ?
              AND replier_id != ?
            GROUP BY replier_id
        """, (owner_id, owner_id)).fetchall()

        if bottom5_ids:
            coverage_rows = conn.execute("""
                SELECT replier_id,
                       COUNT(DISTINCT source_tweet_id) AS covered,
                       MIN(reply_date || 'T' || COALESCE(reply_time, '00:00:00')) AS earliest
                FROM replies
                WHERE source_owner_id = ?
                  AND replier_id != ?
                  AND source_tweet_id IN ({})
                GROUP BY replier_id
            """.format(",".join("?" * len(bottom5_ids))),
                (owner_id, owner_id, *bottom5_ids)
            ).fetchall()
        else:
            coverage_rows = []

    if not count_rows:
        return []

    coverage_map: dict[str, tuple[int, str]] = {
        r[0]: (r[1], r[2] or "9999") for r in coverage_rows
    }

    repliers = {}
    for row in count_rows:
        rid = row[0]
        covered, earliest = coverage_map.get(rid, (0, "9999"))
        repliers[rid] = {
            "replier_id":           rid,
            "replier_username":     row[1],
            "replier_name":         row[2],
            "replier_pfp":          row[3],
            "reply_count":          row[4],
            "low_traffic_coverage": covered,
            "earliest_low_traffic": earliest,
        }

    sorted_repliers = sorted(
        repliers.values(),
        key=lambda r: (
            -r["reply_count"],
            -r["low_traffic_coverage"],
             r["earliest_low_traffic"],
        )
    )

    ranked = []
    for rank, r in enumerate(sorted_repliers[:5], start=1):
        ranked.append({
            "rank":                rank,
            "replier_id":          r["replier_id"],
            "replier_username":    r["replier_username"],
            "replier_name":        r["replier_name"],
            "replier_pfp":         r["replier_pfp"],
            "reply_count":         r["reply_count"],
            "earliest_reply_date": r["earliest_low_traffic"] if r["earliest_low_traffic"] != "9999" else None,
        })
    return ranked


# ── Build result payload ──────────────────────────────────────────────────────

def build_result(owner_id: str, owner_username: str, display_name: str, pfp_url: str,
                 ranked: list[dict]) -> dict:
    return {
        "username":    owner_username,
        "displayName": display_name or owner_username,
        "pfp":         pfp_url,
        "topRepliers": [
            {
                "rank":          r["rank"],
                "username":      r["replier_username"],
                "displayName":   r["replier_name"] or r["replier_username"],
                "pfp":           r["replier_pfp"] or "",
                "replyCount":    r["reply_count"],
                "earliestReply": r["earliest_reply_date"],
            }
            for r in ranked
        ],
    }


# ── Job runner ────────────────────────────────────────────────────────────────

def _run_job(job: Job):
    conn     = db()
    username = job.username
    ip       = job.ip

    log.info(f"[job:{job.job_id}] start for @{username} ip={ip}")

    try:
        # ── Check both DB caches ───────────────────────────────────────────────
        cached, top = lookup_cache(username)
        if cached and top:
            log.info(f"[job:{job.job_id}] served from DB cache for @{username}")
            result = build_result(
                cached["owner_id"], cached["owner_username"],
                cached["display_name"], cached["pfp_url"], top
            )
            with job.lock:
                job.status = "done"
                job.result = result
            _release_slots(1 + REPLY_WORKERS_PER_PROFILE)
            return

        # ── Not in DB cache — live scraping disabled ──────────────────────────
        # twscrape calls are commented out. Return a friendly load error.
        log.info(f"[job:{job.job_id}] @{username} not in DB cache; live scraping disabled")
        _save_overloaded_user(username)
        with job.lock:
            job.status = "error"
            job.error  = "Site is under heavy load. Please try again after a few hours."
        _release_slots(1 + REPLY_WORKERS_PER_PROFILE)
        return

        # ── Needs twscrape — check IP rate limit ──────────────────────────────
        # if not _ip_acquire(ip):
        #     log.warning(f"[job:{job.job_id}] IP {ip} rate-limited")
        #     with job.lock:
        #         job.status = "error"
        #         job.error  = "Too many requests from your IP. Please wait a bit and try again."
        #     _release_slots(1 + REPLY_WORKERS_PER_PROFILE)
        #     return

        # try:
        #     # ── Phase 1: resolve + fetch tweets ───────────────────────────────
        #     accounts_db = my_db()
        #     twitter_id, resolved_uname, meta = resolve_username(username, accounts_db)
        #     if not twitter_id:
        #         raise ValueError(f"User @{username} not found on Twitter.")

        #     tweet_rows = fetch_qualifying_tweets(twitter_id, resolved_uname, accounts_db)
        #     if not tweet_rows:
        #         raise ValueError(f"No qualifying tweets found for @{resolved_uname}.")
        #     insert_tweets(conn, tweet_rows)

        #     tweet_ids = [r["tweet_id"] for r in tweet_rows]
        #     log.info(f"[job:{job.job_id}] phase1 done — {len(tweet_ids)} tweets")

        #     # ── Phase 2: fetch replies (3 workers, split tweets between them) ──
        #     chunks: list[list[str]] = [[] for _ in range(REPLY_WORKERS_PER_PROFILE)]
        #     for i, tid in enumerate(tweet_ids):
        #         chunks[i % REPLY_WORKERS_PER_PROFILE].append(tid)

        #     def worker_fetch_chunk(chunk: list[str]):
        #         adb = my_db()
        #         for tid in chunk:
        #             rows = fetch_replies_for_tweet(tid, twitter_id, adb)
        #             if rows:
        #                 insert_replies(conn, rows)
        #                 log.info(f"[job:{job.job_id}] tweet {tid}: {len(rows)} replies")

        #     futures: list[Future] = []
        #     for chunk in chunks:
        #         if chunk:
        #             futures.append(_executor.submit(worker_fetch_chunk, chunk))
        #     for f in futures:
        #         try:
        #             f.result(timeout=JOB_TIMEOUT)
        #         except Exception as e:
        #             log.warning(f"[job:{job.job_id}] reply worker error: {e}")

        #     log.info(f"[job:{job.job_id}] phase2 done")

        #     # ── Phase 3: compute top repliers ──────────────────────────────────
        #     ranked = compute_top_repliers(conn, twitter_id, resolved_uname)
        #     if not ranked:
        #         raise ValueError(f"No repliers found for @{resolved_uname}.")
        #     upsert_top_repliers(conn, twitter_id, resolved_uname, ranked)

        #     # Get pfp from pfp table; fall back to what resolve gave us
        #     # Also save display_name to pfp table for new users
        #     pfp_url_from_table, display_name_from_table = get_pfp(conn, twitter_id)
        #     pfp_url = pfp_url_from_table or meta.get("pfp_url", "")
        #     display_name = display_name_from_table or meta.get("display_name", "") or resolved_uname

        #     # Upsert display_name and pfp_url for this user into pfp table
        #     now_iso = datetime.now(timezone.utc).isoformat()
        #     with _db_write_lock:
        #         conn.execute("""
        #             INSERT INTO pfp (twitter_id, username, display_name, pfp_url, updated_at)
        #             VALUES (?, ?, ?, ?, ?)
        #             ON CONFLICT(twitter_id) DO UPDATE SET
        #                 username=excluded.username,
        #                 display_name=COALESCE(excluded.display_name, display_name),
        #                 pfp_url=COALESCE(excluded.pfp_url, pfp_url),
        #                 updated_at=excluded.updated_at
        #         """, (twitter_id, resolved_uname, display_name, pfp_url, now_iso))
        #         conn.commit()

        #     result = build_result(twitter_id, resolved_uname, display_name, pfp_url, ranked)

        #     with job.lock:
        #         job.status = "done"
        #         job.result = result
        #     log.info(f"[job:{job.job_id}] complete for @{resolved_uname}")

        # finally:
        #     _ip_release(ip)

    except Exception as e:
        log.exception(f"[job:{job.job_id}] failed: {e}")
        with job.lock:
            job.status = "error"
            job.error  = str(e)
    finally:
        _release_slots(1 + REPLY_WORKERS_PER_PROFILE)


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/csrf-token", methods=["GET"])
def csrf_token_route():
    _csrf_clean()
    return jsonify({"token": issue_csrf()})


@app.route("/api/engagers", methods=["POST"])
def engagers_route():
    token = request.headers.get("X-CSRF-Token", "")
    if not validate_csrf(token):
        return jsonify({"error": "Invalid or missing CSRF token."}), 403

    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip().lstrip("@")
    if not username or len(username) > 50:
        return jsonify({"error": "Invalid username."}), 400

    ip   = get_real_ip()
    conn = db()

    # ── Check DB cache: repliers.db then repliers2.db ─────────────────────────
    cached, top = lookup_cache(username)
    if cached and top:
        log.info(f"[cache] instant hit for @{username}")
        result = build_result(
            cached["owner_id"], cached["owner_username"],
            cached["display_name"], cached["pfp_url"], top
        )
        return jsonify({"cached": True, "result": result})

    # ── IP rate check (only if twscrape needed — disabled while scraping is off) ─
    # if not _ip_can_fetch(ip):
    #     return jsonify({"ip_rate_limited": True}), 200

    # ── Create job ─────────────────────────────────────────────────────────────
    job_id = str(uuid.uuid4())
    job    = Job(job_id, username, ip)
    with _jobs_lock:
        _jobs[job_id] = job

    slots_needed = 1 + REPLY_WORKERS_PER_PROFILE
    if _acquire_slots(slots_needed):
        job.status = "running"
        _executor.submit(_run_job, job)
        log.info(f"[api] job {job_id} started immediately for @{username}")
        return jsonify({"job_id": job_id, "queued": False})
    else:
        _enqueue(job)
        with _queue_lock:
            pos = _queue.index(job) + 1 if job in _queue else 1
        eta = max(1, pos * 2)
        log.info(f"[api] job {job_id} queued (pos={pos}) for @{username}")
        return jsonify({
            "job_id":             job_id,
            "queued":             True,
            "queue_position":     pos,
            "queue_eta_minutes":  eta,
        })


@app.route("/api/job/<job_id>", methods=["GET"])
def job_status_route(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({"status": "error", "error": "Job not found."}), 404

    with job.lock:
        status = job.status
        result = job.result
        error  = job.error

    if status == "queued":
        with _queue_lock:
            try:
                pos = _queue.index(job) + 1
            except ValueError:
                pos = 0
        eta = max(1, pos * 2)
        return jsonify({
            "status":            "queued",
            "queue_position":    pos,
            "queue_eta_minutes": eta,
        })

    if status == "running":
        return jsonify({"status": "running"})

    if status == "done":
        return jsonify({"status": "done", "result": result})

    return jsonify({"status": "error", "error": error or "Unknown error."})


@app.route("/api/imgproxy")
def img_proxy():
    """Proxy Twitter profile images to avoid CORS issues with html2canvas."""
    url = request.args.get("url", "")
    if not url or not url.startswith("https://pbs.twimg.com/"):
        return "", 400
    try:
        req = _urllib_req.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with _urllib_req.urlopen(req, timeout=8) as resp:
            data = resp.read()
            ct = resp.headers.get("Content-Type", "image/jpeg")
        return Response(data, content_type=ct, headers={
            "Cache-Control": "public, max-age=86400",
            "Access-Control-Allow-Origin": "*",
        })
    except Exception as e:
        log.warning(f"[imgproxy] failed for {url}: {e}")
        return "", 502


@app.route("/")
def index():
    return app.send_static_file("index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5236, debug=False, threaded=True)