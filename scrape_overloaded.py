"""
scrape_overloaded.py — Batch scraper for users in overloaded_users.txt

Architecture:
  - Reads all usernames from overloaded_users.txt
  - 100 workers, each pinned to one of accounts1–25.db (round-robin, so each db
    serves ~4 workers). Workers are reused across all phases.
  - repliers1.db  →  tweets + replies tables
  - repliers2.db  →  top_repliers + pfp tables

Phase order (all 100 workers fully engaged in each phase before moving to next):
  1. RESOLVE   — user_by_login → save pfp to repliers2.db  [skip if already in pfp]
  2. TWEETS    — user_tweets   → save to repliers1.db       [skip if already in tweets]
  3. REPLIES   — tweet_replies for every tweet id  → save to repliers1.db
  4. TOP       — compute top_repliers from repliers1.db → save to repliers2.db

Timeouts & retries:
  - resolve:  60s, 1 retry on timeout
  - tweets:  300s, 1 retry on timeout
  - replies: 300s, 1 retry on timeout
  (Only subprocess.TimeoutExpired triggers a retry; other errors fail immediately)

Usage:
  python scrape_overloaded.py [--users overloaded_users.txt]
                              [--accounts-dbs 25]
                              [--workers 100]
                              [--tweet-limit 50]
                              [--top-n 5]
"""

import argparse
import json
import logging
import sqlite3
import subprocess
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scrape_overloaded")

# ── CLI args ───────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser()
parser.add_argument("--users",        default="overloaded_users.txt")
parser.add_argument("--accounts-dbs", type=int, default=25,
                    help="Number of accounts DBs (accounts1.db … accountsN.db)")
parser.add_argument("--workers",      type=int, default=100)
parser.add_argument("--tweet-limit",  type=int, default=50)
parser.add_argument("--top-n",        type=int, default=5)
parser.add_argument("--repliers1",    default="repliers1.db")
parser.add_argument("--repliers2",    default="repliers2.db")
args = parser.parse_args()

USERS_FILE    = Path(args.users)
N_ACCT_DBS    = args.accounts_dbs
N_WORKERS     = args.workers
TWEET_LIMIT   = args.tweet_limit
TOP_N         = args.top_n
REPLIERS1_DB  = Path(args.repliers1)
REPLIERS2_DB  = Path(args.repliers2)
ACCOUNTS_DBS  = [f"accounts{i}.db" for i in range(1, N_ACCT_DBS + 1)]

# ── Worker → accounts DB assignment (fixed, round-robin) ──────────────────────
# Each worker thread gets one DB for its entire lifetime.

_worker_db: dict[str, str] = {}   # thread name → accounts db path
_worker_db_lock = threading.Lock()
_worker_db_counter = 0

def my_accounts_db() -> str:
    name = threading.current_thread().name
    with _worker_db_lock:
        if name not in _worker_db:
            global _worker_db_counter
            slot = _worker_db_counter % len(ACCOUNTS_DBS)
            _worker_db_counter += 1
            _worker_db[name] = ACCOUNTS_DBS[slot]
    return _worker_db[name]

# ── DB setup ───────────────────────────────────────────────────────────────────

DDL1 = """
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

-- Tracks every tweet whose replies have been attempted, so 0-reply tweets
-- are not re-fetched on resume. reply_count is what was actually saved.
CREATE TABLE IF NOT EXISTS scraped_replies_log (
    tweet_id    TEXT PRIMARY KEY,
    owner_id    TEXT NOT NULL,
    reply_count INTEGER NOT NULL DEFAULT 0,
    scraped_at  TEXT NOT NULL
);
"""

DDL2 = """
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

CREATE TABLE IF NOT EXISTS pfp (
    twitter_id   TEXT PRIMARY KEY,
    username     TEXT,
    display_name TEXT,
    pfp_url      TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
"""

def open_db(path: Path, ddl: str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), check_same_thread=False, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=20000")
    conn.executescript(ddl)
    conn.commit()
    return conn

# Opened once; writes serialised through their own locks.
_db1 = open_db(REPLIERS1_DB, DDL1)
_db2 = open_db(REPLIERS2_DB, DDL2)
_db1_lock = threading.Lock()
_db2_lock = threading.Lock()

# ── twscrape helper ────────────────────────────────────────────────────────────

# Keywords in twscrape stderr that indicate a transient network/connection issue
_NETWORK_ERR_MARKERS = (
    "connection timed out",
    "connectiontimeout",
    "read timed out",
    "remotedisconnected",
    "connection reset",
    "connection refused",
    "network is unreachable",
    "temporary failure in name resolution",
    "ssl",
    "errno 110",
    "errno 104",
    "errno 111",
)
_NETWORK_RETRY_WAIT  = 60   # seconds to wait between network retries
_NETWORK_MAX_RETRIES = 5    # max waits before giving up


def _is_network_error(stderr: str) -> bool:
    low = stderr.lower()
    return any(m in low for m in _NETWORK_ERR_MARKERS)


def run_twscrape(cmd_args: list[str], timeout: int = 300) -> str:
    """Run a twscrape command with two retry tiers:

    - subprocess.TimeoutExpired (process ran too long): 1 immediate retry.
    - Network/connection errors in stderr: up to 5 retries, each after a 60s wait.
    - All other errors: raise immediately, no retry.
    """
    accounts_db  = my_accounts_db()
    cmd          = ["twscrape", "--db", accounts_db] + cmd_args
    process_tries   = 0          # counts subprocess.TimeoutExpired hits (max 1 retry)
    network_retries = 0          # counts network-error retries (max _NETWORK_MAX_RETRIES)

    while True:
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            process_tries += 1
            if process_tries <= 1:
                log.warning(
                    f"[twscrape] process timeout after {timeout}s — retrying immediately: {cmd_args}"
                )
                continue
            else:
                raise RuntimeError(f"twscrape timed out twice after {timeout}s: {cmd_args}")

        if result.returncode != 0:
            stderr = result.stderr.strip()
            if _is_network_error(stderr):
                network_retries += 1
                if network_retries <= _NETWORK_MAX_RETRIES:
                    log.warning(
                        f"[twscrape] network error (attempt {network_retries}/{_NETWORK_MAX_RETRIES}), "
                        f"waiting {_NETWORK_RETRY_WAIT}s before retry: {stderr[:120]}"
                    )
                    time.sleep(_NETWORK_RETRY_WAIT)
                    continue
                else:
                    raise RuntimeError(
                        f"twscrape network error after {_NETWORK_MAX_RETRIES} retries: {stderr}"
                    )
            raise RuntimeError(stderr or "twscrape non-zero exit")

        return result.stdout.strip()

# ── datetime parser ────────────────────────────────────────────────────────────

def _parse_dt(s: str) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

# ── Progress tracker ───────────────────────────────────────────────────────────

class Progress:
    def __init__(self, phase: str, total: int):
        self.phase   = phase
        self.total   = total
        self.done    = 0
        self.failed  = 0
        self._lock   = threading.Lock()
        self._start  = time.time()
        log.info(f"[{phase}] starting — {total} items")

    def tick(self, ok: bool = True):
        with self._lock:
            if ok:
                self.done += 1
            else:
                self.failed += 1
            finished = self.done + self.failed
            elapsed  = time.time() - self._start
            rate     = finished / elapsed if elapsed else 0
            eta      = (self.total - finished) / rate if rate else 0
            log.info(
                f"[{self.phase}] {finished}/{self.total} "
                f"(ok={self.done} err={self.failed}) "
                f"rate={rate:.1f}/s eta={eta:.0f}s"
            )

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — RESOLVE
# ══════════════════════════════════════════════════════════════════════════════

def resolve_one(username: str) -> dict | None:
    """Returns meta dict on success, None on failure."""
    try:
        raw = run_twscrape(["user_by_login", username], timeout=60)
    except Exception as e:
        log.warning(f"[resolve] '{username}' twscrape error: {e}")
        return None

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
        uname = obj.get("username") or username
        meta = {
            "twitter_id":   uid,
            "username":     uname,
            "display_name": obj.get("displayname") or obj.get("name") or uname,
            "pfp_url":      obj.get("profileImageUrl", ""),
        }
        log.info(f"[resolve] @{username} → id={uid} (@{uname})")
        return meta

    log.warning(f"[resolve] no user found for '{username}'")
    return None


def save_pfp(meta: dict):
    now = datetime.now(timezone.utc).isoformat()
    with _db2_lock:
        _db2.execute("""
            INSERT INTO pfp (twitter_id, username, display_name, pfp_url, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(twitter_id) DO UPDATE SET
                username=excluded.username,
                display_name=COALESCE(excluded.display_name, display_name),
                pfp_url=COALESCE(NULLIF(excluded.pfp_url,''), pfp_url),
                updated_at=excluded.updated_at
        """, (meta["twitter_id"], meta["username"],
              meta["display_name"], meta["pfp_url"], now))
        _db2.commit()


def _already_resolved(username: str) -> dict | None:
    """Check repliers2.db pfp table. Returns meta dict if already saved, else None."""
    with _db2_lock:
        row = _db2.execute(
            "SELECT twitter_id, username, display_name, pfp_url FROM pfp WHERE LOWER(username)=LOWER(?)",
            (username,)
        ).fetchone()
    if row:
        return {
            "twitter_id":   row[0],
            "username":     row[1],
            "display_name": row[2],
            "pfp_url":      row[3],
        }
    return None


def phase_resolve(usernames: list[str], executor: ThreadPoolExecutor) -> dict[str, dict]:
    """Returns {original_username_lower: meta} for all successfully resolved users."""
    # ── Skip users already saved in pfp table ─────────────────────────────────
    to_fetch: list[str] = []
    results:  dict[str, dict] = {}
    for u in usernames:
        cached = _already_resolved(u)
        if cached:
            log.info(f"[resolve] @{u} already in pfp — skipping")
            results[u.lower()] = cached
        else:
            to_fetch.append(u)

    skipped = len(usernames) - len(to_fetch)
    if skipped:
        log.info(f"[RESOLVE] {skipped} skipped (already in pfp), {len(to_fetch)} to fetch")

    prog = Progress("RESOLVE", len(to_fetch))
    lock = threading.Lock()

    futures = {executor.submit(resolve_one, u): u for u in to_fetch}
    for fut in as_completed(futures):
        u    = futures[fut]
        meta = None
        try:
            meta = fut.result()
        except Exception as e:
            log.warning(f"[resolve] unexpected error for {u}: {e}")
        if meta:
            save_pfp(meta)
            with lock:
                results[u.lower()] = meta
            prog.tick(ok=True)
        else:
            prog.tick(ok=False)

    log.info(f"[RESOLVE] done — {len(results)}/{len(usernames)} resolved ({skipped} from cache)")
    return results

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — TWEETS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_tweets_one(meta: dict) -> list[dict]:
    twitter_id = meta["twitter_id"]
    username   = meta["username"]
    try:
        raw = run_twscrape(
            ["user_tweets", twitter_id, "--limit", str(TWEET_LIMIT)],
            timeout=300,
        )
    except Exception as e:
        log.warning(f"[tweets] @{username} twscrape error: {e}")
        return []

    scraped_at = datetime.now(timezone.utc).isoformat()
    seen: set[str] = set()
    qualifying: list[dict] = []
    pinned_ids: set[str] = set()
    resolved_username = username

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            tweet = json.loads(line)
        except json.JSONDecodeError:
            continue

        user_obj      = tweet.get("user") or {}
        tweet_user_id = str(user_obj.get("id", ""))
        if tweet_user_id != twitter_id:
            continue

        if not pinned_ids:
            pinned_ids = {str(pid) for pid in (user_obj.get("pinnedIds") or [])}

        tid = tweet.get("id_str") or str(tweet.get("id", ""))
        if not tid or tid in seen:
            continue
        seen.add(tid)

        if tid in pinned_ids:
            continue
        if tweet.get("retweetedTweet"):
            continue

        in_reply_to_user = tweet.get("inReplyToUser")
        if in_reply_to_user:
            reply_target_id = str(in_reply_to_user.get("id", ""))
            if reply_target_id and reply_target_id != twitter_id:
                continue

        uname = user_obj.get("username", "")
        if uname:
            resolved_username = uname

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

    log.info(f"[tweets] @{username}: {len(qualifying)} qualifying tweets")
    return qualifying


def save_tweets(rows: list[dict]):
    if not rows:
        return
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
    with _db1_lock:
        _db1.executemany(sql, rows)
        _db1.commit()


def _already_fetched_tweets(twitter_id: str) -> list[str] | None:
    """Return list of tweet_ids already saved for this owner, or None if none exist."""
    with _db1_lock:
        rows = _db1.execute(
            "SELECT tweet_id FROM tweets WHERE owner_id=?", (twitter_id,)
        ).fetchall()
    if rows:
        return [r[0] for r in rows]
    return None


def phase_tweets(resolved: dict[str, dict], executor: ThreadPoolExecutor) -> dict[str, list[str]]:
    """Returns {twitter_id: [tweet_id, ...]} for use in phase 3."""
    # ── Skip users whose tweets are already in repliers1.db ───────────────────
    to_fetch: list[dict] = []
    id_map:   dict[str, list[str]] = {}
    for meta in resolved.values():
        existing = _already_fetched_tweets(meta["twitter_id"])
        if existing:
            log.info(f"[tweets] @{meta['username']} already has {len(existing)} tweets — skipping")
            id_map[meta["twitter_id"]] = existing
        else:
            to_fetch.append(meta)

    skipped = len(resolved) - len(to_fetch)
    if skipped:
        log.info(f"[TWEETS] {skipped} skipped (already in db), {len(to_fetch)} to fetch")

    prog = Progress("TWEETS", len(to_fetch))
    lock = threading.Lock()

    futures = {executor.submit(fetch_tweets_one, m): m for m in to_fetch}
    for fut in as_completed(futures):
        m    = futures[fut]
        rows = []
        try:
            rows = fut.result()
        except Exception as e:
            log.warning(f"[tweets] unexpected error for @{m['username']}: {e}")
        if rows:
            save_tweets(rows)
            with lock:
                id_map[m["twitter_id"]] = [r["tweet_id"] for r in rows]
            prog.tick(ok=True)
        else:
            prog.tick(ok=False)

    log.info(f"[TWEETS] done — {len(id_map)}/{len(resolved)} users have tweets ({skipped} from cache)")
    return id_map

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3 — REPLIES
# Each tweet is one work-item so all 100 workers stay busy.
# ══════════════════════════════════════════════════════════════════════════════

def _already_fetched_replies(tweet_id: str) -> bool:
    """Return True if replies for this tweet have already been attempted.

    Checks two sources:
    1. scraped_replies_log — written by this script after every successful fetch,
       including 0-reply tweets.
    2. replies table — fallback for DBs populated before scraped_replies_log existed;
       if any reply row exists for this tweet_id it was clearly fetched before.
    """
    with _db1_lock:
        in_log = _db1.execute(
            "SELECT 1 FROM scraped_replies_log WHERE tweet_id=?", (tweet_id,)
        ).fetchone()
        if in_log:
            return True
        in_replies = _db1.execute(
            "SELECT 1 FROM replies WHERE source_tweet_id=? LIMIT 1", (tweet_id,)
        ).fetchone()
        return in_replies is not None


_replies_log_queue: list[tuple] = []
_replies_log_queue_lock = threading.Lock()
_REPLIES_LOG_FLUSH_EVERY = 20   # flush to DB after this many entries


def _log_replies_scraped(tweet_id: str, owner_id: str, reply_count: int):
    """Queue a scraped_replies_log entry; flush to DB every N entries."""
    now = datetime.now(timezone.utc).isoformat()
    with _replies_log_queue_lock:
        _replies_log_queue.append((tweet_id, owner_id, reply_count, now))
        if len(_replies_log_queue) >= _REPLIES_LOG_FLUSH_EVERY:
            _flush_replies_log()


def _flush_replies_log():
    """Write all queued log entries to DB. Must be called with _replies_log_queue_lock held."""
    if not _replies_log_queue:
        return
    with _db1_lock:
        _db1.executemany("""
            INSERT OR REPLACE INTO scraped_replies_log (tweet_id, owner_id, reply_count, scraped_at)
            VALUES (?, ?, ?, ?)
        """, _replies_log_queue)
        _db1.commit()
    _replies_log_queue.clear()


def _flush_replies_log_final():
    """Flush any remaining queued entries at end of phase."""
    with _replies_log_queue_lock:
        _flush_replies_log()


def fetch_replies_one(tweet_id: str, owner_id: str) -> tuple[list[dict], bool]:
    """Returns (rows, success). success=True even if 0 replies — False only on error."""
    try:
        raw = run_twscrape(["tweet_replies", tweet_id], timeout=300)
    except Exception as e:
        log.warning(f"[replies] tweet {tweet_id} twscrape error: {e}")
        return [], False

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

        user       = reply.get("user") or {}
        replier_id = str(user.get("id", ""))
        if not replier_id or replier_id == owner_id:
            continue

        dt = _parse_dt(reply.get("date", ""))
        rows.append({
            "source_tweet_id":   tweet_id,
            "source_owner_id":   owner_id,
            "reply_id":          rid,
            "reply_url":         reply.get("url", ""),
            "reply_content":     reply.get("rawContent", ""),
            "reply_lang":        reply.get("lang"),
            "reply_date":        dt.strftime("%Y-%m-%d") if dt else None,
            "reply_time":        dt.strftime("%H:%M:%S") if dt else None,
            "replier_id":        replier_id,
            "replier_username":  user.get("username"),
            "replier_name":      user.get("displayname"),
            "replier_pfp":       user.get("profileImageUrl"),
            "likes":             reply.get("likeCount"),
            "retweets":          reply.get("retweetCount"),
            "replies_count":     reply.get("replyCount"),
            "views":             reply.get("viewCount"),
            "scraped_at":        scraped_at,
        })

    log.info(f"[replies] tweet {tweet_id}: {len(rows)} replies saved")
    return rows, True


def save_replies(rows: list[dict]):
    if not rows:
        return
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
    with _db1_lock:
        _db1.executemany(sql, rows)
        _db1.commit()


def _load_fetched_tweet_ids() -> set[str]:
    """Load all tweet IDs already attempted in one query. Much faster than per-tweet checks."""
    with _db1_lock:
        log_ids = {r[0] for r in _db1.execute(
            "SELECT tweet_id FROM scraped_replies_log"
        ).fetchall()}
        reply_ids = {r[0] for r in _db1.execute(
            "SELECT DISTINCT source_tweet_id FROM replies"
        ).fetchall()}
    combined = log_ids | reply_ids
    log.info(f"[REPLIES] {len(log_ids)} in log + {len(reply_ids)} in replies "
             f"= {len(combined)} already fetched tweet IDs loaded")
    return combined


def phase_replies(id_map: dict[str, list[str]], executor: ThreadPoolExecutor):
    """Fetch replies for every tweet id across all users."""
    # Bulk load already-fetched IDs in one shot — not one query per tweet
    already_fetched = _load_fetched_tweet_ids()

    all_work = [(tid, oid) for oid, tids in id_map.items() for tid in tids]
    to_fetch = [(tid, oid) for tid, oid in all_work if tid not in already_fetched]
    skipped  = len(all_work) - len(to_fetch)

    if skipped:
        log.info(f"[REPLIES] {skipped}/{len(all_work)} tweets skipped (already fetched), "
                 f"{len(to_fetch)} to fetch")

    prog = Progress("REPLIES", len(to_fetch))

    futures = {executor.submit(fetch_replies_one, tid, oid): (tid, oid)
               for tid, oid in to_fetch}
    for fut in as_completed(futures):
        tid, oid = futures[fut]
        rows, success = [], False
        try:
            rows, success = fut.result()
        except Exception as e:
            log.warning(f"[replies] unexpected error tweet {tid}: {e}")

        if success:
            if rows:
                save_replies(rows)
            _log_replies_scraped(tid, oid, len(rows))
            prog.tick(ok=True)
        else:
            prog.tick(ok=False)

    _flush_replies_log_final()
    log.info("[REPLIES] done")

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 4 — TOP REPLIERS
# Computed from repliers1.db, saved to repliers2.db.
# Done owner-by-owner; compute is cheap so we use the executor for parallelism
# but each task is a pure DB read+write, no twscrape.
# ══════════════════════════════════════════════════════════════════════════════

def compute_and_save_top(owner_id: str, owner_username: str) -> bool:
    # ── Read from repliers1.db ─────────────────────────────────────────────────
    with _db1_lock:
        bottom5_rows = _db1.execute("""
            SELECT source_tweet_id
            FROM replies
            WHERE source_owner_id = ?
              AND replier_id != ?
            GROUP BY source_tweet_id
            ORDER BY COUNT(*) ASC
            LIMIT 5
        """, (owner_id, owner_id)).fetchall()
        bottom5_ids = {r[0] for r in bottom5_rows}

        count_rows = _db1.execute("""
            SELECT replier_id, replier_username, replier_name, replier_pfp,
                   COUNT(*) AS reply_count
            FROM replies
            WHERE source_owner_id = ?
              AND replier_id != ?
            GROUP BY replier_id
        """, (owner_id, owner_id)).fetchall()

        if bottom5_ids:
            coverage_rows = _db1.execute("""
                SELECT replier_id,
                       COUNT(DISTINCT source_tweet_id) AS covered,
                       MIN(reply_date || 'T' || COALESCE(reply_time,'00:00:00')) AS earliest
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
        log.warning(f"[top] no replies found for owner_id={owner_id} (@{owner_username})")
        return False

    coverage_map = {r[0]: (r[1], r[2] or "9999") for r in coverage_rows}

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
    for rank, r in enumerate(sorted_repliers[:TOP_N], start=1):
        ranked.append({
            "owner_id":           owner_id,
            "owner_username":     owner_username,
            "rank":               rank,
            "replier_id":         r["replier_id"],
            "replier_username":   r["replier_username"],
            "replier_name":       r["replier_name"],
            "replier_pfp":        r["replier_pfp"],
            "reply_count":        r["reply_count"],
            "earliest_reply_date":
                r["earliest_low_traffic"] if r["earliest_low_traffic"] != "9999" else None,
        })

    # ── Write to repliers2.db ──────────────────────────────────────────────────
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
    with _db2_lock:
        _db2.executemany(sql, ranked)
        _db2.commit()

    log.info(f"[top] @{owner_username}: saved {len(ranked)} top repliers")
    return True


def phase_top(id_map: dict[str, list[str]], resolved: dict[str, dict],
              executor: ThreadPoolExecutor):
    """Compute + save top repliers for every owner who had tweets."""
    # Build (owner_id, owner_username) list
    owners = []
    for uname_lower, meta in resolved.items():
        if meta["twitter_id"] in id_map:
            owners.append((meta["twitter_id"], meta["username"]))

    prog    = Progress("TOP_REPLIERS", len(owners))
    futures = {executor.submit(compute_and_save_top, oid, uname): (oid, uname)
               for oid, uname in owners}
    for fut in as_completed(futures):
        oid, uname = futures[fut]
        ok = False
        try:
            ok = fut.result()
        except Exception as e:
            log.warning(f"[top] unexpected error for @{uname}: {e}")
        prog.tick(ok=ok)

    log.info("[TOP_REPLIERS] done")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def load_usernames(path: Path) -> list[str]:
    if not path.exists():
        log.error(f"Users file not found: {path}")
        return []
    seen: set[str] = set()
    usernames: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        u = line.strip().lstrip("@")
        if u and u.lower() not in seen:
            seen.add(u.lower())
            usernames.append(u)
    log.info(f"Loaded {len(usernames)} unique usernames from {path}")
    return usernames


def main():
    usernames = load_usernames(USERS_FILE)
    if not usernames:
        log.error("No usernames to process. Exiting.")
        return

    t0 = time.time()
    log.info(
        f"Starting — users={len(usernames)} workers={N_WORKERS} "
        f"accounts_dbs={N_ACCT_DBS} tweet_limit={TWEET_LIMIT} top_n={TOP_N}"
    )
    log.info(f"repliers1.db → {REPLIERS1_DB}  (tweets + replies)")
    log.info(f"repliers2.db → {REPLIERS2_DB}  (top_repliers + pfp)")

    # One shared pool reused across all phases
    with ThreadPoolExecutor(max_workers=N_WORKERS,
                            thread_name_prefix="worker") as executor:

        # ── Phase 1: Resolve ──────────────────────────────────────────────────
        log.info("=" * 60)
        log.info("PHASE 1: RESOLVE usernames")
        log.info("=" * 60)
        resolved = phase_resolve(usernames, executor)

        if not resolved:
            log.error("No users resolved. Exiting.")
            return

        # ── Phase 2: Tweets ───────────────────────────────────────────────────
        log.info("=" * 60)
        log.info("PHASE 2: FETCH TWEETS")
        log.info("=" * 60)
        id_map = phase_tweets(resolved, executor)

        if not id_map:
            log.warning("No tweets found for any user. Skipping replies + top.")
        else:
            # ── Phase 3: Replies ──────────────────────────────────────────────
            log.info("=" * 60)
            log.info("PHASE 3: FETCH REPLIES")
            log.info("=" * 60)
            phase_replies(id_map, executor)

            # ── Phase 4: Top repliers ─────────────────────────────────────────
            log.info("=" * 60)
            log.info("PHASE 4: COMPUTE TOP REPLIERS")
            log.info("=" * 60)
            phase_top(id_map, resolved, executor)

    elapsed = time.time() - t0
    log.info(f"All phases complete in {elapsed:.0f}s ({elapsed/60:.1f} min)")


if __name__ == "__main__":
    main()
