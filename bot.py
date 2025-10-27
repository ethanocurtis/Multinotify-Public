from __future__ import annotations
import os
import re
import json
import math
import asyncio
import hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import tasks

import aiosqlite
import aiohttp
import feedparser
from dateutil import parser as dtparser, tz as datetz
import asyncpraw

# ===============================
# .env loader (simple)
# ===============================
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "public-mg-bot/3.1 by you")

if not DISCORD_TOKEN:
    raise SystemExit("DISCORD_TOKEN missing. Set it in .env or environment.")

# ===============================
# Constants
# ===============================
DB_PATH = os.path.join(os.path.dirname(__file__), "bot.db")
DEFAULT_INTERVAL_SEC = 180   # per guild
MAX_POSTS_PER_TICK = 15      # cap per guild channel per tick
DM_POSTS_PER_TICK = 8        # cap per user per tick
DIGEST_MAX_ITEMS = 40        # cap items per digest
SEEN_TTL_DAYS = 30
HTTP_TIMEOUT = 20

INTENTS = discord.Intents.none()
INTENTS.guilds = True
INTENTS.members = True   # needed to open DMs
INTENTS.messages = True

UTC = datetz.tzutc()
DEFAULT_TZ = "America/Chicago"

# ===============================
# DB schema
# ===============================
SCHEMA_SQL = r"""
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS guild_prefs (
  guild_id INTEGER PRIMARY KEY,
  channel_id INTEGER,
  interval_sec INTEGER DEFAULT 180,
  timezone TEXT DEFAULT 'America/Chicago',
  reddit_keywords TEXT DEFAULT '[]',
  reddit_subs TEXT DEFAULT '[]',
  reddit_flairs TEXT DEFAULT '[]',
  rss_feeds TEXT DEFAULT '[]',
  rss_keywords TEXT DEFAULT '[]',
  digest_enabled INTEGER DEFAULT 0,
  digest_hour INTEGER DEFAULT 9,
  digest_minute INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_prefs (
  guild_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  dm_enabled INTEGER DEFAULT 0,
  timezone TEXT DEFAULT 'America/Chicago',
  reddit_keywords TEXT DEFAULT '[]',
  reddit_subs TEXT DEFAULT '[]',
  rss_feeds TEXT DEFAULT '[]',
  rss_keywords TEXT DEFAULT '[]',
  digest_enabled INTEGER DEFAULT 0,
  digest_hour INTEGER DEFAULT 9,
  digest_minute INTEGER DEFAULT 0,
  PRIMARY KEY (guild_id, user_id)
);

-- target_type: 'guild' or 'user'
CREATE TABLE IF NOT EXISTS seen (
  guild_id INTEGER NOT NULL,
  source TEXT NOT NULL,
  item_id TEXT NOT NULL,
  target_type TEXT NOT NULL,
  target_id INTEGER NOT NULL,
  created_at TIMESTAMP DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY (guild_id, source, item_id, target_type, target_id)
);

CREATE TABLE IF NOT EXISTS digest_queue (
  guild_id INTEGER NOT NULL,
  target_type TEXT NOT NULL,
  target_id INTEGER NOT NULL,
  source TEXT NOT NULL,
  item_id TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY (guild_id, target_type, target_id, source, item_id)
);

CREATE TABLE IF NOT EXISTS digest_meta (
  guild_id INTEGER NOT NULL,
  target_type TEXT NOT NULL,
  target_id INTEGER NOT NULL,
  last_sent_at TIMESTAMP,
  PRIMARY KEY (guild_id, target_type, target_id)
);
"""

async def db_init() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA_SQL)
        await db.commit()

# ----------------- common helpers -----------------
async def _json_load(s: Optional[str], default: Any) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default

# ---- guild prefs ----
async def get_guild_prefs(db: aiosqlite.Connection, guild_id: int) -> Dict[str, Any]:
    async with db.execute(
        "SELECT channel_id, interval_sec, timezone, reddit_keywords, reddit_subs, reddit_flairs, rss_feeds, rss_keywords, digest_enabled, digest_hour, digest_minute FROM guild_prefs WHERE guild_id=?",
        (guild_id,),
    ) as cur:
        row = await cur.fetchone()
    if not row:
        await db.execute("INSERT OR IGNORE INTO guild_prefs(guild_id) VALUES(?)", (guild_id,))
        await db.commit()
        return {
            "channel_id": None,
            "interval_sec": DEFAULT_INTERVAL_SEC,
            "timezone": DEFAULT_TZ,
            "reddit_keywords": [],
            "reddit_subs": [],
            "reddit_flairs": [],
            "rss_feeds": [],
            "rss_keywords": [],
            "digest_enabled": 0,
            "digest_hour": 9,
            "digest_minute": 0,
        }
    (channel_id, interval_sec, tz, rk, rs, rf, rfeeds, rsk, d_en, d_h, d_m) = row
    return {
        "channel_id": channel_id,
        "interval_sec": interval_sec or DEFAULT_INTERVAL_SEC,
        "timezone": tz or DEFAULT_TZ,
        "reddit_keywords": await _json_load(rk, []),
        "reddit_subs": await _json_load(rs, []),
        "reddit_flairs": await _json_load(rf, []),
        "rss_feeds": await _json_load(rfeeds, []),
        "rss_keywords": await _json_load(rsk, []),
        "digest_enabled": int(d_en or 0),
        "digest_hour": int(d_h or 9),
        "digest_minute": int(d_m or 0),
    }

async def set_guild_pref(db: aiosqlite.Connection, guild_id: int, key: str, value: Any) -> None:
    json_keys = {"reddit_keywords", "reddit_subs", "reddit_flairs", "rss_feeds", "rss_keywords"}
    if key in json_keys:
        value = json.dumps(value, ensure_ascii=False)
    await db.execute(
        f"INSERT INTO guild_prefs(guild_id,{key}) VALUES(?,?) ON CONFLICT(guild_id) DO UPDATE SET {key}=excluded.{key}",
        (guild_id, value),
    )
    await db.commit()

# ---- user prefs ----
async def get_user_prefs(db: aiosqlite.Connection, guild_id: int, user_id: int) -> Dict[str, Any]:
    async with db.execute(
        "SELECT dm_enabled, timezone, reddit_keywords, reddit_subs, rss_feeds, rss_keywords, digest_enabled, digest_hour, digest_minute FROM user_prefs WHERE guild_id=? AND user_id=?",
        (guild_id, user_id),
    ) as cur:
        row = await cur.fetchone()
    if not row:
        await db.execute("INSERT OR IGNORE INTO user_prefs(guild_id, user_id) VALUES(?,?)", (guild_id, user_id))
        await db.commit()
        return {
            "dm_enabled": 0,
            "timezone": DEFAULT_TZ,
            "reddit_keywords": [],
            "reddit_subs": [],
            "rss_feeds": [],
            "rss_keywords": [],
            "digest_enabled": 0,
            "digest_hour": 9,
            "digest_minute": 0,
        }
    (dm, tz, rk, rs, rfeeds, rsk, d_en, d_h, d_m) = row
    return {
        "dm_enabled": int(dm or 0),
        "timezone": tz or DEFAULT_TZ,
        "reddit_keywords": await _json_load(rk, []),
        "reddit_subs": await _json_load(rs, []),
        "rss_feeds": await _json_load(rfeeds, []),
        "rss_keywords": await _json_load(rsk, []),
        "digest_enabled": int(d_en or 0),
        "digest_hour": int(d_h or 9),
        "digest_minute": int(d_m or 0),
    }

async def set_user_pref(db: aiosqlite.Connection, guild_id: int, user_id: int, key: str, value: Any) -> None:
    json_keys = {"reddit_keywords", "reddit_subs", "rss_feeds", "rss_keywords"}
    if key in json_keys:
        value = json.dumps(value, ensure_ascii=False)
    await db.execute(
        f"INSERT INTO user_prefs(guild_id, user_id, {key}) VALUES(?,?,?) ON CONFLICT(guild_id, user_id) DO UPDATE SET {key}=excluded.{key}",
        (guild_id, user_id, value),
    )
    await db.commit()

# ---- seen & digest helpers ----
async def is_seen(db: aiosqlite.Connection, guild_id: int, source: str, item_id: str, target_type: str, target_id: int) -> bool:
    async with db.execute(
        "SELECT 1 FROM seen WHERE guild_id=? AND source=? AND item_id=? AND target_type=? AND target_id=?",
        (guild_id, source, item_id, target_type, target_id),
    ) as cur:
        return (await cur.fetchone()) is not None

async def mark_seen(db: aiosqlite.Connection, guild_id: int, source: str, item_id: str, target_type: str, target_id: int) -> None:
    try:
        await db.execute(
            "INSERT OR IGNORE INTO seen(guild_id, source, item_id, target_type, target_id) VALUES(?,?,?,?,?)",
            (guild_id, source, item_id, target_type, target_id),
        )
        await db.commit()
    except Exception:
        pass

async def prune_seen(db: aiosqlite.Connection) -> None:
    await db.execute("DELETE FROM seen WHERE created_at < datetime('now', ?)", (f"-{SEEN_TTL_DAYS} days",))
    await db.commit()

async def enqueue_digest(db: aiosqlite.Connection, guild_id: int, target_type: str, target_id: int, source: str, item_id: str, payload: Dict[str, Any]) -> None:
    try:
        await db.execute(
            "INSERT OR IGNORE INTO digest_queue(guild_id, target_type, target_id, source, item_id, payload) VALUES(?,?,?,?,?,?)",
            (guild_id, target_type, target_id, source, item_id, json.dumps(payload, ensure_ascii=False)),
        )
        await db.commit()
    except Exception:
        pass

async def dequeue_digest(db: aiosqlite.Connection, guild_id: int, target_type: str, target_id: int, limit: int) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    async with db.execute(
        "SELECT source, item_id, payload FROM digest_queue WHERE guild_id=? AND target_type=? AND target_id=? ORDER BY created_at DESC LIMIT ?",
        (guild_id, target_type, target_id, limit),
    ) as cur:
        rows = await cur.fetchall()
    for s, iid, payload in rows:
        try:
            data = json.loads(payload)
            data["_source"] = s
            data["_id"] = iid
            items.append(data)
        except Exception:
            continue
    # delete consumed
    async with db.execute(
        "DELETE FROM digest_queue WHERE guild_id=? AND target_type=? AND target_id=?",
        (guild_id, target_type, target_id),
    ):
        pass
    await db.commit()
    return items

async def get_due_targets(db: aiosqlite.Connection, now_utc) -> Tuple[List[Tuple[int,int,Dict[str,Any]]], List[Tuple[int,int,Dict[str,Any]]]]:
    """Return (guild_digest_targets, user_digest_targets).
    Each element is (guild_id, target_id, prefs).
    """
    due_guilds: List[Tuple[int,int,Dict[str,Any]]] = []
    due_users: List[Tuple[int,int,Dict[str,Any]]] = []

    # guild digests
    async with db.execute("SELECT guild_id, timezone, digest_enabled, digest_hour, digest_minute FROM guild_prefs WHERE digest_enabled=1") as cur:
        for gid, tz, en, hh, mm in await cur.fetchall():
            tz = tz or DEFAULT_TZ
            try:
                zone = datetz.gettz(tz)
            except Exception:
                zone = datetz.gettz(DEFAULT_TZ)
            local = now_utc.astimezone(zone)
            if int(en or 0) and local.hour == int(hh or 9) and local.minute == int(mm or 0):
                due_guilds.append((gid, gid, {"timezone": tz, "digest_hour": hh, "digest_minute": mm}))

    # user digests
    async with db.execute("SELECT guild_id, user_id, timezone, digest_enabled, digest_hour, digest_minute FROM user_prefs WHERE digest_enabled=1") as cur:
        for gid, uid, tz, en, hh, mm in await cur.fetchall():
            tz = tz or DEFAULT_TZ
            try:
                zone = datetz.gettz(tz)
            except Exception:
                zone = datetz.gettz(DEFAULT_TZ)
            local = now_utc.astimezone(zone)
            if int(en or 0) and local.hour == int(hh or 9) and local.minute == int(mm or 0):
                due_users.append((gid, uid, {"timezone": tz, "digest_hour": hh, "digest_minute": mm}))

    return due_guilds, due_users

# ===============================
# Fetchers (async)
# ===============================
class Fetchers:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.reddit: Optional[asyncpraw.Reddit] = None

    async def ensure_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT))
        return self.session

    async def ensure_reddit(self) -> Optional[asyncpraw.Reddit]:
        if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET):
            return None
        if self.reddit is None:
            self.reddit = asyncpraw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent=REDDIT_USER_AGENT,
            )
        return self.reddit

    async def fetch_reddit(self, subs: List[str], keywords: List[str], flairs: List[str], limit_per_sub: int = 20) -> List[Dict[str, Any]]:
        reddit = await self.ensure_reddit()
        if reddit is None or not subs:
            return []
        out: List[Dict[str, Any]] = []
        words = [w.lower() for w in keywords]
        flairset = set(f.lower() for f in flairs)
        for sub in subs:
            try:
                subreddit = await reddit.subreddit(sub, fetch=True)
                async for post in subreddit.new(limit=limit_per_sub):
                    title = (post.title or "").lower()
                    flair = (getattr(post, 'link_flair_text', None) or "").lower()
                    if words and not any(w in title for w in words):
                        continue
                    if flairset and flair not in flairset:
                        continue
                    url = f"https://reddit.com{post.permalink}"
                    out.append({
                        "id": post.id,
                        "title": post.title,
                        "url": url,
                        "author": str(post.author) if post.author else "",
                        "created_utc": float(getattr(post, 'created_utc', 0) or 0),
                        "subreddit": str(post.subreddit.display_name),
                    })
            except Exception:
                continue
        return out

    async def fetch_rss(self, urls: List[str], keywords: List[str]) -> List[Dict[str, Any]]:
        if not urls:
            return []
        session = await self.ensure_session()
        out: List[Dict[str, Any]] = []
        words = [w.lower() for w in keywords]
        for url in urls:
            try:
                async with session.get(url, headers={"User-Agent": REDDIT_USER_AGENT}) as r:
                    if r.status != 200:
                        continue
                    data = await r.read()
                feed = feedparser.parse(data)
                for e in feed.entries[:30]:
                    title = (e.get("title") or "")
                    title_l = title.lower()
                    link = e.get("link") or ""
                    guid = e.get("id") or link or title
                    if words and not any(w in title_l for w in words):
                        continue
                    # best-effort timestamp
                    ts = None
                    for k in ("published", "updated", "created"):
                        if e.get(k):
                            try:
                                ts = dtparser.parse(e[k]).timestamp()
                                break
                            except Exception:
                                pass
                    out.append({
                        "id": hashlib.sha1((guid or link).encode("utf-8", "ignore")).hexdigest(),
                        "title": title,
                        "url": link,
                        "created_utc": ts or 0.0,
                        "feed": feed.feed.get("title") if feed.feed else "",
                    })
            except Exception:
                continue
        return out

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
        if self.reddit:
            try:
                await self.reddit.close()
            except Exception:
                pass

fetchers = Fetchers()

# ===============================
# Rate limiter (token bucket, simple)
# ===============================
@dataclass
class Bucket:
    tokens: float
    rate: float  # tokens per sec
    capacity: float
    last: float

    def take(self, now: float, cost: float = 1.0) -> float:
        # refill
        elapsed = max(0.0, now - self.last)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last = now
        if self.tokens >= cost:
            self.tokens -= cost
            return 0.0
        # need to wait
        needed = cost - self.tokens
        wait = needed / self.rate if self.rate > 0 else 1.0
        self.tokens = 0.0
        return wait

# one bucket for channel sends, another for DMs
CHANNEL_BUCKET = Bucket(tokens=10, rate=0.5, capacity=10, last=0.0)
DM_BUCKET = Bucket(tokens=5, rate=0.3, capacity=5, last=0.0)

# ===============================
# Discord bot
# ===============================
class PublicBot(discord.AutoShardedClient):
    def __init__(self):
        super().__init__(intents=INTENTS)
        self.tree = app_commands.CommandTree(self)
        self.db: Optional[aiosqlite.Connection] = None
        self._bg_started = False

    async def setup_hook(self) -> None:
        self.db = await aiosqlite.connect(DB_PATH)
        await db_init()
        # sync once globally at startup
        try:
            await self.tree.sync()
        except Exception:
            pass
        if not self._bg_started:
            self.poller.start()
            self.digest_scheduler.start()
            self.pruner.start()
            self._bg_started = True

    async def on_guild_join(self, guild: discord.Guild):
        if self.db:
            # touch row
            await set_guild_pref(self.db, guild.id, "interval_sec", DEFAULT_INTERVAL_SEC)

    # -------------- background loops --------------
    @tasks.loop(seconds=30)
    async def poller(self):
        if not self.db:
            return
        try:
            await self._poll_all_guilds()
        except Exception:
            pass

    async def _poll_all_guilds(self):
        if not self.db:
            return
        async with self.db.execute("SELECT guild_id FROM guild_prefs") as cur:
            gids = [row[0] for row in await cur.fetchall()]
        gids = [g.id for g in self.guilds if g.id in set(gids)]

        now = discord.utils.utcnow().timestamp()
        for gid in gids:
            prefs = await get_guild_prefs(self.db, gid)
            chan_id = prefs.get("channel_id")
            interval = int(prefs.get("interval_sec") or DEFAULT_INTERVAL_SEC)
            if not chan_id:
                continue
            if int(now) % interval != 0:
                continue
            channel = self.get_channel(chan_id) or (await self.fetch_channel(chan_id))
            if not isinstance(channel, (discord.TextChannel, discord.Thread)):
                continue

            subs = list(prefs.get("reddit_subs") or [])
            rwords = list(prefs.get("reddit_keywords") or [])
            flairs = list(prefs.get("reddit_flairs") or [])
            feeds = list(prefs.get("rss_feeds") or [])
            fwords = list(prefs.get("rss_keywords") or [])

            posts: List[Tuple[str, Dict[str, Any]]] = []
            try:
                r_items = await fetchers.fetch_reddit(subs, rwords, flairs, limit_per_sub=15)
                posts.extend(("reddit", it) for it in r_items)
            except Exception:
                pass
            try:
                rss_items = await fetchers.fetch_rss(feeds, fwords)
                posts.extend(("rss", it) for it in rss_items)
            except Exception:
                pass

            posts.sort(key=lambda x: float(x[1].get("created_utc") or 0.0), reverse=True)

            sent = 0
            for src, item in posts:
                if sent >= MAX_POSTS_PER_TICK:
                    break
                item_id = item.get("id") or item.get("url") or item.get("title")
                if not item_id:
                    continue
                if await is_seen(self.db, gid, src, item_id, "guild", chan_id):
                    continue
                wait = CHANNEL_BUCKET.take(now=discord.utils.utcnow().timestamp(), cost=1.0)
                if wait > 0:
                    await asyncio.sleep(wait)
                ok = await self._send_embed(channel, src, item)
                if ok:
                    await mark_seen(self.db, gid, src, item_id, "guild", chan_id)
                    sent += 1
                await asyncio.sleep(0.75)

            # DM notifications for users who opted-in
            async with self.db.execute("SELECT user_id FROM user_prefs WHERE guild_id=? AND dm_enabled=1", (gid,)) as cur:
                dm_user_ids = [r[0] for r in await cur.fetchall()]
            if dm_user_ids:
                await self._poll_user_dms(gid, dm_user_ids)

    async def _poll_user_dms(self, guild_id: int, user_ids: List[int]):
        # Build per-user unions (their personal + guild-wide)
        prefs_g = await get_guild_prefs(self.db, guild_id)
        g_subs = set(prefs_g.get("reddit_subs") or [])
        g_rwords = set((prefs_g.get("reddit_keywords") or []))
        g_flairs = set((prefs_g.get("reddit_flairs") or []))
        g_feeds = set((prefs_g.get("rss_feeds") or []))
        g_fwords = set((prefs_g.get("rss_keywords") or []))

        # Fetch once per guild and reuse for users to reduce calls
        posts_by_source: List[Tuple[str, Dict[str, Any]]] = []
        try:
            r_items = await fetchers.fetch_reddit(list(g_subs), list(g_rwords), list(g_flairs), limit_per_sub=10)
            posts_by_source.extend(("reddit", it) for it in r_items)
        except Exception:
            pass
        try:
            rss_items = await fetchers.fetch_rss(list(g_feeds), list(g_fwords))
            posts_by_source.extend(("rss", it) for it in rss_items)
        except Exception:
            pass

        posts_by_source.sort(key=lambda x: float(x[1].get("created_utc") or 0.0), reverse=True)

        for uid in user_ids:
            up = await get_user_prefs(self.db, guild_id, uid)
            u_subs = set(up.get("reddit_subs") or [])
            u_rwords = set((up.get("reddit_keywords") or []))
            u_feeds = set((up.get("rss_feeds") or []))
            u_fwords = set((up.get("rss_keywords") or []))
            # unions
            subs = list(g_subs | u_subs)
            rwords = list(g_rwords | u_rwords)
            feeds = list(g_feeds | u_feeds)
            fwords = list(g_fwords | u_fwords)

            # filter per-user
            sent = 0
            for src, item in posts_by_source:
                if sent >= DM_POSTS_PER_TICK:
                    break
                # filter by union
                title_l = (item.get("title") or "").lower()
                if src == "reddit" and rwords and not any(w.lower() in title_l for w in rwords):
                    continue
                if src == "rss" and fwords and not any(w.lower() in title_l for w in fwords):
                    continue
                # seen check per user
                item_id = item.get("id") or item.get("url") or item.get("title")
                if not item_id:
                    continue
                if await is_seen(self.db, guild_id, src, item_id, "user", uid):
                    continue
                # send DM
                user = self.get_user(uid) or await self.fetch_user(uid)
                if not user:
                    continue
                wait = DM_BUCKET.take(now=discord.utils.utcnow().timestamp(), cost=1.0)
                if wait > 0:
                    await asyncio.sleep(wait)
                ok = await self._send_dm(user, src, item)
                if ok:
                    await mark_seen(self.db, guild_id, src, item_id, "user", uid)
                    sent += 1
                await asyncio.sleep(0.75)

    @tasks.loop(seconds=30)
    async def digest_scheduler(self):
        if not self.db:
            return
        now = discord.utils.utcnow()
        try:
            g_targets, u_targets = await get_due_targets(self.db, now)
        except Exception:
            return

        # guild digests to channel
        for gid, _target_id, _prefs in g_targets:
            prefs = await get_guild_prefs(self.db, gid)
            chan_id = prefs.get("channel_id")
            if not chan_id:
                continue
            channel = self.get_channel(chan_id) or (await self.fetch_channel(chan_id))
            if not isinstance(channel, (discord.TextChannel, discord.Thread)):
                continue
            await self._send_digest_guild(gid, channel)

        # user digests via DM
        for gid, uid, _prefs in u_targets:
            user = self.get_user(uid) or await self.fetch_user(uid)
            if not user:
                continue
            await self._send_digest_user(gid, user)

    @tasks.loop(hours=6)
    async def pruner(self):
        if not self.db:
            return
        try:
            await prune_seen(self.db)
        except Exception:
            pass

    # -------------- send helpers --------------
    async def _send_embed(self, channel: discord.abc.MessageableChannel, src: str, item: Dict[str, Any]) -> bool:
        try:
            title = item.get("title") or "(no title)"
            url = item.get("url") or None
            emb = discord.Embed(title=title, url=url, color=discord.Color.blurple())
            if src == "reddit":
                emb.set_author(name=f"r/{item.get('subreddit','reddit')}")
                if item.get("author"):
                    emb.set_footer(text=f"by u/{item['author']}")
            elif src == "rss":
                if item.get("feed"):
                    emb.set_author(name=item["feed"])
            await channel.send(embed=emb)
            return True
        except Exception:
            return False

    async def _send_dm(self, user: discord.User | discord.Member, src: str, item: Dict[str, Any]) -> bool:
        try:
            dm = user.dm_channel or await user.create_dm()
            title = item.get("title") or "(no title)"
            url = item.get("url") or None
            emb = discord.Embed(title=title, url=url, color=discord.Color.green())
            if src == "reddit":
                emb.set_author(name=f"r/{item.get('subreddit','reddit')}")
                if item.get("author"):
                    emb.set_footer(text=f"by u/{item['author']}")
            elif src == "rss":
                if item.get("feed"):
                    emb.set_author(name=item["feed"])
            await dm.send(embed=emb)
            return True
        except Exception:
            return False

    async def _send_digest_guild(self, guild_id: int, channel: discord.abc.MessageableChannel):
        items = await dequeue_digest(self.db, guild_id, "guild", channel.id, DIGEST_MAX_ITEMS)
        if not items:
            return
        chunks = [items[i:i+10] for i in range(0, len(items), 10)]
        for chunk in chunks:
            desc = "\n".join(f"• [{i.get('title','(no title)')}]({i.get('url','#')})" for i in chunk)
            emb = discord.Embed(title="Daily Digest", description=desc, color=discord.Color.orange())
            await channel.send(embed=emb)
            await asyncio.sleep(1.0)

    async def _send_digest_user(self, guild_id: int, user: discord.User | discord.Member):
        items = await dequeue_digest(self.db, guild_id, "user", user.id, DIGEST_MAX_ITEMS)
        if not items:
            return
        dm = user.dm_channel or await user.create_dm()
        chunks = [items[i:i+10] for i in range(0, len(items), 10)]
        for chunk in chunks:
            desc = "\n".join(f"• [{i.get('title','(no title)')}]({i.get('url','#')})" for i in chunk)
            emb = discord.Embed(title="Your Daily Digest", description=desc, color=discord.Color.gold())
            await dm.send(embed=emb)
            await asyncio.sleep(1.0)

bot = PublicBot()

# ===============================
# Slash commands
# ===============================
ADMIN_PERMS = app_commands.default_permissions(manage_guild=True)
GUILD_ONLY = app_commands.guild_only()

@bot.tree.command(name="help", description="Show commands and usage")
async def help_cmd(inter: discord.Interaction):
    txt = (
        "**Admin (Manage Server)**\n"
        "• /set_channel #channel — choose where the bot posts\n"
        "• /set_interval seconds — how often to poll sources\n"
        "• /set_timezone tz — e.g. America/Chicago\n"
        "• /add_subreddit name — add a subreddit (no r/)\n"
        "• /remove_subreddit name — remove a subreddit\n"
        "• /add_flair name — restrict to flair (optional, repeatable)\n"
        "• /remove_flair name — remove flair filter\n"
        "• /set_reddit_keywords \"kw1, kw2, ...\" — keyword filter (optional)\n"
        "• /add_rss url — add an RSS/Atom feed URL\n"
        "• /remove_rss url — remove feed\n"
        "• /set_rss_keywords \"kw1, kw2, ...\" — keyword filter for RSS\n"
        "• /set_guild_digest on/off hour minute — daily channel digest\n"
        "\n**User (per guild)**\n"
        "• /dm_on — enable DMs\n"
        "• /dm_off — disable DMs\n"
        "• /my_timezone tz — set your timezone\n"
        "• /my_digest on/off hour minute — daily DM digest\n"
        "• /my_add_sub name — add your subreddit (no r/)\n"
        "• /my_remove_sub name — remove your subreddit\n"
        "• /my_set_reddit_keywords \"kw1, kw2\" — set your reddit keywords\n"
        "• /my_add_rss url — add your feed\n"
        "• /my_remove_rss url — remove your feed\n"
        "• /my_set_rss_keywords \"kw1, kw2\" — set your rss keywords\n"
        "\n**Diagnostics**\n"
        "• /show_settings — view server settings\n"
        "• /my_settings — view your settings\n"
        "• /test_digest — push a sample digest now\n"
    )
    await inter.response.send_message(txt, ephemeral=True)

# ---- Admin commands ----
@bot.tree.command(name="set_channel", description="Set the channel where the bot posts")
@ADMIN_PERMS
@GUILD_ONLY
async def set_channel(inter: discord.Interaction, channel: discord.TextChannel):
    assert bot.db
    await set_guild_pref(bot.db, inter.guild_id, "channel_id", channel.id)
    await inter.response.send_message(f"Posting channel set to {channel.mention}.", ephemeral=True)

@bot.tree.command(name="set_interval", description="Set polling interval in seconds (min 60)")
@ADMIN_PERMS
@GUILD_ONLY
async def set_interval(inter: discord.Interaction, seconds: app_commands.Range[int, 60, 21600]):
    assert bot.db
    await set_guild_pref(bot.db, inter.guild_id, "interval_sec", int(seconds))
    await inter.response.send_message(f"Interval set to {int(seconds)} seconds.", ephemeral=True)

@bot.tree.command(name="set_timezone", description="Set timezone, e.g., America/Chicago")
@ADMIN_PERMS
@GUILD_ONLY
async def set_timezone(inter: discord.Interaction, tz: str):
    assert bot.db
    await set_guild_pref(bot.db, inter.guild_id, "timezone", tz)
    await inter.response.send_message(f"Timezone set to `{tz}`.", ephemeral=True)

@bot.tree.command(name="add_subreddit", description="Add a subreddit (no r/)")
@ADMIN_PERMS
@GUILD_ONLY
async def add_subreddit(inter: discord.Interaction, name: str):
    assert bot.db
    name = name.strip().lstrip("r/")
    prefs = await get_guild_prefs(bot.db, inter.guild_id)
    subs = set(prefs["reddit_subs"]) | {name}
    await set_guild_pref(bot.db, inter.guild_id, "reddit_subs", sorted(subs))
    await inter.response.send_message(f"Added subreddit `r/{name}`.", ephemeral=True)

@bot.tree.command(name="remove_subreddit", description="Remove a subreddit")
@ADMIN_PERMS
@GUILD_ONLY
async def remove_subreddit(inter: discord.Interaction, name: str):
    assert bot.db
    name = name.strip().lstrip("r/")
    prefs = await get_guild_prefs(bot.db, inter.guild_id)
    subs = [s for s in prefs["reddit_subs"] if s.lower() != name.lower()]
    await set_guild_pref(bot.db, inter.guild_id, "reddit_subs", subs)
    await inter.response.send_message(f"Removed subreddit `r/{name}`.", ephemeral=True)

@bot.tree.command(name="add_flair", description="Add an allowed flair filter (title must match flair)")
@ADMIN_PERMS
@GUILD_ONLY
async def add_flair(inter: discord.Interaction, flair: str):
    assert bot.db
    prefs = await get_guild_prefs(bot.db, inter.guild_id)
    flairs = set(prefs["reddit_flairs"]) | {flair}
    await set_guild_pref(bot.db, inter.guild_id, "reddit_flairs", sorted(flairs))
    await inter.response.send_message(f"Added flair filter `{flair}`.", ephemeral=True)

@bot.tree.command(name="remove_flair", description="Remove a flair filter")
@ADMIN_PERMS
@GUILD_ONLY
async def remove_flair(inter: discord.Interaction, flair: str):
    assert bot.db
    prefs = await get_guild_prefs(bot.db, inter.guild_id)
    flairs = [f for f in prefs["reddit_flairs"] if f.lower() != flair.lower()]
    await set_guild_pref(bot.db, inter.guild_id, "reddit_flairs", flairs)
    await inter.response.send_message(f"Removed flair filter `{flair}`.", ephemeral=True)

@bot.tree.command(name="set_reddit_keywords", description="Comma-separated keyword list for Reddit (optional)")
@ADMIN_PERMS
@GUILD_ONLY
async def set_reddit_keywords(inter: discord.Interaction, keywords: str):
    assert bot.db
    words = [w.strip() for w in re.split(r",|;", keywords) if w.strip()]
    await set_guild_pref(bot.db, inter.guild_id, "reddit_keywords", words)
    await inter.response.send_message(f"Reddit keywords set: {', '.join(words) if words else '(none)'}.", ephemeral=True)

@bot.tree.command(name="add_rss", description="Add an RSS/Atom feed URL")
@ADMIN_PERMS
@GUILD_ONLY
async def add_rss(inter: discord.Interaction, url: str):
    assert bot.db
    prefs = await get_guild_prefs(bot.db, inter.guild_id)
    feeds = set(prefs["rss_feeds"]) | {url}
    await set_guild_pref(bot.db, inter.guild_id, "rss_feeds", sorted(feeds))
    await inter.response.send_message(f"Added RSS feed `{url}`.", ephemeral=True)

@bot.tree.command(name="remove_rss", description="Remove an RSS/Atom feed URL")
@ADMIN_PERMS
@GUILD_ONLY
async def remove_rss(inter: discord.Interaction, url: str):
    assert bot.db
    prefs = await get_guild_prefs(bot.db, inter.guild_id)
    feeds = [u for u in prefs["rss_feeds"] if u.lower() != url.lower()]
    await set_guild_pref(bot.db, inter.guild_id, "rss_feeds", feeds)
    await inter.response.send_message(f"Removed RSS feed `{url}`.", ephemeral=True)

@bot.tree.command(name="set_rss_keywords", description="Comma-separated keyword list for RSS (optional)")
@ADMIN_PERMS
@GUILD_ONLY
async def set_rss_keywords(inter: discord.Interaction, keywords: str):
    assert bot.db
    words = [w.strip() for w in re.split(r",|;", keywords) if w.strip()]
    await set_guild_pref(bot.db, inter.guild_id, "rss_keywords", words)
    await inter.response.send_message(f"RSS keywords set: {', '.join(words) if words else '(none)'}.", ephemeral=True)

@bot.tree.command(name="set_guild_digest", description="Enable/disable daily guild digest to the configured channel")
@ADMIN_PERMS
@GUILD_ONLY
async def set_guild_digest(inter: discord.Interaction, enabled: bool, hour: app_commands.Range[int,0,23], minute: app_commands.Range[int,0,59]):
    assert bot.db
    await set_guild_pref(bot.db, inter.guild_id, "digest_enabled", 1 if enabled else 0)
    await set_guild_pref(bot.db, inter.guild_id, "digest_hour", int(hour))
    await set_guild_pref(bot.db, inter.guild_id, "digest_minute", int(minute))
    await inter.response.send_message(f"Guild digest {'enabled' if enabled else 'disabled'} at {hour:02d}:{minute:02d}.", ephemeral=True)

@bot.tree.command(name="show_settings", description="Show current settings for this server")
@ADMIN_PERMS
@GUILD_ONLY
async def show_settings(inter: discord.Interaction):
    assert bot.db
    prefs = await get_guild_prefs(bot.db, inter.guild_id)
    lines = [
        f"**Channel:** {('<not set>' if not prefs['channel_id'] else f'<#%s>' % prefs['channel_id'])}",
        f"**Interval:** {prefs['interval_sec']} sec",
        f"**Timezone:** {prefs['timezone']}",
        f"**Subreddits:** {', '.join(prefs['reddit_subs']) or '(none)'}",
        f"**Flairs:** {', '.join(prefs['reddit_flairs']) or '(none)'}",
        f"**Reddit keywords:** {', '.join(prefs['reddit_keywords']) or '(none)'}",
        f"**RSS feeds:** {', '.join(prefs['rss_feeds']) or '(none)'}",
        f"**RSS keywords:** {', '.join(prefs['rss_keywords']) or '(none)'}",
        f"**Digest:** {'on' if prefs['digest_enabled'] else 'off'} at {prefs['digest_hour']:02d}:{prefs['digest_minute']:02d}",
    ]
    await inter.response.send_message("\n".join(lines), ephemeral=True)

# ---- User commands ----
@bot.tree.command(name="dm_on", description="Enable DMs for this guild")
@GUILD_ONLY
async def dm_on(inter: discord.Interaction):
    assert bot.db
    await set_user_pref(bot.db, inter.guild_id, inter.user.id, "dm_enabled", 1)
    await inter.response.send_message("DMs enabled.", ephemeral=True)

@bot.tree.command(name="dm_off", description="Disable DMs for this guild")
@GUILD_ONLY
async def dm_off(inter: discord.Interaction):
    assert bot.db
    await set_user_pref(bot.db, inter.guild_id, inter.user.id, "dm_enabled", 0)
    await inter.response.send_message("DMs disabled.", ephemeral=True)

@bot.tree.command(name="my_timezone", description="Set your timezone, e.g., America/Chicago")
@GUILD_ONLY
async def my_timezone(inter: discord.Interaction, tz: str):
    assert bot.db
    await set_user_pref(bot.db, inter.guild_id, inter.user.id, "timezone", tz)
    await inter.response.send_message(f"Your timezone set to `{tz}`.", ephemeral=True)

@bot.tree.command(name="my_digest", description="Enable/disable your daily DM digest")
@GUILD_ONLY
async def my_digest(inter: discord.Interaction, enabled: bool, hour: app_commands.Range[int,0,23], minute: app_commands.Range[int,0,59]):
    assert bot.db
    await set_user_pref(bot.db, inter.guild_id, inter.user.id, "digest_enabled", 1 if enabled else 0)
    await set_user_pref(bot.db, inter.guild_id, inter.user.id, "digest_hour", int(hour))
    await set_user_pref(bot.db, inter.guild_id, inter.user.id, "digest_minute", int(minute))
    await inter.response.send_message(f"Your digest {'enabled' if enabled else 'disabled'} at {hour:02d}:{minute:02d}.", ephemeral=True)

@bot.tree.command(name="my_add_sub", description="Add a personal subreddit (no r/)")
@GUILD_ONLY
async def my_add_sub(inter: discord.Interaction, name: str):
    assert bot.db
    name = name.strip().lstrip("r/")
    prefs = await get_user_prefs(bot.db, inter.guild_id, inter.user.id)
    subs = set(prefs["reddit_subs"]) | {name}
    await set_user_pref(bot.db, inter.guild_id, inter.user.id, "reddit_subs", sorted(subs))
    await inter.response.send_message(f"Added your subreddit `r/{name}`.", ephemeral=True)

@bot.tree.command(name="my_remove_sub", description="Remove a personal subreddit")
@GUILD_ONLY
async def my_remove_sub(inter: discord.Interaction, name: str):
    assert bot.db
    name = name.strip().lstrip("r/")
    prefs = await get_user_prefs(bot.db, inter.guild_id, inter.user.id)
    subs = [s for s in prefs["reddit_subs"] if s.lower() != name.lower()]
    await set_user_pref(bot.db, inter.guild_id, inter.user.id, "reddit_subs", subs)
    await inter.response.send_message(f"Removed your subreddit `r/{name}`.", ephemeral=True)

@bot.tree.command(name="my_set_reddit_keywords", description="Comma-separated keyword list for your Reddit filter (optional)")
@GUILD_ONLY
async def my_set_reddit_keywords(inter: discord.Interaction, keywords: str):
    assert bot.db
    words = [w.strip() for w in re.split(r",|;", keywords) if w.strip()]
    await set_user_pref(bot.db, inter.guild_id, inter.user.id, "reddit_keywords", words)
    await inter.response.send_message(f"Your Reddit keywords set: {', '.join(words) if words else '(none)'}.", ephemeral=True)

@bot.tree.command(name="my_add_rss", description="Add a personal RSS/Atom feed URL")
@GUILD_ONLY
async def my_add_rss(inter: discord.Interaction, url: str):
    assert bot.db
    prefs = await get_user_prefs(bot.db, inter.guild_id, inter.user.id)
    feeds = set(prefs["rss_feeds"]) | {url}
    await set_user_pref(bot.db, inter.guild_id, inter.user.id, "rss_feeds", sorted(feeds))
    await inter.response.send_message(f"Added your feed `{url}`.", ephemeral=True)

@bot.tree.command(name="my_remove_rss", description="Remove a personal RSS/Atom feed URL")
@GUILD_ONLY
async def my_remove_rss(inter: discord.Interaction, url: str):
    assert bot.db
    prefs = await get_user_prefs(bot.db, inter.guild_id, inter.user.id)
    feeds = [u for u in prefs["rss_feeds"] if u.lower() != url.lower()]
    await set_user_pref(bot.db, inter.guild_id, inter.user.id, "rss_feeds", feeds)
    await inter.response.send_message(f"Removed your feed `{url}`.", ephemeral=True)

@bot.tree.command(name="my_set_rss_keywords", description="Comma-separated keyword list for your RSS filter (optional)")
@GUILD_ONLY
async def my_set_rss_keywords(inter: discord.Interaction, keywords: str):
    assert bot.db
    words = [w.strip() for w in re.split(r",|;", keywords) if w.strip()]
    await set_user_pref(bot.db, inter.guild_id, inter.user.id, "rss_keywords", words)
    await inter.response.send_message(f"Your RSS keywords set: {', '.join(words) if words else '(none)'}.", ephemeral=True)

@bot.tree.command(name="my_settings", description="Show your settings for this guild")
@GUILD_ONLY
async def my_settings(inter: discord.Interaction):
    assert bot.db
    prefs = await get_user_prefs(bot.db, inter.guild_id, inter.user.id)
    lines = [
        f"**DMs:** {'on' if prefs['dm_enabled'] else 'off'}",
        f"**Timezone:** {prefs['timezone']}",
        f"**Subreddits:** {', '.join(prefs['reddit_subs']) or '(none)'}",
        f"**Reddit keywords:** {', '.join(prefs['reddit_keywords']) or '(none)'}",
        f"**RSS feeds:** {', '.join(prefs['rss_feeds']) or '(none)'}",
        f"**RSS keywords:** {', '.join(prefs['rss_keywords']) or '(none)'}",
        f"**Digest:** {'on' if prefs['digest_enabled'] else 'off'} at {prefs['digest_hour']:02d}:{prefs['digest_minute']:02d}",
    ]
    await inter.response.send_message("\n".join(lines), ephemeral=True)

@bot.tree.command(name="test_digest", description="Send a sample digest now (guild channel & DMs if enabled)")
@ADMIN_PERMS
@GUILD_ONLY
async def test_digest(inter: discord.Interaction):
    assert bot.db
    prefs = await get_guild_prefs(bot.db, inter.guild_id)
    chan_id = prefs.get("channel_id")
    await inter.response.send_message("Triggering test digest…", ephemeral=True)
    # write a few fake items into queues
    sample = [
        {"title":"Sample A","url":"https://example.com/a"},
        {"title":"Sample B","url":"https://example.com/b"},
        {"title":"Sample C","url":"https://example.com/c"},
    ]
    for it in sample:
        iid = hashlib.sha1((it['url']).encode()).hexdigest()
        await enqueue_digest(bot.db, inter.guild_id, "guild", chan_id or inter.channel_id, "rss", iid, it)
        await enqueue_digest(bot.db, inter.guild_id, "user", inter.user.id, "rss", iid, it)
    # send
    if chan_id:
        channel = bot.get_channel(chan_id) or (await bot.fetch_channel(chan_id))
        if isinstance(channel, (discord.TextChannel, discord.Thread)):
            await bot._send_digest_guild(inter.guild_id, channel)
    await bot._send_digest_user(inter.guild_id, inter.user)

# ===============================
# Run
# ===============================
async def main():
    try:
        await bot.start(DISCORD_TOKEN)
    finally:
        await fetchers.close()
        if bot.db:
            await bot.db.close()

if __name__ == "__main__":
    asyncio.run(main())
