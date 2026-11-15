import asyncio
import logging
import sqlite3
import time
from collections import deque

log = logging.getLogger(__name__)


class Ledger:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS claims (
                chat INTEGER, token TEXT, slot INTEGER, state TEXT,
                PRIMARY KEY (chat, token)
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS claims_reserved ON claims(state) WHERE state='reserved';
            CREATE TABLE IF NOT EXISTS turns (
                owner INTEGER PRIMARY KEY, turn INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS history (
                owner INTEGER, reader INTEGER, generation INTEGER NOT NULL,
                anchor INTEGER NOT NULL, high INTEGER NOT NULL,
                PRIMARY KEY (owner, reader)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS pauses (
                scope TEXT PRIMARY KEY, deadline REAL NOT NULL
            );
        """)
        self.writer = sqlite3.connect(path, check_same_thread=False)
        self.writes = deque()
        self.confirmed = deque()
        self.flusher = None
        with self.db:
            self.db.execute("DELETE FROM pauses WHERE deadline <= ?", (time.time(),))
        self.pauses = dict(self.db.execute("SELECT scope, deadline FROM pauses"))
        self.cursors = {
            (uid, generation, reader): (anchor, high)
            for uid, reader, generation, anchor, high in self.db.execute(
                "SELECT owner, reader, generation, anchor, high FROM history"
            )
        }

    def submit(self, operation, *args):
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        item = (operation, args, future)
        self.writes.append(item)
        self.start_flush()
        return future

    def confirm(self, chat, token):
        self.confirmed.append((chat, token))
        self.start_flush()

    def start_flush(self):
        if self.flusher is None:
            self.flusher = asyncio.create_task(self.flush())

    async def flush(self):
        try:
            while self.writes or self.confirmed:
                batch = [self.writes.popleft() for _ in range(min(128, len(self.writes)))]
                confirmed = [self.confirmed.popleft() for _ in range(min(256, len(self.confirmed)))]
                try:
                    results = await asyncio.to_thread(self.transaction, batch, confirmed)
                except asyncio.CancelledError:
                    for _, _, future in (*batch, *self.writes):
                        future.cancel()
                    self.writes.clear()
                    self.confirmed.clear()
                    raise
                except Exception as exc:
                    if confirmed:
                        log.exception(
                            "Gagal menyimpan status %s balasan terkonfirmasi; klaim tetap tersimpan",
                            len(confirmed),
                        )
                    for _, _, future in batch:
                        if not future.done():
                            future.set_exception(exc)
                else:
                    for (_, _, future), result in zip(batch, results):
                        if not future.done():
                            future.set_result(result)
        finally:
            self.flusher = None

    def transaction(self, batch, confirmed):
        with self.writer:
            self.writer.execute("BEGIN IMMEDIATE")
            self.batch_turns = {}
            results = [operation(*args) for operation, args, _ in batch]
            if confirmed:
                self.writer.executemany(
                    "UPDATE claims SET state='sent' WHERE chat=? AND token=? AND state='claimed'",
                    confirmed,
                )
            if self.batch_turns:
                self.writer.executemany(
                    "INSERT OR REPLACE INTO turns VALUES (?, ?)", self.batch_turns.items()
                )
            return results

    def claim(self, uid, chat, token, accounts, observer, direct=False):
        db = self.writer
        turn = self.batch_turns.get(uid)
        if turn is None:
            row = db.execute("SELECT turn FROM turns WHERE owner=?", (uid,)).fetchone()
            turn = row[0] if row else 0
        account = accounts[(turn // 2) % len(accounts)]
        armed = direct and account.peer is not None
        mark = db.execute(
            "INSERT OR IGNORE INTO claims VALUES (?, ?, ?, ?)",
            (chat, token, account.slot, "claimed" if armed else "reserved"),
        )
        if not mark.rowcount:
            resumed = db.execute(
                "UPDATE claims SET state='reserved' WHERE chat=? AND token=? AND slot=? AND state=?",
                (chat, token, observer.slot, f"await-copy/{uid}/{observer.user_id}"),
            )
            return (observer, False) if resumed.rowcount else None
        self.batch_turns[uid] = turn + 1
        return account, armed

    def checkpoint(self, uid, reader, generation, anchor, high):
        self.writer.execute(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(owner, reader) DO UPDATE SET generation=excluded.generation, "
            "anchor=excluded.anchor, high=excluded.high "
            "WHERE excluded.generation > history.generation OR "
            "(excluded.generation = history.generation AND excluded.high > history.high)",
            (uid, reader, generation, anchor, high),
        )

    def set_dispatch(self, chat, token, armed):
        before, after = ("reserved", "claimed") if armed else ("claimed", "reserved")
        changed = self.writer.execute(
            "UPDATE claims SET state=? WHERE chat=? AND token=? AND state=?",
            (after, chat, token, before),
        )
        return bool(changed.rowcount)

    def recover_reservations(self):
        with self.db:
            self.db.execute("DELETE FROM claims WHERE state='reserved'")

    def finish(self, chat, token, state):
        self.writer.execute(
            "UPDATE claims SET state=? WHERE chat=? AND token=? AND state IN ('claimed', 'reserved')",
            (state, chat, token),
        )

    def release(self, chat, token):
        self.writer.execute(
            "DELETE FROM claims WHERE chat=? AND token=? AND state IN ('claimed', 'reserved')",
            (chat, token),
        )

    async def close(self):
        if self.flusher is not None:
            await asyncio.shield(self.flusher)
        self.writer.close()
        self.db.close()

    def persist_pause(self, deadlines):
        self.writer.executemany(
            "INSERT INTO pauses VALUES (?, ?) ON CONFLICT(scope) DO UPDATE "
            "SET deadline=max(pauses.deadline, excluded.deadline)",
            deadlines,
        )

    def defer(self, uid, chat, seconds):
        scopes = (f"user/{uid}", f"chat/{chat}") if chat is not None else (f"user/{uid}",)
        return self.defer_scopes(scopes, seconds)

    def defer_scopes(self, scopes, seconds):
        deadline = time.time() + seconds
        deadlines = []
        for scope in scopes:
            if deadline > self.pauses.get(scope, 0):
                self.pauses[scope] = deadline
                deadlines.append((scope, deadline))
        if deadlines:
            return self.submit(self.persist_pause, deadlines)

    def scope_remaining(self, scope):
        deadline = self.pauses.get(scope)
        if deadline is None:
            return 0.0
        seconds = deadline - time.time()
        if seconds > 0:
            return seconds
        self.pauses.pop(scope, None)
        return 0.0

    def remaining(self, uid, chat):
        if not self.pauses:
            return 0.0
        return max(self.scope_remaining(f"user/{uid}"), self.scope_remaining(f"chat/{chat}"))
