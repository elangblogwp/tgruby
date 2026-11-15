import asyncio
import logging

from telethon import errors

TRANSIENT = (OSError, TimeoutError, errors.ServerError, errors.FloodError)
RETRY_INITIAL = 2
RETRY_MAX = 60
log = logging.getLogger(__name__)


class Connections:
    def __init__(self, manager):
        self.manager = manager
        self.watches = {}
        self.repairs = {}
        self.closed = False

    def watch(self, pool, account):
        key = pool.uid, account.slot
        future = account.client.disconnected

        def disconnected(done):
            if self.watches.get(key) != (done, disconnected):
                return
            del self.watches[key]
            error = None if done.cancelled() else done.exception()
            if self.closed or pool.stopped or pool.accounts.get(account.slot) is not account:
                return
            pool.remove(account.slot)
            self.manager.by_account.pop(account.user_id, None)
            log.warning(
                "Worker %s terputus (%s)",
                account.slot,
                type(error).__name__ if error else "offline",
            )
            self.retry(pool, account.slot, account.client)

        self.watches[key] = future, disconnected
        future.add_done_callback(disconnected)

    def retry(self, pool, slot, client=None):
        key = pool.uid, slot
        if self.closed or pool.stopped or (key in self.repairs and not self.repairs[key].done()):
            return
        task = asyncio.create_task(self._repair(pool, slot, client))
        self.repairs[key] = task

        def finished(done):
            if self.repairs.get(key) is done:
                del self.repairs[key]
            if not done.cancelled() and done.exception() is not None:
                log.error("Pemulihan worker %s gagal", slot, exc_info=done.exception())

        task.add_done_callback(finished)

    async def _repair(self, pool, slot, previous):
        uid = pool.uid
        if previous is not None:
            try:
                await previous.disconnect()
            except TRANSIENT as exc:
                log.warning("Worker %s: koneksi lama telah ditutup (%s)", slot, type(exc).__name__)
        delay = RETRY_INITIAL
        while not self.closed and not pool.stopped:
            await asyncio.sleep(max(delay, self.manager.ledger.remaining(uid, pool.target)))
            async with self.manager.lock(uid):
                if (
                    self.closed
                    or pool.stopped
                    or not self.manager.store.is_authorized(uid)
                    or self.manager.workers.get(uid) is not pool
                    or slot in pool.accounts
                    or not self.manager.session(uid, slot).with_suffix(".session").is_file()
                ):
                    return
                pending = self.manager.pending.get(uid)
                if self.manager.ledger.remaining(uid, pool.target) or (
                    pending and pending.slot == slot and pending.client is not None
                ):
                    continue
                try:
                    connected = await self.manager.restore_once(uid, slot)
                except TRANSIENT:
                    delay = min(delay * 2, RETRY_MAX)
                    continue
                if connected:
                    account = pool.accounts.get(slot)
                    if account is None:
                        continue
                    await self.manager.catch_up(account)
                    if pool.accounts.get(slot) is not account:
                        await account.client.disconnect()
                        delay = RETRY_INITIAL
                        continue
                    pool.start_history_check(after_pause=True)
                return

    async def stop(self, uid=None):
        if uid is None:
            self.closed = True
        for key, (future, callback) in list(self.watches.items()):
            if uid is None or uid == key[0]:
                future.remove_done_callback(callback)
                self.watches.pop(key)
        tasks = [
            self.repairs.pop(key) for key in tuple(self.repairs) if uid is None or key[0] == uid
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
