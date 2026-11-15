import asyncio
import fcntl
import logging
import os
import re
import signal
from contextlib import AsyncExitStack

from telethon import TelegramClient, utils
from telethon.tl.types import PeerChannel

from .config import Config
from .ledger import Ledger
from .panel import register_panel
from .sessions import Manager
from .store import Store

log = logging.getLogger(__name__)


class ChannelSyncWarning(logging.Handler):
    pattern = re.compile(
        r"Getting difference for channel updates (\d+) caused PersistentTimestamp(?:Outdated|Invalid)Error"
    )

    def __init__(self, manager):
        super().__init__(logging.WARNING)
        self.manager = manager
        self.loop = asyncio.get_running_loop()

    def emit(self, record):
        match = self.pattern.search(record.getMessage())
        if match and not self.loop.is_closed():
            chat = utils.get_peer_id(PeerChannel(int(match[1])))
            self.loop.call_soon_threadsafe(self.check_target, chat)

    def check_target(self, chat):
        for pool in self.manager.workers.values():
            if pool.target == chat:
                pool.start_history_check()


async def run_service(config):
    session_dir = config.root / "sessions"
    store = Store(config.root / "data.json", config.owner_id)
    async with AsyncExitStack() as cleanup:
        ledger = Ledger(session_dir / "deliveries.sqlite3")
        cleanup.push_async_callback(ledger.close)
        cleanup.push_async_callback(store.drain)
        ledger.recover_reservations()
        manager = Manager(store, ledger, config)
        cleanup.push_async_callback(manager.shutdown)
        bot = TelegramClient(
            str(session_dir / "panel"), config.api_id, config.api_hash, connection_retries=-1
        )
        cleanup.push_async_callback(bot.disconnect)
        bot.parse_mode = None
        register_panel(bot, store, manager)
        updates_log = logging.getLogger("telethon.client.updates")
        warning_handler = ChannelSyncWarning(manager)
        updates_log.addHandler(warning_handler)
        cleanup.callback(warning_handler.close)
        cleanup.callback(updates_log.removeHandler, warning_handler)
        await manager.restore()
        await bot.start(bot_token=config.bot_token)
        identity = await bot.get_me()
        if (
            not identity
            or not identity.bot
            or str(identity.id) != config.bot_token.partition(":")[0]
        ):
            raise RuntimeError(
                "Sesi panel tidak cocok dengan BOT_TOKEN. Periksa sessions/panel.session."
            )
        manager.panel_username = identity.username
        for pool in manager.workers.values():
            pool.panel_username = identity.username
        monitor = asyncio.create_task(manager.monitor_history())
        try:
            log.info("panel online")
            await bot.run_until_disconnected()
        finally:
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)


async def main(config=None):
    if config is None:
        config = Config.load()
    session_dir = config.root / "sessions"
    os.umask(0o077)
    session_dir.mkdir(exist_ok=True)
    os.chmod(session_dir, 0o700)
    with (session_dir / "service.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("Ruby UBot sudah berjalan dengan direktori sesi ini.") from None
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        stopping = False

        def terminate():
            nonlocal stopping
            if not stopping:
                stopping = True
                task.cancel()

        loop.add_signal_handler(signal.SIGTERM, terminate)
        try:
            await run_service(config)
        except asyncio.CancelledError:
            if not stopping:
                raise
        finally:
            loop.remove_signal_handler(signal.SIGTERM)
