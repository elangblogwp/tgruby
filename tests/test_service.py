import asyncio
import os
import signal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ruby_ubot import app
from ruby_ubot.config import Config


class Panel:
    def __init__(self, ready, identity):
        self.ready = ready
        self.get_me = AsyncMock(return_value=identity)
        self.start = AsyncMock()
        self.disconnect = AsyncMock()

    def on(self, builder):
        return lambda handler: handler

    async def run_until_disconnected(self):
        self.ready.set()
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_service_graceful_shutdown_and_lock_release(tmp_path, monkeypatch):
    config = Config(tmp_path, 7, 1, "test", "42:token")
    ready = asyncio.Event()
    panel = Panel(ready, SimpleNamespace(id=42, username="TestPanelBot", bot=True))
    monkeypatch.setattr(app, "TelegramClient", lambda *args, **kwargs: panel)
    loop = asyncio.get_running_loop()
    handlers = {}
    monkeypatch.setattr(loop, "add_signal_handler", lambda sig, fn: handlers.update({sig: fn}))
    monkeypatch.setattr(loop, "remove_signal_handler", handlers.pop)
    previous_umask = os.umask(0o077)
    try:
        for run in range(2):
            ready.clear()
            task = asyncio.create_task(app.main(config))
            try:
                await asyncio.wait_for(ready.wait(), 4)
                assert signal.SIGTERM in handlers
                handlers[signal.SIGTERM]()
                await asyncio.wait_for(task, 4)
                assert signal.SIGTERM not in handlers
                assert panel.disconnect.await_count == run + 1
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
    finally:
        os.umask(previous_umask)
    assert (tmp_path / "sessions" / "deliveries.sqlite3").is_file()


@pytest.mark.asyncio
async def test_wrong_bot_session_disconnected_on_startup(tmp_path, monkeypatch):
    (tmp_path / "sessions").mkdir()
    panel = Panel(asyncio.Event(), SimpleNamespace(id=99, username="OtherBot", bot=True))
    monkeypatch.setattr(app, "TelegramClient", lambda *args, **kwargs: panel)
    with pytest.raises(RuntimeError, match="BOT_TOKEN"):
        await app.run_service(Config(tmp_path, 7, 1, "test", "42:test"))
    panel.disconnect.assert_awaited_once()
    assert not panel.ready.is_set()
