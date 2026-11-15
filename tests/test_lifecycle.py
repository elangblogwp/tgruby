import asyncio
import json
from threading import Event

import pytest
from telethon import errors

from ruby_ubot import connections
from ruby_ubot.config import Config
from ruby_ubot.ledger import Ledger
from ruby_ubot.sessions import Manager
from ruby_ubot.store import Store

from .support import CHAT, Client, emit, history_reader, local_config, make_pool, post


@pytest.mark.asyncio
async def test_transient_restore_then_disconnect_uses_same_slot(tmp_path, monkeypatch):
    monkeypatch.setattr(connections, "RETRY_INITIAL", 0.01)
    ledger = Ledger(tmp_path / "db")
    manager = Manager(Store(tmp_path / "cfg", 7), ledger, local_config(tmp_path))
    manager.session_dir.mkdir()
    manager.session(7, 1).with_suffix(".session").touch()
    first, second, third = Client(9), Client(9), Client(9)
    first.connect.side_effect = ConnectionError("temporary")
    clients = iter((first, second, third))
    manager.make_client = lambda uid, slot: next(clients)
    try:
        await manager.restore()
        async with asyncio.timeout(2):
            while 9 not in manager.by_account:
                await asyncio.sleep(0.001)
        assert manager.pool(7).accounts[1].client is second
        second.disconnected.set_exception(ConnectionError("offline"))
        async with asyncio.timeout(2):
            while (
                not manager.pool(7).accounts.get(1)
                or manager.pool(7).accounts[1].client is not third
            ):
                await asyncio.sleep(0.001)
        second.disconnect.assert_awaited_once()
        assert not second.handlers and len(third.handlers) == 3 and manager.by_account == {9: 7}
    finally:
        await manager.shutdown()
        await ledger.close()
    assert not manager.connections.repairs and not manager.connections.watches


@pytest.mark.asyncio
async def test_logout_cancels_reconnect_backoff(tmp_path, monkeypatch):
    monkeypatch.setattr(connections, "RETRY_INITIAL", 30)
    manager = Manager(Store(tmp_path / "cfg", 7), Ledger(tmp_path / "db"), local_config(tmp_path))
    manager.session_dir.mkdir()
    manager.session(7, 1).with_suffix(".session").touch()
    failed = Client(9)
    failed.connect.side_effect = TimeoutError()
    manager.make_client = lambda uid, slot: failed
    try:
        await manager.restore()
        assert (7, 1) in manager.connections.repairs
        await manager.logout(7)
        await asyncio.sleep(0)
        assert not manager.connections.repairs and not manager.slots(7) and not manager.workers
        failed.connect.assert_awaited_once()
    finally:
        await manager.shutdown()
        await manager.ledger.close()


@pytest.mark.asyncio
async def test_flood_during_login_is_shared_with_next_login_and_survives_restart(tmp_path):
    config = local_config(tmp_path)
    manager = Manager(Store(tmp_path / "cfg", 7), Ledger(tmp_path / "db"), config)
    worker = Client(9)
    worker.send_code_request.side_effect = errors.FloodWaitError(None, capture=40)
    manager.make_client = lambda uid, slot: worker
    await manager.begin_login(7)
    await manager.submit(7, "4")
    assert "40 detik" in await manager.submit(7, "+62811111111")
    assert "Telegram membatasi login" in await manager.begin_login(7)
    await manager.shutdown()
    await manager.ledger.close()
    manager = Manager(Store(tmp_path / "cfg", 7), Ledger(tmp_path / "db"), config)
    try:
        assert "Telegram membatasi login" in await manager.begin_login(7)
        assert not manager.pending
        worker.send_code_request.assert_awaited_once()
    finally:
        await manager.shutdown()
        await manager.ledger.close()


@pytest.mark.asyncio
async def test_many_concurrent_config_changes_coalesce_without_blocking(tmp_path, monkeypatch):
    store = Store(tmp_path / "data.json", 7)
    entered, resume = Event(), Event()
    writes = []
    actual = store._write

    def save(snapshot):
        writes.append(snapshot["settings"]["7"]["texts"])
        entered.set()
        assert resume.wait(3)
        actual(snapshot)

    monkeypatch.setattr(store, "_write", save)
    cfg = store.settings(7)
    cfg["texts"] = ["awal"]
    first = asyncio.create_task(store.save())
    assert await asyncio.to_thread(entered.wait, 1)
    first.cancel()
    await asyncio.gather(first, return_exceptions=True)
    for n in range(100):
        cfg["texts"] = [str(n)]
        store.queue_save()
    resume.set()
    await store.drain()
    assert writes == [["awal"], ["99"]]
    assert json.loads(store.path.read_text())["settings"]["7"]["texts"] == ["99"]


@pytest.mark.asyncio
async def test_flood_overflow_recovers_with_bounded_waiting_jobs(tmp_path):
    pool = make_pool(tmp_path, 4)
    records = [post(i, text="noktel").message for i in range(1, 161)]
    history_reader(pool.ordered[0], records, before_id=0)
    try:
        await pool.ledger.defer(7, CHAT, 0.20)
        work = asyncio.gather(
            *(pool.on_post(pool.ordered[0], post(i, text="noktel")) for i in range(1, 161))
        )
        await asyncio.sleep(0.015)
        assert 0 < len(pool.jobs) <= 32 and pool.pause_timer is not None
        assert not pool.sent
        await work
        if pool.history_task:
            await pool.history_task
        await pool.check_history()
        ids = [r.reply_to.reply_to_msg_id for a in pool.ordered for r in a.client.sent]
        assert sorted(ids) == list(range(1, 161)) and pool.recovered >= 128
    finally:
        await pool.stop()
        await pool.ledger.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [errors.ChannelPrivateError, errors.AuthKeyDuplicatedError])
async def test_permanent_access_errors_stop_all_workers(pool, error):
    pool.ordered[0].client.failures.append(error(None))
    await emit(pool, post(77))
    assert not pool.active
    await emit(pool, post(78))
    assert all(not a.client.sent for a in pool.ordered)


@pytest.mark.asyncio
async def test_exact_maximum_promotion_falls_back_only_if_no_emoji_fits(pool):
    await pool.commands.cmd_text("x" * 4096)
    await emit(pool, post(12, text="samsung 🤙"))
    assert pool.ordered[0].client.sent[0].message == "x" * 4096
    await pool.commands.cmd_text("x" * 4096 + "|ready")
    await emit(pool, post(13, text="samsung 🤙"))
    assert pool.ordered[0].client.sent[1].message == "ready 🤙"


@pytest.mark.parametrize("timeout", ["nan", "inf", "a"])
def test_invalid_runtime_environment_fails_without_showing_secrets(tmp_path, monkeypatch, timeout):
    for name, value in {
        "OWNER_ID": "7",
        "API_ID": "1",
        "BOT_TOKEN": "secret_token",
        "API_HASH": "secret_hash",
        "SEND_TIMEOUT": timeout,
    }.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(SystemExit) as error:
        Config.load(tmp_path)
    assert "SEND_TIMEOUT" in str(error.value) and "secret" not in str(error.value)


def test_dotenv_resolves_from_install_directory(tmp_path, monkeypatch):
    for name in ("API_ID", "API_HASH", "BOT_TOKEN", "OWNER_ID", "SEND_TIMEOUT"):
        monkeypatch.delenv(name, raising=False)
    (tmp_path / ".env").write_text("API_ID=1\nAPI_HASH=hidden\nBOT_TOKEN=private\nOWNER_ID=7\n")
    config = Config.load(tmp_path)
    assert config.owner_id == 7 and config.send_timeout == 30
    assert "hidden" not in repr(config) and "private" not in repr(config)
