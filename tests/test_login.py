import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from telethon import errors, functions, types

from ruby_ubot import sessions
from ruby_ubot.ledger import Ledger
from ruby_ubot.panel import register_panel
from ruby_ubot.sessions import Manager
from ruby_ubot.store import Store

from .support import Client, local_config


@pytest.mark.asyncio
async def test_two_step_login_resume_duplicates_and_logout(tmp_path, monkeypatch):
    manager = Manager(
        Store(tmp_path / "data.json", 7), Ledger(tmp_path / "ledger.sqlite"), local_config(tmp_path)
    )
    clients = iter([Client(11), Client(11), Client(22)])
    manager.make_client = lambda uid, slot: next(clients)
    assert "Berapa worker" in await manager.begin_login(7)
    assert "minimal" in await manager.submit(7, "1")
    assert "Worker 1/2" in await manager.submit(7, "2")
    await manager.submit(7, "+62811111111")
    client = manager.pending[7].client
    client.sign_in.side_effect = [errors.SessionPasswordNeededError(None), None]
    assert "dua langkah" in await manager.submit(7, "12345")
    assert "Worker 2/2" in await manager.submit(7, " secret ")
    client.sign_in.assert_awaited_with(password=" secret ")
    await manager.submit(7, "+62811111111")
    assert "sudah terhubung" in await manager.submit(7, "12345")
    assert len(manager.pool(7).accounts) == 1
    await manager.begin_login(7)
    assert "Worker 2/2" in await manager.submit(7, "2")
    await manager.submit(7, "+62822222222")
    assert "2 worker terhubung" in await manager.submit(7, "12345")
    assert 7 not in manager.pending and len(manager.pool(7).ordered) == 2
    await manager.logout(7)
    client.log_out.assert_awaited_once()
    await manager.ledger.close()


@pytest.mark.asyncio
async def test_restore_legacy_account_and_numbered_sessions(tmp_path, monkeypatch):
    manager = Manager(
        Store(tmp_path / "data.json", 7), Ledger(tmp_path / "ledger.sqlite"), local_config(tmp_path)
    )
    manager.session_dir.mkdir(exist_ok=True)
    for slot in [1, 2]:
        manager.session(7, slot).with_suffix(".session").touch()
    manager.make_client = lambda uid, slot: Client(slot)
    await manager.restore()
    assert [a.slot for a in manager.pool(7).ordered] == [1, 2]
    for a in manager.pool(7).ordered:
        a.client.catch_up.assert_awaited_once()
    await manager.shutdown()
    await manager.ledger.close()


@pytest.mark.asyncio
async def test_no_configured_worker_maximum_and_no_preallocation(tmp_path):
    manager = Manager(
        Store(tmp_path / "data.json", 7), Ledger(tmp_path / "ledger.sqlite"), local_config(tmp_path)
    )
    await manager.begin_login(7)
    assert "1/10000" in await manager.submit(7, "10000") and not manager.pool(7).accounts
    await manager.shutdown()
    await manager.ledger.close()


@pytest.mark.asyncio
async def test_login_panel_count_through_callback(tmp_path):
    store = Store(tmp_path / "data.json", 7)
    manager = Manager(store, Ledger(tmp_path / "ledger"), local_config(tmp_path))
    callbacks = {}

    def on(builder):
        def add(fn):
            callbacks[fn.__name__] = (fn, builder)
            return fn

        return add

    register_panel(NS(on=on), store, manager)
    e = NS(sender_id=7, raw_text="/login", is_private=True, respond=AsyncMock(), delete=AsyncMock())
    fn, builder = callbacks["login"]
    assert builder.pattern("/login")
    await fn(e)
    assert "Berapa worker" in e.respond.await_args.args[0]
    e.raw_text = "4"
    fn, builder = callbacks["secret"]
    assert builder.func(e)
    await fn(e)
    assert "Worker 1/4" in e.respond.await_args.args[0]
    await manager.shutdown()
    await manager.ledger.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "redirect", [errors.PhoneMigrateError, errors.NetworkMigrateError, errors.UserMigrateError]
)
async def test_real_telethon_login_redirect_completes_four_workers(tmp_path, monkeypatch, redirect):
    manager = Manager(
        Store(tmp_path / "cfg.json", 7), Ledger(tmp_path / "ledger"), local_config(tmp_path)
    )
    factory = manager.make_client
    attempts = {}

    def build(uid, slot):
        manager.session_dir.mkdir(exist_ok=True)
        client = factory(uid, slot)
        monkeypatch.setattr(
            type(client),
            "disconnected",
            property(lambda self: asyncio.get_running_loop().create_future()),
        )
        client.connect = AsyncMock()
        client.is_user_authorized = AsyncMock(return_value=False)
        identity = types.User(id=100 + slot, first_name="Worker")
        authorized = False

        async def get_me():
            return identity if authorized else None

        async def migrated(dc):
            client.session.set_dc(dc, "127.0.0.1", 443)
            client.session.save()

        async def login_done(user):
            nonlocal authorized
            authorized = True
            return user

        async def send(request, ordered=False):
            if isinstance(request, functions.auth.SendCodeRequest):
                attempts.setdefault(slot, []).append(client.session.dc_id)
                if client.session.dc_id != 4:
                    raise redirect(request, capture=4)
                return types.auth.SentCode(types.auth.SentCodeTypeApp(5), f"hash-{slot}")
            assert isinstance(request, functions.auth.SignInRequest)
            assert request.phone_code_hash == f"hash-{slot}" and request.phone_code == "12345"
            return types.auth.Authorization(user=identity)

        client._sender.send = send
        client._switch_dc = migrated
        client._on_login = login_done
        client.get_me = get_me
        return client

    manager.make_client = build
    try:
        await manager.begin_login(7)
        assert "Worker 1/4" in await manager.submit(7, "4")
        for slot in range(1, 5):
            assert "masukkan kode" in await manager.submit(7, f"+6281111111{slot}")
            assert (
                manager.pending[7].slot == slot and manager.pending[7].code_hash == f"hash-{slot}"
            )
            reply = await manager.submit(7, "12345")
            assert (f"Worker {slot + 1}/4" if slot < 4 else "4 worker terhubung") in reply
            assert len(attempts[slot]) == 2 and attempts[slot][-1] == 4
        assert len(manager.pool(7).accounts) == 4
    finally:
        await manager.shutdown()
        await manager.ledger.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("dcs", [[4, 4], [3, 4, 5]])
async def test_login_redirect_loop_bounded_and_keeps_slot(tmp_path, monkeypatch, dcs):
    manager = Manager(
        Store(tmp_path / "cfg", 7), Ledger(tmp_path / "ledger"), local_config(tmp_path)
    )
    client = Client(3)
    client.session = NS(dc_id=2)
    seq = iter(dcs)

    async def send(phone):
        dc = next(seq)
        client.session.dc_id = dc
        raise errors.PhoneMigrateError(None, capture=dc)

    client.send_code_request.side_effect = send
    manager.make_client = lambda uid, slot: client
    await manager.begin_login(7)
    await manager.submit(7, "4")
    assert "Worker 1/4" in await manager.submit(7, "+62811111111")
    assert client.send_code_request.await_count == len(dcs)
    assert manager.pending[7].stage == "phone"
    await manager.shutdown()
    await manager.ledger.close()


@pytest.mark.asyncio
async def test_expired_code_invalid_phone_and_flood(tmp_path, monkeypatch):
    manager = Manager(
        Store(tmp_path / "cfg", 7), Ledger(tmp_path / "ledger"), local_config(tmp_path)
    )
    good, expired, invalid, flood = [Client(i) for i in range(4)]
    expired.sign_in.side_effect = errors.PhoneCodeExpiredError(None)
    invalid.send_code_request.side_effect = errors.PhoneNumberInvalidError(None)
    flood.send_code_request.side_effect = errors.FloodWaitError(None, capture=31)
    clients = iter([good, expired, invalid, flood])
    manager.make_client = lambda uid, slot: next(clients)
    await manager.begin_login(7)
    await manager.submit(7, "4")
    await manager.submit(7, "+62811111111")
    await manager.submit(7, "12345")
    await manager.submit(7, "+62822222222")
    assert "Worker 2/4" in await manager.submit(7, "12345")
    assert "Worker 2/4" in await manager.submit(7, "+62833333333")
    assert "31 detik" in await manager.submit(7, "+62844444444")
    assert not manager.pending and len(manager.pool(7).accounts) == 1
    flood.send_code_request.assert_awaited_once()
    await manager.shutdown()
    await manager.ledger.close()


@pytest.mark.asyncio
async def test_malformed_session_and_failed_catchup_do_not_stop_restore(tmp_path, monkeypatch):
    import sqlite3

    mgr = Manager(Store(tmp_path / "cfg", 7), Ledger(tmp_path / "ledger"), local_config(tmp_path))
    mgr.session_dir.mkdir(exist_ok=True)
    for slot in range(1, 4):
        mgr.session(7, slot).with_suffix(".session").touch()
    a, b = Client(2), Client(3)
    a.catch_up.side_effect = ConnectionError("offline")

    def build(uid, slot):
        if slot == 1:
            raise sqlite3.DatabaseError("invalid session")
        return {2: a, 3: b}[slot]

    mgr.make_client = build
    await mgr.restore()
    assert list(mgr.pool(7).accounts) == [2, 3]
    b.catch_up.assert_awaited_once()
    assert mgr.session(7, 1).with_suffix(".session").exists()
    await mgr.shutdown()
    await mgr.ledger.close()


@pytest.mark.asyncio
async def test_slow_send_code_gives_user_full_time_to_enter_code(tmp_path, monkeypatch):
    monkeypatch.setattr(sessions, "LOGIN_TTL", 0.03)
    mgr = Manager(Store(tmp_path / "cfg", 7), Ledger(tmp_path / "ledger"), local_config(tmp_path))
    client = Client(1)

    async def sent(phone):
        await asyncio.sleep(0.07)
        return NS(phone_code_hash="hash")

    client.send_code_request.side_effect = sent
    mgr.make_client = lambda uid, slot: client
    await mgr.begin_login(7)
    await mgr.submit(7, "4")
    assert "masukkan kode" in await mgr.submit(7, "+62811111111")
    await asyncio.sleep(0.005)
    assert mgr.pending[7].stage == "code"
    await asyncio.sleep(0.05)
    assert not mgr.pending
    await mgr.shutdown()
    await mgr.ledger.close()


@pytest.mark.asyncio
async def test_already_authorized_on_send_code_continues_on_same_slot(tmp_path, monkeypatch):
    mgr = Manager(Store(tmp_path / "cfg", 7), Ledger(tmp_path / "ledger"), local_config(tmp_path))
    client = Client(1)
    client.send_code_request.side_effect = RuntimeError("logged in right after sending the code")
    mgr.make_client = lambda uid, slot: client
    await mgr.begin_login(7)
    await mgr.submit(7, "4")
    assert "Worker 2/4" in await mgr.submit(7, "+62811111111")
    assert mgr.pool(7).accounts[1].client is client
    client.sign_in.assert_not_awaited()
    await mgr.shutdown()
    await mgr.ledger.close()
