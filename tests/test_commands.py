import re
from datetime import UTC
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from telethon import types

from ruby_ubot.help import COMMAND_RE, WORKER_HELP, panel_page
from ruby_ubot.ledger import Ledger
from ruby_ubot.panel import register_panel
from ruby_ubot.sessions import Manager
from ruby_ubot.store import Store
from ruby_ubot.text import EMOJI_RE

from .support import CHAT, emit, local_config, post


@pytest.mark.asyncio
async def test_setting_from_other_worker_and_help(pool):
    raw = ".addprice samsung 2d\n8u\n30k"
    e = NS(pattern_match=re.match(COMMAND_RE, raw), edit=AsyncMock(), chat_id=CHAT)
    await pool.accounts[3].client.handlers[-1][0](e)
    await emit(pool, post(text="samsung"))
    assert pool.accounts[1].client.sent[0].message == "2d\n8u\n30k\n\nready stock!"
    assert ".addallgc" not in await pool.commands.cmd_help("")


@pytest.mark.asyncio
async def test_group_replace_and_remove(pool):
    from datetime import datetime

    entity = types.Chat(
        id=23,
        title="New",
        photo=types.ChatPhotoEmpty(),
        participants_count=3,
        date=datetime.now(UTC),
        version=1,
    )
    for a in pool.ordered:
        a.client.get_entity = AsyncMock(return_value=entity)
    assert "New" in await pool.commands.cmd_addgc("-23") and pool.target == -23
    await emit(pool, post())
    assert pool.sent == 0
    await pool.commands.cmd_rmgc("-23")
    assert pool.target is None


def test_ambiguous_old_targets_and_legacy_prefix(tmp_path):
    s = Store(tmp_path / "data.json", 7)
    s.data["settings"]["7"] = {
        "keywords": ["noktel"],
        "price_text": "15k",
        "targets": {"-1": "A", "-2": "B"},
    }
    c = s.settings(7)
    assert c["targets"] == {} and c["prices"] == {"noktel": "15k"} and c["worker_count"] == 2


@pytest.mark.asyncio
async def test_cold_dialog_cache_finds_joined_group(pool):
    from datetime import datetime

    entity = types.Chat(
        id=23,
        title="Grup",
        photo=types.ChatPhotoEmpty(),
        participants_count=3,
        date=datetime.now(UTC),
        version=1,
    )

    async def dialogs(**kwargs):
        yield NS(is_group=True, id=-23, entity=entity)

    for a in pool.ordered:
        a.client.get_entity = AsyncMock(side_effect=ValueError("entity cache miss"))
        a.client.iter_dialogs = dialogs
    assert "Grup" in await pool.commands.cmd_addgc("-23")
    assert pool.target == -23


@pytest.mark.asyncio
async def test_empty_separators_keep_config(pool):
    cfg = pool.store.settings(7)
    await pool.commands.cmd_keyword(", ,")
    await pool.commands.cmd_text("| |")
    assert (
        cfg["keywords"] == ["noktel", "samsung"]
        and cfg["texts"] == ["ready stock!"]
        and "noktel" in cfg["prices"]
    )


@pytest.mark.asyncio
async def test_panel_permissions_and_minimal_ui(tmp_path):
    store = Store(tmp_path / "data.json", 7)
    manager = Manager(store, Ledger(tmp_path / "ledger"), local_config(tmp_path))
    callbacks = {}

    def on(builder):
        def register(fn):
            callbacks[fn.__name__] = (fn, builder)
            return fn

        return register

    bot = NS(on=on, send_message=AsyncMock())
    register_panel(bot, store, manager)

    async def call(name, uid, raw):
        fn, builder = callbacks[name]
        event = NS(
            sender_id=uid,
            raw_text=raw,
            is_private=True,
            respond=AsyncMock(),
            pattern_match=builder.pattern(raw),
        )
        await fn(event)
        return event.respond.await_args.args[0]

    try:
        assert "Bantuan" in await call("start", 7, "/start")
        assert "Akses belum aktif" in await call("start", 8, "/start")
        assert "Akses diberikan" in await call("adduser", 7, "/adduser 8")
        assert "/adduser" not in await call("start", 8, "/start")
        assert "Khusus" in await call("users", 8, "/users")
        assert "8" in await call("users", 7, "/users")
        assert "dicabut" in await call("deluser", 7, "/deluser 8")
        assert "Akses belum aktif" in await call("logout", 8, "/logout")
        commands = re.findall(r"\.\w+", WORKER_HELP)
        assert len(commands) == len(set(commands)) == 10
        assert not EMOJI_RE.search(WORKER_HELP + panel_page("users", owner=True)[0])
    finally:
        await manager.shutdown()
        await manager.ledger.close()


@pytest.mark.asyncio
async def test_worker_commands_through_registered_handler(pool):
    worker = pool.ordered[0]
    command, builder = worker.client.handlers[-1]

    async def run(raw):
        event = NS(pattern_match=builder.pattern(raw), edit=AsyncMock(), chat_id=CHAT)
        await command(event)
        return event.edit.await_args.args[0]

    assert ".addprice" in await run(".help")
    assert str(CHAT) in await run(".listgc")
    assert "Ping " in await run(".ping")
    assert "dijeda" in await run(".close")
    assert "aktif" in await run(".open")
    assert "noktel" in await run(".keyword")
    assert "ready stock!" in await run(".text")
    assert "15k" in await run(".addprice noktel")
    assert "dihapus" in await run(".rmgc " + str(CHAT))
    assert "Belum ada target" in await run(".listgc")
