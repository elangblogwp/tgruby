import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon import TelegramClient, errors, events, types

from ruby_ubot.help import EMOJI_RE, panel_page
from ruby_ubot.ledger import Ledger
from ruby_ubot.panel import register_panel
from ruby_ubot.sessions import Manager
from ruby_ubot.store import Store

from .support import CHAT, Client, emit, local_config, post


@pytest.mark.asyncio
async def test_callback_navigation_and_live_permissions(tmp_path):
    ledger = Ledger(tmp_path / "db")
    store = Store(tmp_path / "cfg", 7)
    manager = Manager(store, ledger, local_config(tmp_path))
    bot = TelegramClient(None, 1, "test")
    bot.edit_message = AsyncMock()
    register_panel(bot, store, manager)
    handler, builder = next(
        (fn, b) for fn, b in bot.list_event_handlers() if isinstance(b, events.CallbackQuery)
    )
    await builder.resolve(bot)

    async def click(uid, peer, data):
        update = types.UpdateBotCallbackQuery(501, uid, peer, 23, 0, data=data)
        event = builder.build(update)
        assert builder.filter(event)
        event.answer = AsyncMock()
        event.get_input_chat = AsyncMock(return_value=types.InputPeerUser(uid, 0))
        before = bot.edit_message.await_count
        await handler(event)
        event.answer.assert_awaited_once()
        return bot.edit_message.await_args if bot.edit_message.await_count > before else None

    try:
        root = await click(7, types.PeerUser(7), b"help:home")
        _, groups = panel_page("home", owner=True)
        assert bytes(bot.build_reply_markup(root.kwargs["buttons"]))
        assert [[button.type.data for button in row] for row in root.kwargs["buttons"]] == [
            [button.type.data for button in row] for row in groups
        ]
        for row in groups:
            for button in row:
                sub = await click(7, types.PeerUser(7), button.type.data)
                assert sub.args[1] == 23
                assert sub.kwargs["buttons"][0][0].type.data == b"help:home"
                assert not EMOJI_RE.search(sub.args[2] + button.text)
        assert await click(7, types.PeerUser(7), b"help:home") is not None
        assert await click(7, types.PeerChannel(1), b"help:users") is None
        assert await click(7, types.PeerUser(7), b"help:missing") is None
        await store.add_user(8)
        assert await click(8, types.PeerUser(8), b"help:promo") is not None
        assert await click(8, types.PeerUser(8), b"help:users") is None
        await store.remove_user(8)
        assert await click(8, types.PeerUser(8), b"help:promo") is None
        worker = Client(9)
        await manager.attach(7, 1, worker)
        assert await click(9, types.PeerUser(9), b"help:group") is not None
        assert await click(9, types.PeerUser(9), b"help:work") is None
        bot.edit_message.side_effect = errors.MessageNotModifiedError(None)
        await click(7, types.PeerUser(7), b"help:home")
    finally:
        await manager.shutdown()
        await ledger.close()
        await bot.disconnect()


@pytest.mark.asyncio
async def test_ui_without_emoji_and_unmodified_promotion(pool):
    pool.panel_username = "RubyExampleBot"
    worker = pool.ordered[0]
    fn, builder = worker.client.handlers[-1]

    async def command(raw, reply=None):
        event = SimpleNamespace(
            pattern_match=builder.pattern(raw),
            edit=AsyncMock(),
            chat_id=CHAT,
            reply_to_msg_id=reply,
            raw_text=raw,
        )
        await fn(event)
        return event.edit.await_args.args[0] if event.edit.await_args else None

    assert "https://t.me/RubyExampleBot?start=help" in await command(".help")
    assert "✨" not in await command(".text ready ✨")
    assert "✨" not in await command(".text")
    await emit(pool, post(12, text="samsung 🤙"))
    assert worker.client.sent[-1].message == "ready ✨ 🤙"
    assert "disimpan" in await command(".text .close")
    await emit(pool, post(13, text="samsung"))
    assert worker.client.sent[-1].message == ".close"
    assert await command(".close", reply=13) is None and pool.active
    assert await command(".close", reply=12) == "Autoreply dijeda."


@pytest.mark.asyncio
async def test_replaced_login_ignores_delayed_private_input(tmp_path):
    ledger = Ledger(tmp_path / "db")
    manager = Manager(Store(tmp_path / "cfg", 7), ledger, local_config(tmp_path))
    bot = TelegramClient(None, 1, "test")
    register_panel(bot, manager.store, manager)
    callback, builder = next(
        (fn, b) for fn, b in bot.list_event_handlers() if fn.__name__ == "secret"
    )
    gate, entered = asyncio.Event(), asyncio.Event()

    async def deleting():
        entered.set()
        await gate.wait()

    e = SimpleNamespace(
        sender_id=7, is_private=True, raw_text="4", delete=deleting, respond=AsyncMock()
    )
    try:
        await manager.begin_login(7)
        task = asyncio.create_task(callback(e))
        await entered.wait()
        await manager.begin_login(7)
        gate.set()
        await task
        assert manager.pending[7].stage == "count"
        e.respond.assert_not_awaited()
        e.raw_text = "/my-password"
        assert builder.func(e)
        for raw in ("/help", "/start help", "/logout"):
            e.raw_text = raw
            assert not builder.func(e)
    finally:
        gate.set()
        await manager.shutdown()
        await ledger.close()
        await bot.disconnect()
