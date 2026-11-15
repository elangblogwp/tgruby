import re

from telethon import errors, events

from .help import panel_page
from .text import split_text


def register_panel(bot, store, manager):
    def private(event):
        return event.is_private

    def waiting(event):
        return (
            event.is_private
            and event.sender_id in manager.pending
            and not re.match(
                r"(?i)^/(?:help(?:@\w+)?$|(?:start|login|logout|adduser|deluser|users)(?:@\w+)?(?:\s|$))",
                event.raw_text or "",
            )
        )

    async def deny(event):
        await event.respond(f"Akses belum aktif. Kirim ID {event.sender_id} ke pemilik bot.")

    def page_for(uid, key):
        authorized = store.is_authorized(uid)
        if not authorized and uid not in manager.by_account:
            return None
        return panel_page(key, owner=uid == store.owner_id, worker_only=not authorized)

    @bot.on(
        events.NewMessage(
            incoming=True,
            pattern=r"(?i)^/(?:help(?:@\w+)?|start(?:@\w+)?(?:\s+help)?)$",
            func=private,
        )
    )
    async def start(event):
        page = page_for(event.sender_id, "home")
        if page is None:
            return await deny(event)
        text, buttons = page
        await event.respond(text, buttons=buttons, parse_mode=None)

    @bot.on(events.CallbackQuery(pattern=rb"^help:([a-z]+)$"))
    async def help_page(event):
        uid = event.sender_id
        key = event.pattern_match.group(1).decode("ascii")
        page = page_for(uid, key) if event.chat_id == uid else None
        if page is None:
            await event.answer("Halaman ini tidak tersedia.", alert=True)
            return
        text, buttons = page
        try:
            await event.answer()
        except errors.QueryIdInvalidError:
            return
        try:
            await bot.edit_message(
                await event.get_input_chat(),
                event.message_id,
                text,
                buttons=buttons,
                parse_mode=None,
            )
        except (errors.MessageNotModifiedError, errors.MessageIdInvalidError):
            pass

    @bot.on(
        events.NewMessage(
            incoming=True, pattern=r"(?is)^/login(?:@\w+)?(?:\s+(.+))?$", func=private
        )
    )
    async def login(event):
        uid = event.sender_id
        if not store.is_authorized(uid):
            return await deny(event)
        await event.respond(await manager.begin_login(uid))

    @bot.on(events.NewMessage(incoming=True, func=waiting))
    async def secret(event):
        uid = event.sender_id
        state = manager.pending.get(uid)
        if state is None:
            return
        expected = state, state.slot, state.stage
        text = event.raw_text or ""
        try:
            await event.delete()
        except errors.RPCError:
            pass
        if uid not in manager.pending:
            return
        reply = await manager.submit(uid, text, expected)
        if reply:
            await event.respond(reply)

    @bot.on(events.NewMessage(incoming=True, pattern=r"(?i)^/logout(?:@\w+)?$", func=private))
    async def logout(event):
        uid = event.sender_id
        if not store.is_authorized(uid):
            return await deny(event)
        await event.respond(await manager.logout(uid))

    @bot.on(
        events.NewMessage(
            incoming=True, pattern=r"(?i)^/adduser(?:@\w+)?(?:\s+(\d+))?$", func=private
        )
    )
    async def adduser(event):
        if event.sender_id != store.owner_id:
            return await event.respond("Khusus pemilik bot.")
        raw = event.pattern_match.group(1)
        if not raw:
            return await event.respond("Gunakan /adduser <id pengguna>.")
        uid = int(raw)
        if not await store.add_user(uid):
            return await event.respond("Pengguna sudah memiliki akses.")
        await event.respond(f"Akses diberikan kepada {uid}.")
        try:
            await bot.send_message(uid, "Akses aktif. Gunakan /login untuk menghubungkan worker.")
        except (errors.RPCError, OSError):
            pass

    @bot.on(
        events.NewMessage(
            incoming=True, pattern=r"(?i)^/deluser(?:@\w+)?(?:\s+(\d+))?$", func=private
        )
    )
    async def deluser(event):
        if event.sender_id != store.owner_id:
            return await event.respond("Khusus pemilik bot.")
        raw = event.pattern_match.group(1)
        if not raw:
            return await event.respond("Gunakan /deluser <id pengguna>.")
        uid = int(raw)
        if uid == store.owner_id:
            return await event.respond("Akses pemilik selalu aktif.")
        pool = manager.workers.get(uid)
        if pool is not None:
            pool.stopped = True
            pool.invalidate()
        removed = await store.remove_user(uid)
        if removed:
            await manager.logout(uid)
        await event.respond(
            f"Akses {uid} dicabut." if removed else "Pengguna belum memiliki akses."
        )

    @bot.on(events.NewMessage(incoming=True, pattern=r"(?i)^/users(?:@\w+)?$", func=private))
    async def users(event):
        if event.sender_id != store.owner_id:
            return await event.respond("Khusus pemilik bot.")
        rows = [
            f"{uid} - {'terhubung' if uid in manager.workers and manager.workers[uid].accounts else 'terputus'}"
            for uid in store.all_users()
        ]
        for chunk in split_text("Pengguna\n\n" + "\n".join(rows)):
            await event.respond(chunk)
