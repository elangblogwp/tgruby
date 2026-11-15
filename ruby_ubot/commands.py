import asyncio
import logging
import random
import re
import time

from telethon import errors, functions, utils
from telethon.tl.types import Channel, Chat

from .config import MAX_REPLY_LEN
from .help import admin_text, display_value, worker_help
from .text import normalize, parse_group_id, split_text

log = logging.getLogger(__name__)


class WorkerCommands:
    def __init__(self, pool):
        self.pool = pool

    async def on_command(self, account, event):
        if self.pool.stopped or not self.pool.store.is_authorized(self.pool.uid):
            return
        parent = getattr(event, "reply_to_msg_id", None)
        if (
            parent
            and (account.user_id, event.chat_id, parent, event.raw_text)
            in self.pool.command_replies
        ):
            return
        name = event.pattern_match.group(1).lower()
        arg = event.pattern_match.group(2) or ""
        try:
            if name == "ping":
                text = await self.cmd_ping(account)
            else:
                text = await getattr(self, f"cmd_{name}")(arg)
            chunks = split_text(admin_text(text))
            try:
                await event.edit(chunks[0], parse_mode=None, link_preview=False)
            except errors.MessageNotModifiedError:
                pass
            for chunk in chunks[1:]:
                await account.client.send_message(event.chat_id, chunk, parse_mode=None)
        except errors.FloodError as exc:
            await self.pool.rate_limit(exc, self.pool.target)
        except Exception:
            log.exception("owner %s: command %s failed", self.pool.uid, name)
            await event.respond("Perintah gagal. Periksa log bot.")

    async def cmd_help(self, arg):
        return worker_help(self.pool.panel_username)

    async def cmd_ping(self, account):
        pool = self.pool
        start = time.perf_counter()
        async with asyncio.timeout(pool.send_timeout):
            await account.client(functions.PingRequest(ping_id=random.getrandbits(62)))
        dispatch = "-" if pool.dispatch_ms is None else f"{pool.dispatch_ms:.2f} ms"
        ack = "-" if pool.ack_ms is None else f"{pool.ack_ms:.2f} ms"
        if not pool.active:
            status = "Dijeda"
        elif len(pool.accounts) != pool.goal or pool.restoring:
            status = "Menunggu worker"
        elif not pool.target or not pool.pattern or not pool.texts:
            status = "Perlu konfigurasi"
        elif pool.ledger.remaining(pool.uid, pool.target):
            status = "Menunggu Telegram"
        else:
            status = "Aktif"
        return (
            f"Ruby UBot  ·  Ping {(time.perf_counter() - start) * 1000:.0f} ms\n\n"
            f"Worker {len(pool.accounts)}/{pool.goal}  ·  {status}\n"
            f"Target {pool.target or 'belum dipilih'}\n"
            f"Jeda Telegram {pool.ledger.remaining(pool.uid, pool.target):.1f} detik\n\n"
            f"Event ke kirim  {dispatch}\nKirim ke konfirmasi  {ack}\n\n"
            f"Terkirim {pool.sent}  ·  Dipulihkan {pool.recovered}\n"
            f"Diproses {len(pool.jobs)}  ·  Ulang {pool.retries}\n"
            f"Gagal atau belum pasti {pool.failed}"
        )

    async def cmd_listgc(self, arg):
        targets = self.pool.settings["targets"]
        return (
            "\n".join(f"{display_value(name)} ({gid})" for gid, name in targets.items())
            or "Belum ada target. Gunakan .addgc <id>."
        )

    @staticmethod
    async def group_entity(client, gid):
        try:
            return await client.get_entity(gid)
        except ValueError:
            async for dialog in client.iter_dialogs(ignore_migrated=True):
                if dialog.is_group and dialog.id == gid:
                    return dialog.entity
            raise ValueError("Grup tidak ditemukan dalam daftar dialog.")

    async def cmd_addgc(self, arg):
        gid = parse_group_id(arg)
        if not gid:
            return "Gunakan .addgc <id grup>."
        accounts = self.pool.ordered
        epoch = self.pool.epoch
        peers = []
        for account in accounts:
            try:
                entity = await self.group_entity(account.client, gid)
            except (ValueError, errors.NotFoundError, errors.ChannelPrivateError):
                return f"Worker {account.slot} tidak dapat mengakses grup tersebut."
            if not (isinstance(entity, Chat) or isinstance(entity, Channel) and entity.megagroup):
                return "ID tersebut bukan grup."
            if getattr(entity, "left", False) or getattr(entity, "deactivated", False):
                return f"Worker {account.slot} belum menjadi anggota grup."
            peers.append(utils.get_input_peer(entity))
        if self.pool.epoch != epoch or accounts != self.pool.ordered or not peers:
            return "Daftar worker atau target berubah. Ulangi perintah."
        self.pool.invalidate()
        for account, peer in zip(accounts, peers):
            account.peer = peer
        self.pool.settings["targets"] = {str(gid): utils.get_display_name(entity)}
        await self.pool.commit()
        return f"Target grup\n{display_value(utils.get_display_name(entity))} ({gid})"

    async def cmd_rmgc(self, arg):
        if self.pool.target is None or parse_group_id(arg) != self.pool.target:
            return "Gunakan .rmgc <id grup target>."
        self.pool.invalidate()
        self.pool.settings["targets"] = {}
        for account in self.pool.ordered:
            account.peer = None
        await self.pool.commit()
        return "Target dihapus."

    async def cmd_keyword(self, arg):
        arg = arg.strip()
        cfg = self.pool.settings
        if not arg:
            return "Keyword\n" + (", ".join(map(display_value, cfg["keywords"])) or "Belum ada.")
        if arg.lower() == "clear":
            words = []
        else:
            words = list({key: w.strip() for w in arg.split(",") if (key := normalize(w))}.values())
            if not words:
                return "Masukkan keyword atau gunakan .keyword clear."
        keep = {normalize(w) for w in words}
        cfg["keywords"] = words
        cfg["prices"] = {k: v for k, v in cfg["prices"].items() if normalize(k) in keep}
        await self.pool.commit()
        return f"{len(words)} keyword disimpan." if words else "Daftar keyword dikosongkan."

    async def cmd_text(self, arg):
        cfg = self.pool.settings
        if not arg:
            return "Teks promosi\n" + (
                "\n".join(f"{i}. {display_value(t)}" for i, t in enumerate(cfg["texts"], 1))
                or "Belum ada."
            )
        texts = [] if arg.strip().lower() == "clear" else [t for t in arg.split("|") if t.strip()]
        if not texts and arg.strip().casefold() != "clear":
            return "Masukkan teks promosi atau gunakan .text clear."
        if any(len(t) > MAX_REPLY_LEN for t in texts):
            return f"Teks promosi maksimal {MAX_REPLY_LEN} karakter."
        cfg["texts"] = texts
        await self.pool.commit()
        response = f"{len(texts)} teks promosi disimpan." if texts else "Teks promosi dikosongkan."
        if texts:
            shortest = min(map(len, texts))
            too_long = [
                word
                for word, price in self.pool.prices
                if len(price) + 2 + shortest > MAX_REPLY_LEN
            ]
            if too_long:
                response += (
                    "\n\nHarga dan promosi melebihi batas untuk " + ", ".join(too_long) + "."
                )
        return response

    async def cmd_addprice(self, arg):
        arg = arg.strip()
        cfg = self.pool.settings
        words = {normalize(word): word for word in cfg["keywords"] if normalize(word)}
        prices = {normalize(key): value for key, value in cfg["prices"].items()}
        usage = ".addprice <keyword> <teks>"
        if not arg:
            rows = [
                f"{display_value(word)}: {display_value(prices[key])}"
                for key, word in words.items()
                if key in prices
            ]
            return usage + "\n\n" + ("\n".join(rows) or "Belum ada harga.")
        if not words:
            return "Atur keyword dengan .keyword terlebih dahulu."
        target = None
        text = ""
        longest = max(map(len, words))
        for boundary in re.finditer(r"\s+|$", arg):
            key = normalize(arg[: boundary.start()])
            if len(key) > longest:
                break
            if key in words:
                target = key
                text = arg[boundary.end() :].strip()
        if target is None:
            return f"Keyword belum terdaftar. Pilih dari {', '.join(words.values())}\n{usage}"
        word = display_value(words[target])
        if not text:
            price = display_value(prices[target]) if target in prices else "Belum ada harga."
            return f"Harga {word}\n{price}"
        if text.casefold() == "clear":
            cfg["prices"] = {
                key: value for key, value in cfg["prices"].items() if normalize(key) != target
            }
            await self.pool.commit()
            return f"Harga {word} dihapus."
        shortest = min((len(t) for t in self.pool.texts), default=1)
        if len(text) + 2 + shortest > MAX_REPLY_LEN:
            return f"Harga dan teks promosi harus muat dalam {MAX_REPLY_LEN} karakter."
        cfg["prices"] = {
            key: value for key, value in cfg["prices"].items() if normalize(key) != target
        }
        cfg["prices"][target] = text
        await self.pool.commit()
        return f"Harga {word} disimpan."

    async def cmd_open(self, arg):
        if len(self.pool.accounts) != self.pool.goal or self.pool.target is None:
            return "Lengkapi worker dengan /login dan pilih satu grup dengan .addgc <id>."
        if not self.pool.pattern or not self.pool.texts:
            return "Atur .keyword dan .text sebelum mengaktifkan autoreply."
        self.pool.settings["active"] = True
        await self.pool.commit()
        return "Autoreply aktif."

    async def cmd_close(self, arg):
        await self.pool.pause(".close")
        return "Autoreply dijeda."
