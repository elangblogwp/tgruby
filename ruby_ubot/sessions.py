import asyncio
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from math import ceil
from pathlib import Path
from weakref import WeakValueDictionary

from telethon import TelegramClient, errors, utils

from .connections import TRANSIENT, Connections
from .pool import Pool

LOGIN_TTL = 300
HISTORY_INTERVAL = 30
MIGRATION_ERRORS = (errors.PhoneMigrateError, errors.NetworkMigrateError, errors.UserMigrateError)
log = logging.getLogger(__name__)


@dataclass(slots=True)
class Login:
    stage: str = "count"
    slot: int = 0
    phone: str = field(default="", repr=False)
    code_hash: str = field(default="", repr=False)
    signed_in: bool = False
    client: object = None
    timer: object = None

    async def request(self, method, *args, **kwargs):
        redirects = set()
        while True:
            try:
                return await method(*args, **kwargs)
            except MIGRATION_ERRORS as exc:
                dc = exc.new_dc
                if dc != self.client.session.dc_id or dc in redirects or len(redirects) >= 2:
                    raise
                redirects.add(dc)


class Manager:
    def __init__(self, store, ledger, config):
        self.config = config
        self.session_dir = config.root / "sessions"
        self.panel_username = None
        self.store = store
        self.ledger = ledger
        self.workers = {}
        self.pending = {}
        self.locks = WeakValueDictionary()
        self.connections = Connections(self)
        self.by_account = {}

    def lock(self, uid):
        lock = self.locks.get(uid)
        if lock is None:
            self.locks[uid] = lock = asyncio.Lock()
        return lock

    def pool(self, uid):
        if uid not in self.workers:
            self.workers[uid] = Pool(uid, self.store, self.ledger, self.config.send_timeout)
            self.workers[uid].panel_username = self.panel_username
        return self.workers[uid]

    def session(self, uid, slot):
        return self.session_dir / (str(uid) if slot == 1 else f"{uid}_{slot}")

    def slots(self, uid):
        prefix = f"{uid}_"
        slots = {1} if self.session(uid, 1).with_suffix(".session").is_file() else set()
        for path in self.session_dir.glob(f"{uid}_*.session"):
            suffix = path.stem[len(prefix) :]
            if suffix.isascii() and suffix.isdecimal() and int(suffix) >= 2:
                slots.add(int(suffix))
        return sorted(slots)

    def make_client(self, uid, slot):
        return TelegramClient(
            str(self.session(uid, slot)),
            self.config.api_id,
            self.config.api_hash,
            catch_up=True,
            flood_sleep_threshold=0,
            connection_retries=3,
            request_retries=0,
            raise_last_call_error=True,
        )

    def purge(self, uid, slot):
        base = self.session(uid, slot).with_suffix(".session")
        for suffix in ("", "-journal", "-shm", "-wal"):
            Path(str(base) + suffix).unlink(missing_ok=True)

    async def attach(self, uid, slot, client):
        me = await client.get_me()
        if me is None:
            raise errors.UnauthorizedError(request=None, message="SESSION_UNAUTHORIZED", code=401)
        if getattr(me, "bot", False):
            raise ValueError("Gunakan akun pengguna Telegram.")
        pool = self.pool(uid)
        target = pool.target
        peer = None
        target_unavailable = False
        if target is not None:
            try:
                peer = await client.get_input_entity(target)
            except ValueError:
                try:
                    entity = await pool.commands.group_entity(client, target)
                    if getattr(entity, "left", False) or getattr(entity, "deactivated", False):
                        raise ValueError("Worker belum menjadi anggota grup.")
                    peer = utils.get_input_peer(entity)
                except (ValueError, errors.NotFoundError, errors.ChannelPrivateError):
                    target_unavailable = True
                except errors.FloodError as exc:
                    await pool.rate_limit(exc, target)
            except errors.FloodError as exc:
                await pool.rate_limit(exc, target)
        if target_unavailable and target == pool.target:
            await pool.pause("akses target perlu diperiksa dengan .addgc")
        if me.id in self.by_account:
            raise ValueError("Akun Telegram ini sudah terhubung sebagai worker.")
        pool.add(slot, me.id, client)
        account = pool.accounts[slot]
        if pool.target == target:
            account.peer = peer
        self.by_account[me.id] = uid
        self.connections.watch(pool, account)

    async def restore_once(self, uid, slot):
        client = None
        attached = False
        invalid = False
        try:
            client = self.make_client(uid, slot)
            async with asyncio.timeout(self.config.send_timeout):
                await client.connect()
                await self.attach(uid, slot, client)
            attached = True
            return True
        except errors.UnauthorizedError:
            invalid = True
        except errors.FloodError as exc:
            pool = self.pool(uid)
            if await pool.rate_limit(exc, pool.target):
                raise
        except TRANSIENT:
            raise
        except (ValueError, sqlite3.Error, errors.RPCError, RuntimeError):
            log.exception("Sesi worker %s dari %s belum bisa dipulihkan", slot, uid)
        finally:
            if client is not None and not attached:
                await client.disconnect()
            if invalid:
                self.purge(uid, slot)
        return False

    async def restore_slot(self, uid, slot):
        try:
            return await self.restore_once(uid, slot)
        except TRANSIENT as exc:
            log.warning("Pemulihan worker %s akan diulang (%s)", slot, type(exc).__name__)
            self.connections.retry(self.pool(uid), slot)
            return False

    async def catch_up(self, account):
        try:
            await account.client.catch_up()
        except (OSError, errors.RPCError, RuntimeError) as exc:
            log.warning("Sinkronisasi worker %s tertunda (%s)", account.slot, type(exc).__name__)

    async def restore(self):
        for uid in self.store.all_users():
            slots = self.slots(uid)
            if not slots:
                continue
            pool = self.pool(uid)
            async with self.lock(uid):
                pool.restoring = True
                try:
                    for i in range(0, len(slots), 4):
                        await asyncio.gather(
                            *(self.restore_slot(uid, slot) for slot in slots[i : i + 4])
                        )
                finally:
                    pool.restoring = False
                for account in pool.ordered:
                    await self.catch_up(account)

    def arm(self, uid, state):
        if state.timer:
            state.timer.cancel()
        state.timer = asyncio.create_task(self.expire(uid, state))

    async def expire(self, uid, state):
        await asyncio.sleep(LOGIN_TTL)
        async with self.lock(uid):
            if self.pending.get(uid) is state:
                await self.drop(uid)

    async def drop(self, uid):
        state = self.pending.pop(uid, None)
        if state is None:
            return
        if state.timer and state.timer is not asyncio.current_task():
            state.timer.cancel()
        await self.release_login_client(uid, state)

    async def release_login_client(self, uid, state):
        if state.client is not None:
            try:
                if state.signed_in:
                    await state.client.log_out()
            except (OSError, errors.RPCError):
                log.warning("worker %s: could not revoke the incomplete login", state.slot)
            finally:
                await state.client.disconnect()
                self.purge(uid, state.slot)
                state.client = None
                state.signed_in = False
        state.phone = ""
        state.code_hash = ""

    async def retry_slot(self, uid, state, reason):
        await self.release_login_client(uid, state)
        return f"{reason}\n{self.prompt_phone(self.pool(uid), state)}"

    async def begin_login(self, uid):
        async with self.lock(uid):
            if not self.store.is_authorized(uid):
                return "Akses ditolak."
            wait = self.ledger.scope_remaining(f"login/{uid}")
            if wait:
                return f"Telegram membatasi login. Coba /login setelah {ceil(wait)} detik."
            await self.drop(uid)
            state = self.pending[uid] = Login()
            self.arm(uid, state)
            connected = len(self.pool(uid).accounts)
            return f"Berapa worker yang ingin dipakai? Minimal {max(2, connected)}. Sudah terhubung: {connected}."

    def prompt_phone(self, pool, state):
        state.slot = 1
        while state.slot in pool.accounts:
            state.slot += 1
        state.stage = "phone"
        return f"Worker {state.slot}/{pool.goal}: kirim nomor Telegram dengan kode negara."

    async def submit(self, uid, text, expected=None):
        async with self.lock(uid):
            state = self.pending.get(uid)
            if state is None or (
                expected is not None
                and (state is not expected[0] or (state.slot, state.stage) != expected[1:])
            ):
                return None
            if not self.store.is_authorized(uid):
                await self.drop(uid)
                return "Akses ditolak."
            pool = self.pool(uid)
            if state.timer:
                state.timer.cancel()
            try:
                if state.stage == "count":
                    raw = text.strip()
                    if not re.fullmatch(r"[0-9]+", raw) or int(raw) < max(2, len(pool.accounts)):
                        return f"Kirim angka minimal {max(2, len(pool.accounts))}."
                    count = int(raw)
                    if count != pool.goal:
                        pool.invalidate()
                    self.store.settings(uid)["worker_count"] = count
                    await pool.commit()
                    if len(pool.accounts) == count:
                        await self.drop(uid)
                        return f"{count} worker siap. Pilih satu grup dengan .addgc <id>."
                    return self.prompt_phone(pool, state)
                if state.stage == "phone":
                    phone = re.sub(r"[\s()-]", "", text.strip())
                    if not re.fullmatch(r"\+?[0-9]{7,15}", phone):
                        return "Nomor tidak valid. Sertakan kode negara."
                    path = self.session(uid, state.slot).with_suffix(".session")
                    if path.exists():
                        if await self.restore_slot(uid, state.slot):
                            if len(pool.accounts) == pool.goal:
                                await self.drop(uid)
                                return f"{pool.goal} worker terhubung."
                            return f"Sesi worker {state.slot} berhasil dipulihkan.\n{self.prompt_phone(pool, state)}"
                        if path.exists():
                            return f"Sesi worker {state.slot} belum bisa dibuka. Periksa log lalu kirim kembali nomor."
                    state.phone = "+" + phone.lstrip("+")
                    state.client = self.make_client(uid, state.slot)
                    await asyncio.wait_for(state.client.connect(), self.config.send_timeout)
                    try:
                        result = await asyncio.wait_for(
                            state.request(state.client.send_code_request, state.phone),
                            self.config.send_timeout,
                        )
                    except RuntimeError:
                        if (
                            await asyncio.wait_for(state.client.get_me(), self.config.send_timeout)
                            is None
                        ):
                            raise
                    else:
                        state.code_hash = result.phone_code_hash
                        state.stage = "code"
                        return f"Worker {state.slot}: masukkan kode verifikasi Telegram."
                elif state.stage == "code":
                    code = "".join(text.split())
                    if not re.fullmatch(r"[0-9]+", code):
                        return "Kirim kode verifikasi numerik."
                    async with asyncio.timeout(self.config.send_timeout):
                        await state.request(
                            state.client.sign_in, state.phone, code, phone_code_hash=state.code_hash
                        )
                else:
                    async with asyncio.timeout(self.config.send_timeout):
                        await state.request(state.client.sign_in, password=text)
                state.signed_in = True
                async with asyncio.timeout(self.config.send_timeout):
                    await self.attach(uid, state.slot, state.client)
                state.client = None
                state.signed_in = False
                if len(pool.accounts) == pool.goal:
                    await self.drop(uid)
                    return f"{pool.goal} worker terhubung. Gunakan .addgc <id>, .keyword, .text dan .open."
                return self.prompt_phone(pool, state)
            except errors.SessionPasswordNeededError:
                state.stage = "password"
                return f"Worker {state.slot}: masukkan password verifikasi dua langkah."
            except errors.PhoneCodeExpiredError:
                return await self.retry_slot(
                    uid, state, "Kode kedaluwarsa. Minta kode baru untuk worker ini."
                )
            except (errors.PhoneNumberInvalidError, errors.PhoneNumberBannedError) as exc:
                return await self.retry_slot(
                    uid, state, f"Telegram menolak nomor tersebut ({type(exc).__name__})."
                )
            except MIGRATION_ERRORS:
                return await self.retry_slot(
                    uid, state, "Pengalihan pusat data belum selesai. Coba kembali."
                )
            except (errors.PhoneCodeInvalidError, errors.PasswordHashInvalidError):
                return "Kode atau password salah. Silakan ulangi."
            except errors.FloodError as exc:
                seconds = getattr(exc, "seconds", None)
                if seconds is not None:
                    ticket = self.ledger.defer_scopes((f"login/{uid}",), max(1, seconds))
                    if ticket is not None:
                        await asyncio.shield(ticket)
                await self.drop(uid)
                return f"Telegram membatasi login. Coba /login setelah {getattr(exc, 'seconds', '?')} detik."
            except ValueError as exc:
                if state.stage != "count":
                    return await self.retry_slot(uid, state, f"Login gagal: {exc}.")
                return "Jumlah worker tidak valid."
            except (TimeoutError, errors.RPCError, OSError, RuntimeError, sqlite3.Error) as exc:
                log.warning(
                    "owner %s worker %s: login failed (%s)", uid, state.slot, type(exc).__name__
                )
                await self.drop(uid)
                return f"Login gagal ({type(exc).__name__}). Lanjutkan dengan /login."
            finally:
                if self.pending.get(uid) is state:
                    self.arm(uid, state)

    async def logout(self, uid):
        async with self.lock(uid):
            await self.drop(uid)
            pool = self.workers.pop(uid, None)
            if pool:
                pool.stopped = True
                for user_id in pool.own_ids:
                    self.by_account.pop(user_id, None)
            await self.connections.stop(uid)
            if pool:
                await pool.stop(logout=True)
            cfg = self.store.data["settings"].get(str(uid), {})
            if cfg.pop("history_window", None) is not None:
                await self.store.save()
            for slot in self.slots(uid):
                self.purge(uid, slot)
            return "Login dibatalkan dan seluruh sesi worker lokal dihapus."

    async def monitor_history(self):
        while True:
            for pool in tuple(self.workers.values()):
                pool.start_history_check()
            await asyncio.sleep(HISTORY_INTERVAL)

    async def shutdown(self):
        await self.connections.stop()
        for uid in list(self.pending):
            async with self.lock(uid):
                await self.drop(uid)
        for pool in self.workers.values():
            await pool.stop()
