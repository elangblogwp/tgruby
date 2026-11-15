import asyncio
import logging
import random
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime

from telethon import errors, events, functions, utils
from telethon.tl.types import InputReplyToMessage, PeerChannel

from .commands import WorkerCommands
from .config import MAX_POST_LEN, MAX_REPLY_LEN
from .help import COMMAND_RE
from .history import check_history
from .text import build_reply, match_keyword, normalize

log = logging.getLogger(__name__)
SEEN_LIMIT = 4096
SERVICE_ID = 777000
PAUSED_JOB_LIMIT = 32


@dataclass(slots=True)
class Account:
    slot: int
    user_id: int
    client: object
    peer: object = None


@dataclass(slots=True)
class Delivery:
    message: object
    revision: int
    received: float
    text: str | None
    armed: bool = False
    account: object = None
    local_copy: object = None
    copies: object = None
    copy_ready: object = None
    attempted: bool = False
    random_id: int = 0


class Pool:
    def __init__(self, uid, store, ledger, send_timeout=30.0):
        self.send_timeout = send_timeout
        self.panel_username = None
        self.commands = WorkerCommands(self)
        self.uid = uid
        self.store = store
        self.ledger = ledger
        self.accounts = {}
        self.ordered = ()
        self.own_ids = set()
        self.jobs = {}
        self.recent = OrderedDict()
        self.command_replies = OrderedDict()
        self.last_shared_id = None
        self.last_shared_key = None
        self.handlers = {}
        self.tasks = {}
        self.epoch = 0
        self.stopped = False
        self.restoring = False
        self.sent = self.failed = self.retries = self.recovered = 0
        self.dispatch_ms = self.ack_ms = None
        self.revision = 0
        self.last_history_start = float("-inf")
        self.reader_index = 0
        self.history_task = None
        self.pause_timer = None
        self.history_config = None
        self.initial_write = self.store.queue_save() if self.reload() else None

    def reload(self):
        new_window = False
        cfg = self.settings = self.store.settings(self.uid)
        self.goal = cfg["worker_count"]
        self.active = cfg["active"]
        self.target = next((int(gid) for gid in cfg["targets"]), None)
        words = sorted(
            dict.fromkeys(filter(None, map(normalize, cfg["keywords"]))), key=len, reverse=True
        )
        self.pattern = re.compile("|".join(map(re.escape, words))) if words else None
        prices = {normalize(k): v for k, v in cfg["prices"].items() if v.strip()}
        self.prices = [(w, prices[w]) for w in words if w in prices]
        self.texts = tuple(t for t in cfg["texts"] if t.strip())
        self.revision += 1
        scope = [self.target, bool(self.active and words and self.texts)]
        if self.history_config != scope:
            self.history_config = scope
            window = cfg.get("history_window", {})
            if window.get("scope") != scope:
                window = cfg["history_window"] = {
                    "scope": scope,
                    "generation": max(time.time_ns(), window.get("generation", 0) + 1),
                }
                new_window = True
            self.generation = window["generation"]
            self.history_since = datetime.fromtimestamp(self.generation // 1_000_000_000, UTC)
            self.history_cursors = {
                reader: point
                for (owner, generation, reader), point in self.ledger.cursors.items()
                if owner == self.uid and generation == self.generation
            }
            self.last_history_start = float("-inf")
            current = asyncio.current_task()
            if (
                self.history_task is not None
                and self.history_task is not current
                and current not in self.tasks
            ):
                self.history_task.cancel()
        return new_window

    async def ensure_window(self):
        ticket = self.initial_write
        if ticket is not None:
            await asyncio.shield(ticket)
            if self.initial_write is ticket:
                self.initial_write = None

    def select_price(self, text):
        return next((price for word, price in self.prices if word in text), None)

    async def commit(self):
        changed = self.reload()
        ticket = self.store.queue_save()
        if changed:
            self.initial_write = ticket
        await asyncio.shield(ticket)

    def ready(self, chat):
        return (
            not self.stopped
            and not self.restoring
            and self.active
            and self.target == chat
            and len(self.ordered) == self.goal
            and self.goal >= 2
        )

    def accepts_target(self, event):
        return self.active and self.target == event.chat_id

    def add(self, slot, user_id, client):
        if slot in self.accounts or user_id in self.own_ids:
            raise ValueError("Akun tersebut sudah terdaftar sebagai worker.")
        account = Account(slot, user_id, client)
        self.accounts[slot] = account
        self.ordered = tuple(self.accounts[k] for k in sorted(self.accounts))
        self.own_ids.add(user_id)

        def post(event):
            return self.on_post(account, event)

        def command(event):
            return self.commands.on_command(account, event)

        self.handlers[slot] = (post, command)
        client.add_event_handler(
            post, events.NewMessage(incoming=True, forwards=True, func=self.accepts_target)
        )
        client.add_event_handler(
            post, events.MessageEdited(incoming=True, forwards=True, func=self.accepts_target)
        )
        client.add_event_handler(
            command, events.NewMessage(outgoing=True, forwards=False, pattern=COMMAND_RE)
        )

    def remove(self, slot):
        account = self.accounts.pop(slot)
        for handler in self.handlers.pop(slot):
            account.client.remove_event_handler(handler)
        self.own_ids.discard(account.user_id)
        self.ordered = tuple(self.accounts[k] for k in sorted(self.accounts))
        self.invalidate(finish_attempts=True)
        return account

    def matching_text(self, msg):
        text = msg.message
        if (
            not self.texts
            or not self.pattern
            or not text
            or len(text) > MAX_POST_LEN
            or msg.out
            or msg.post
        ):
            return None
        fwd = msg.fwd_from
        sender = msg.sender_id
        if not fwd or not fwd.channel_post or sender is None or sender in self.own_ids:
            return None
        if sender != SERVICE_ID and (not fwd.from_id or sender != utils.get_peer_id(fwd.from_id)):
            return None
        return match_keyword(self.pattern, text)

    @staticmethod
    def token(msg):
        if msg.grouped_id:
            return f"a/{msg.grouped_id}"
        fwd = msg.fwd_from
        if fwd and fwd.from_id and fwd.channel_post:
            return f"f/{utils.get_peer_id(fwd.from_id)}/{fwd.channel_post}"
        if isinstance(msg.peer_id, PeerChannel):
            return f"m/{msg.id}"
        return None

    def observe(self, job, observer, msg):
        if msg.grouped_id and (
            not msg.fwd_from
            or not job.message.fwd_from
            or job.message.fwd_from.channel_post != msg.fwd_from.channel_post
        ):
            return
        stamp = getattr(msg, "edit_date", None)
        if stamp is None and job.copies is None:
            return
        previous = getattr(job.message, "edit_date", None)
        if (
            stamp is not None
            and (previous is None or stamp >= previous)
            and (stamp != previous or msg.message != job.message.message)
        ):
            job.message = msg
            job.revision = -1
        if job.copies is not None:
            job.copies[observer.slot] = msg.id
            if observer is job.account:
                job.local_copy = msg.id
                job.copy_ready.set()

    async def on_post(self, observer, event):
        work = self.route(observer, event.message)
        if work is not None and work is not False:
            await work

    def route(self, observer, msg, recovered=False):
        chat = msg.chat_id
        if (
            not self.ready(chat)
            or not msg.fwd_from
            or getattr(msg, "date", self.history_since) < self.history_since
        ):
            return None
        shared_ids = isinstance(msg.peer_id, PeerChannel)
        if (
            shared_ids
            and self.last_shared_key is not None
            and msg.id == self.last_shared_id
            and self.last_shared_key[0] == chat
            and getattr(msg, "edit_date", None) is None
        ):
            if self.last_shared_key in self.jobs:
                return False
            if self.last_shared_key in self.recent:
                return None
        normalized = self.matching_text(msg)
        if normalized is None and (
            not self.jobs or shared_ids and getattr(msg, "edit_date", None) is None
        ):
            return None
        token = self.token(msg)
        if token is None:
            return None
        key = chat, token
        if key in self.recent:
            return None
        pending = self.jobs.get(key)
        if pending is not None:
            self.observe(pending, observer, msg)
            return False
        if normalized is None:
            return None
        if len(self.jobs) >= PAUSED_JOB_LIMIT and self.ledger.remaining(self.uid, chat):
            self.wake_after_pause(chat)
            return False
        return self.process_match(observer, msg, key, normalized, recovered, self.epoch)

    async def process_match(self, observer, msg, key, normalized, recovered, epoch):
        chat, token = key
        if recovered:
            if epoch != self.epoch or not self.ready(chat):
                return False
            if key in self.recent:
                return
            pending = self.jobs.get(key)
            if pending is not None:
                self.observe(pending, observer, msg)
                return False
        start = time.perf_counter()
        text = self.reply_text(msg.message, normalized)
        if text is None:
            return
        job = Delivery(msg, self.revision, start, text, random_id=random.getrandbits(63))
        if isinstance(msg.peer_id, PeerChannel):
            self.last_shared_id, self.last_shared_key = msg.id, key
            job.local_copy = msg.id
        else:
            job.copies = {observer.slot: msg.id}
            job.copy_ready = asyncio.Event()
        task = asyncio.current_task()
        self.jobs[key] = job
        self.tasks[task] = job
        ticket = None
        try:
            if self.initial_write is not None:
                await self.ensure_window()
                if epoch != self.epoch or not self.ready(chat):
                    return False
            direct = (
                job.local_copy is not None
                and not recovered
                and not self.ledger.remaining(self.uid, chat)
            )
            ticket = self.ledger.submit(
                self.ledger.claim, self.uid, chat, token, self.ordered, observer, direct
            )
            assignment = await asyncio.shield(ticket)
            if assignment is None and job.copies is not None:
                for account in self.ordered:
                    if account is not observer and account.slot in job.copies:
                        ticket = self.ledger.submit(
                            self.ledger.claim, self.uid, chat, token, self.ordered, account
                        )
                        assignment = await asyncio.shield(ticket)
                        if assignment is not None:
                            break
            if assignment is None:
                return
            job.account, job.armed = assignment
            if job.copies is not None:
                job.local_copy = job.copies.get(job.account.slot)
                if job.local_copy is None:
                    try:
                        await asyncio.wait_for(job.copy_ready.wait(), self.send_timeout)
                    except TimeoutError:
                        if job.local_copy is None:
                            state = f"await-copy/{self.uid}/{job.account.user_id}"
                            await asyncio.shield(
                                self.ledger.submit(self.ledger.finish, chat, token, state)
                            )
                            if job.local_copy is None:
                                return
                            ticket = self.ledger.submit(
                                self.ledger.claim, self.uid, chat, token, self.ordered, job.account
                            )
                            if await asyncio.shield(ticket) is None:
                                return
            state = await self.deliver(job, chat, token, epoch)
            if state in ("withdrawn", "filtered"):
                await asyncio.shield(self.ledger.submit(self.ledger.release, chat, token))
                return state == "filtered"
            if state == "sent":
                self.ledger.confirm(chat, token)
            else:
                await self.ledger.submit(self.ledger.finish, chat, token, state)
            self.recent[key] = None
            if len(self.recent) > SEEN_LIMIT:
                self.recent.popitem(last=False)
            if state == "sent":
                self.sent += 1
                self.recovered += int(recovered)
            else:
                self.failed += 1
        except asyncio.CancelledError:
            if ticket is not None and await asyncio.shield(ticket) is not None:
                if job.attempted:
                    await asyncio.shield(
                        self.ledger.submit(self.ledger.finish, chat, token, "unknown")
                    )
                else:
                    await asyncio.shield(self.ledger.submit(self.ledger.release, chat, token))
            raise
        except Exception:
            self.failed += 1
            log.exception("owner %s: post %s gagal", self.uid, key)
            if job.account is not None:
                if job.attempted:
                    await self.ledger.submit(self.ledger.finish, chat, token, "unknown")
                else:
                    await self.ledger.submit(self.ledger.release, chat, token)
            if recovered:
                raise
        finally:
            self.jobs.pop(key, None)
            self.tasks.pop(task, None)

    def start_history_check(self, *, after_pause=False):
        if not (self.target and self.ready(self.target) and self.pattern and self.texts):
            return
        if (
            self.ledger.remaining(self.uid, self.target)
            or self.history_task
            and not self.history_task.done()
        ):
            return
        now = time.monotonic()
        if not after_pause and now - self.last_history_start < 10:
            return
        self.last_history_start = now
        self.history_task = asyncio.create_task(self.check_history())

    async def check_history(self):
        await self.ensure_window()
        await check_history(self)

    def wake_after_pause(self, chat):
        if self.stopped or not self.active or self.target != chat or self.pause_timer is not None:
            return
        remaining = self.ledger.remaining(self.uid, chat)
        if remaining:
            self.pause_timer = asyncio.get_running_loop().call_later(
                remaining, self.resume_history, chat
            )
        else:
            self.start_history_check(after_pause=True)

    def resume_history(self, chat):
        self.pause_timer = None
        self.wake_after_pause(chat)

    def invalidate(self, *, finish_attempts=False):
        if self.pause_timer is not None:
            self.pause_timer.cancel()
            self.pause_timer = None
        self.epoch += 1
        for task, job in self.tasks.items():
            if task is not asyncio.current_task() and not (finish_attempts and job.attempted):
                task.cancel()

    async def pause(self, reason):
        self.invalidate()
        self.settings["active"] = False
        await self.commit()
        log.warning("owner %s: pool paused (%s)", self.uid, reason)

    async def rate_limit(self, exc, chat):
        seconds = getattr(exc, "seconds", None)
        if seconds is None:
            await self.pause(type(exc).__name__)
            return False
        ticket = self.ledger.defer(self.uid, chat, max(1, seconds))
        if ticket is not None:
            await asyncio.shield(ticket)
        self.wake_after_pause(chat)
        return True

    def reply_text(self, post, value):
        price = self.select_price(value)
        start = random.randrange(len(self.texts)) if len(self.texts) > 1 else 0
        fallback = None
        for offset in range(len(self.texts)):
            body = self.texts[(start + offset) % len(self.texts)]
            if len(body) + (len(price) + 2 if price else 0) > MAX_REPLY_LEN:
                continue
            template = f"{price}\n\n{body}" if price else body
            if fallback is None:
                fallback = template
            text = build_reply(template, post)
            if len(text) <= MAX_REPLY_LEN:
                return text
        return fallback

    async def deliver(self, job, chat, token, epoch):
        account = job.account
        request = None
        transport_retried = False
        uncertain = False
        armed = job.armed
        while self.ready(chat) and self.epoch == epoch:
            wait = self.ledger.remaining(self.uid, chat)
            if wait:
                if armed and not uncertain:
                    await self.ledger.submit(self.ledger.set_dispatch, chat, token, False)
                    armed = False
                await asyncio.sleep(wait)
                continue
            try:
                if account.peer is None:
                    peer = await asyncio.wait_for(
                        account.client.get_input_entity(chat), self.send_timeout
                    )
                    if self.epoch != epoch:
                        return "withdrawn"
                    account.peer = peer
                    continue
                if job.revision != self.revision:
                    normalized = self.matching_text(job.message)
                    job.revision = self.revision
                    if normalized is None:
                        job.text = None
                    elif not uncertain:
                        job.text = self.reply_text(job.message.message, normalized)
                        request = None
                if job.text is None:
                    return "unknown" if uncertain else "filtered"
                if request is None:
                    request = functions.messages.SendMessageRequest(
                        peer=account.peer,
                        message=job.text,
                        random_id=job.random_id,
                        reply_to=InputReplyToMessage(reply_to_msg_id=job.local_copy),
                    )
                    if job.text.startswith(".") and re.fullmatch(COMMAND_RE, job.text):
                        self.command_replies[(account.user_id, chat, job.local_copy, job.text)] = (
                            None
                        )
                        if len(self.command_replies) > 128:
                            self.command_replies.popitem(last=False)
                if not armed:
                    armed = await self.ledger.submit(self.ledger.set_dispatch, chat, token, True)
                    if not armed:
                        return "withdrawn"
                    continue
                sending = time.perf_counter()
                if not job.attempted:
                    self.dispatch_ms = (sending - job.received) * 1000
                job.attempted = True
                await asyncio.wait_for(account.client(request), self.send_timeout)
                self.ack_ms = (time.perf_counter() - sending) * 1000
                return "sent"
            except errors.RandomIdDuplicateError:
                return "sent"
            except errors.FloodError as exc:
                job.attempted = uncertain
                if not uncertain:
                    request = None
                if not await self.rate_limit(exc, chat):
                    return "failed"
                self.retries += 1
            except (
                errors.PeerFloodError,
                errors.AuthKeyDuplicatedError,
                errors.ChannelPrivateError,
                errors.ForbiddenError,
                errors.UserBannedInChannelError,
                errors.ChatAdminRequiredError,
                errors.UnauthorizedError,
            ) as exc:
                await self.pause(type(exc).__name__)
                return "failed"
            except (TimeoutError, OSError, errors.ServerError, errors.RpcCallFailError):
                uncertain = uncertain or job.attempted
                if transport_retried:
                    return "unknown" if uncertain else "withdrawn"
                transport_retried = True
                self.retries += 1
            except errors.RPCError as exc:
                log.warning("worker %s: %s", account.slot, type(exc).__name__)
                return "unknown" if uncertain else "failed"
        return "unknown" if uncertain else "withdrawn"

    async def stop(self, logout=False):
        self.stopped = True
        self.invalidate()
        tasks = list(self.tasks)
        if self.history_task is not None:
            tasks.append(self.history_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.store.drain()
        for slot, account in self.accounts.items():
            for handler in self.handlers[slot]:
                account.client.remove_event_handler(handler)
            try:
                if logout:
                    await account.client.log_out()
            except (OSError, errors.RPCError):
                log.exception("worker %s: logout failed", slot)
            finally:
                try:
                    await account.client.disconnect()
                except (OSError, errors.RPCError):
                    log.exception("worker %s: disconnect failed", slot)
