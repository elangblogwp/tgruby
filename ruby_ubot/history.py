import asyncio
import logging
from datetime import timedelta

from telethon import errors, utils
from telethon.tl.types import PeerChannel

HISTORY_PAGE = 100
log = logging.getLogger(__name__)


async def check_history(pool):
    chat = pool.target
    if chat is None or not pool.ready(chat):
        return
    supergroup = utils.resolve_id(chat)[1] is PeerChannel
    readers = (pool.ordered[pool.reader_index % len(pool.ordered)],) if supergroup else pool.ordered

    async def scan(account):
        reader = 0 if supergroup else account.user_id
        try:
            await scan_history(pool, chat, account, reader)
        except errors.FloodError as exc:
            await pool.rate_limit(exc, chat)
        except (
            errors.PeerFloodError,
            errors.UnauthorizedError,
            errors.ForbiddenError,
            errors.AuthKeyDuplicatedError,
            errors.ChannelPrivateError,
            errors.UserBannedInChannelError,
        ) as exc:
            await pool.pause(type(exc).__name__)
        except (TimeoutError, errors.ServerError, OSError) as exc:
            pool.reader_index += 1
            log.warning(
                "worker %s: riwayat target belum tersedia (%s)",
                account.slot,
                type(exc).__name__,
            )
        except Exception:
            log.exception("worker %s: gagal memeriksa riwayat target", account.slot)

    if supergroup:
        await scan(readers[0])
    else:
        await asyncio.gather(*(scan(account) for account in readers))


async def scan_history(pool, chat, account, reader):
    epoch = pool.epoch
    generation = pool.generation

    def active():
        return (
            pool.ready(chat)
            and epoch == pool.epoch
            and generation == pool.generation
            and not pool.ledger.remaining(pool.uid, chat)
        )

    if not active():
        return
    peer = account.peer or chat
    checkpoint = pool.history_cursors.get(reader)
    if checkpoint is None:
        before = await asyncio.wait_for(
            account.client.get_messages(
                peer, limit=1, offset_date=pool.history_since - timedelta(seconds=1)
            ),
            pool.send_timeout,
        )
        if not active():
            return
        anchor = high = before[0].id if before else 0
        await pool.ledger.submit(pool.ledger.checkpoint, pool.uid, reader, generation, anchor, high)
        if not active():
            return
        pool.history_cursors[reader] = anchor, high
    else:
        anchor, high = checkpoint
    low = max(anchor, high - 32)
    saved = high
    pages = 0

    async def checkpoint():
        nonlocal saved
        if high <= saved:
            return
        await pool.ledger.submit(pool.ledger.checkpoint, pool.uid, reader, generation, anchor, high)
        saved = high
        if pool.generation == generation:
            pool.history_cursors[reader] = anchor, high

    try:
        while active():
            page = await asyncio.wait_for(
                account.client.get_messages(peer, limit=HISTORY_PAGE, min_id=low, reverse=True),
                pool.send_timeout,
            )
            if not active() or not page or page[-1].id <= low:
                return
            work = []
            busy = False
            for msg in page:
                if (
                    msg.id <= low
                    or not getattr(msg, "fwd_from", None)
                    or not getattr(msg, "message", None)
                ):
                    continue
                candidate = pool.route(account, msg, recovered=True)
                if candidate is False:
                    busy = True
                elif candidate is not None:
                    work.append(candidate)
            if work:
                for result in await asyncio.gather(*work, return_exceptions=True):
                    if isinstance(result, BaseException):
                        raise result
                    if result is False:
                        busy = True
            if busy or not active():
                return
            low = page[-1].id
            high = max(low, high)
            pages += 1
            if len(page) < HISTORY_PAGE:
                return
            if pages % 8 == 0:
                await checkpoint()
    finally:
        await checkpoint()
