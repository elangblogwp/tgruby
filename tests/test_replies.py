import asyncio
import time
from types import SimpleNamespace as NS

import pytest
from telethon import errors, types

from ruby_ubot.store import Store
from ruby_ubot.text import build_reply

from .support import CHAT, emit, history_reader, make_pool, post


@pytest.mark.asyncio
async def test_three_accounts_concurrent_burst(pool):
    gate = asyncio.Event()
    for a in pool.ordered:
        a.client.gate = gate
    tasks = [asyncio.create_task(emit(pool, post(i))) for i in range(1, 601)]
    deadline = time.monotonic() + 5
    while sum(len(a.client.calls) for a in pool.ordered) < 600 and time.monotonic() < deadline:
        await asyncio.sleep(0.001)
    assert sum(len(a.client.calls) for a in pool.ordered) == 600
    for a in pool.ordered:
        assert [r.reply_to.reply_to_msg_id for r in a.client.calls] == [
            i for i in range(1, 601) if ((i - 1) // 2) % 3 + 1 == a.slot
        ]
    gate.set()
    await asyncio.gather(*tasks)
    assert pool.sent == 600 and not pool.jobs
    assert all(
        r.message == "1d\n4u\n15k\n\nready stock! 🤙" for a in pool.ordered for r in a.client.sent
    )


@pytest.mark.asyncio
async def test_replay_after_restart_keeps_rotation(tmp_path):
    p = make_pool(tmp_path)
    await emit(p, post(1))
    await emit(p, post(2))
    await p.ledger.close()
    p = make_pool(tmp_path)
    await emit(p, post(1))
    await emit(p, post(3))
    assert p.sent == 1 and len(p.accounts[2].client.sent) == 1
    await p.ledger.close()


@pytest.mark.asyncio
async def test_cross_pool_dedup(pool, tmp_path):
    other = make_pool(tmp_path)
    await asyncio.gather(emit(pool, post()), emit(other, post()))
    assert pool.sent + other.sent == 1
    await other.ledger.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [errors.FloodWaitError, errors.SlowModeWaitError])
async def test_group_wide_pause_and_same_owner_retry(pool, error):
    a = pool.accounts[1]
    a.client.failures.append(error(None, capture=1))
    start = time.monotonic()
    first = asyncio.create_task(emit(pool, post(1)))
    deadline = time.monotonic() + 2
    while not pool.retries and time.monotonic() < deadline:
        await asyncio.sleep(0.001)
    assert pool.retries == 1
    rest = asyncio.gather(emit(pool, post(2)), emit(pool, post(3)))
    await asyncio.sleep(0.03)
    assert len(a.client.calls) == 1 and not pool.accounts[2].client.calls
    await asyncio.gather(first, rest)
    assert time.monotonic() - start >= 1
    ids = [r.random_id for r in a.client.calls if r.reply_to.reply_to_msg_id == 1]
    assert len(ids) == 2 and len(set(ids)) == 1 and pool.sent == 3


@pytest.mark.asyncio
async def test_ambiguous_result_never_fails_over(pool, tmp_path):
    pool.accounts[1].client.failures.extend([TimeoutError(), TimeoutError()])
    await emit(pool, post(1))
    await emit(pool, post(1))
    assert pool.failed == 1 and sum(len(a.client.calls) for a in pool.ordered) == 2
    assert pool.accounts[1].client.calls[0] is pool.accounts[1].client.calls[1]
    row = pool.ledger.db.execute("select state from claims").fetchone()
    assert row == ("unknown",)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc", [errors.PeerFloodError, errors.UserBannedInChannelError, errors.ChatWriteForbiddenError]
)
async def test_restriction_stops_whole_pool(pool, exc):
    pool.accounts[1].client.failures.append(exc(None))
    await emit(pool, post(1))
    await emit(pool, post(2))
    await emit(pool, post(3))
    assert not pool.active and sum(len(a.client.calls) for a in pool.ordered) == 1
    assert not Store(pool.store.path, 7).settings(7)["active"]


@pytest.mark.asyncio
async def test_edited_during_wait_and_settings(pool):
    pool.ledger.defer(7, CHAT, 0.08)
    task = asyncio.create_task(emit(pool, post(1)))
    await asyncio.sleep(0.005)
    await emit(pool, post(1, text="noktel T.U", edit_date=100))
    await task
    assert pool.sent == 0
    await emit(pool, post(1, text="noktel", edit_date=101))
    assert pool.sent == 1


@pytest.mark.asyncio
async def test_close_cancels_rate_wait(pool):
    pool.ledger.defer(7, CHAT, 3600)
    task = asyncio.create_task(emit(pool, post()))
    await asyncio.sleep(0.01)
    await pool.commands.cmd_close("")
    await asyncio.gather(task, return_exceptions=True)
    assert not pool.jobs and not pool.sent
    await pool.commands.cmd_open("")
    assert pool.ledger.remaining(7, CHAT) > 3590


@pytest.mark.asyncio
async def test_basic_group_local_message_id(pool):
    pool.store.settings(7)["targets"] = {"-23": "basic"}
    await pool.commit()
    common = dict(
        peer_id=types.PeerChat(23),
        chat_id=-23,
        fwd_from=NS(channel_post=89, from_id=types.PeerChannel(99)),
    )
    first = asyncio.create_task(pool.on_post(pool.accounts[2], post(20, **common)))
    await asyncio.sleep(0)
    assert not pool.accounts[1].client.sent
    await pool.on_post(pool.accounts[1], post(50, **common))
    await first
    assert pool.accounts[1].client.sent[0].reply_to.reply_to_msg_id == 50


@pytest.mark.asyncio
async def test_album_single_reply(pool):
    await asyncio.gather(*(emit(pool, post(i, grouped_id=1234)) for i in range(1, 6)))
    assert pool.sent == 1
    await emit(pool, post(1234))
    assert pool.sent == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "text,allowed",
    [
        ("noktel" + "x" * 93, True),
        ("noktel" + "x" * 94, False),
        ("noktel T.U", False),
        ("ｎｏｋｔｅｌ", True),
        ("random", False),
    ],
)
async def test_filter(pool, text, allowed):
    await emit(pool, post(text=text))
    assert bool(pool.sent) == allowed


@pytest.mark.asyncio
async def test_single_group_and_incomplete_pool(pool):
    await emit(pool, post(chat_id=42))
    pool.store.settings(7)["worker_count"] = 4
    await pool.commit()
    await emit(pool, post())
    assert pool.sent == 0


@pytest.mark.asyncio
async def test_ambiguous_accepted_retry_does_not_duplicate(pool):
    client = pool.accounts[1].client
    client.failures.extend([TimeoutError(), errors.RandomIdDuplicateError(None)])
    await emit(pool, post())
    await emit(pool, post())
    assert pool.sent == 1 and len(client.calls) == 2
    assert client.calls[0] is client.calls[1]


@pytest.mark.asyncio
async def test_same_channel_post_in_two_discussion_wrappers(pool):
    p = post(1)
    await emit(pool, p)
    await emit(pool, post(160, fwd_from=p.message.fwd_from))
    assert pool.sent == 1


@pytest.mark.asyncio
async def test_late_basic_group_copy_sent_once_on_reserved_worker(pool, monkeypatch):
    pool.send_timeout = 0.02
    pool.store.settings(7)["targets"] = {"-23": "basic"}
    await pool.commit()
    fwd = NS(channel_post=12, from_id=types.PeerChannel(9))
    common = dict(chat_id=-23, peer_id=types.PeerChat(23), fwd_from=fwd)
    await pool.on_post(pool.accounts[3], post(10, **common))
    await pool.on_post(pool.accounts[2], post(30, **common))
    assert pool.sent == 0
    await pool.on_post(pool.accounts[1], post(15, **common))
    await pool.on_post(pool.accounts[1], post(15, **common))
    assert pool.sent == 1 and pool.accounts[1].client.sent[0].reply_to.reply_to_msg_id == 15
    assert pool.ledger.db.execute("select turn from turns").fetchone() == (1,)


def test_unicode_presentation_variants_deduplicated():
    assert build_reply("ready", "☝️ noktel ❤❤️") == "ready ❤"


@pytest.mark.asyncio
async def test_background_scan_cancelled_on_close(pool):
    gate = asyncio.Event()

    async def slow(*args, **kwargs):
        await gate.wait()
        return []

    pool.ordered[0].client.get_messages = slow
    pool.start_history_check()
    await asyncio.sleep(0.002)
    task = pool.history_task
    await pool.commands.cmd_close("")
    await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled() and not pool.sent


@pytest.mark.asyncio
async def test_unsent_peer_lookup_failure_is_recoverable(pool):
    a = pool.ordered[0]
    a.peer = None
    a.client.get_input_entity.side_effect = [
        ConnectionError(),
        ConnectionError(),
        types.InputPeerChannel(9, 1),
    ]
    await emit(pool, post(102))
    assert pool.sent == 0 and not pool.ledger.db.execute("SELECT slot FROM claims").fetchone()
    history_reader(a, [post(102).message], 100)
    await pool.check_history()
    assert pool.sent == pool.recovered == 1


@pytest.mark.asyncio
async def test_claim_submission_error_can_be_recovered(pool, monkeypatch):
    import sqlite3

    original = pool.ledger.submit

    def failure(*args):
        raise sqlite3.OperationalError("simulated database fault")

    monkeypatch.setattr(pool.ledger, "submit", failure)
    await emit(pool, post(17))
    assert not pool.jobs and not pool.tasks
    monkeypatch.setattr(pool.ledger, "submit", original)
    await emit(pool, post(17))
    assert pool.sent == 1


@pytest.mark.asyncio
async def test_late_local_message_arrives_during_reservation_write(pool, monkeypatch):
    from threading import Event

    pool.send_timeout = 0.03
    pool.store.settings(7)["targets"] = {"-23": "basic"}
    await pool.commit()
    held, resume = Event(), Event()
    original = pool.ledger.finish

    def held_finish(chat, token, state):
        if state.startswith("await-copy/"):
            held.set()
            assert resume.wait(2)
        return original(chat, token, state)

    monkeypatch.setattr(pool.ledger, "finish", held_finish)
    args = dict(
        chat_id=-23,
        peer_id=types.PeerChat(23),
        fwd_from=NS(channel_post=1, from_id=types.PeerChannel(9)),
    )
    task = asyncio.create_task(pool.on_post(pool.ordered[1], post(44, **args)))
    try:
        assert await asyncio.to_thread(held.wait, 1)
        await pool.on_post(pool.ordered[0], post(77, **args))
    finally:
        resume.set()
    await task
    assert pool.sent == 1 and pool.ordered[0].client.sent[0].reply_to.reply_to_msg_id == 77


@pytest.mark.asyncio
async def test_restart_distinguishes_reserved_from_ambiguous(tmp_path):
    p = make_pool(tmp_path, 4)
    a = p.ordered[0]
    messages = [post(i).message for i in (101, 102, 103)]
    for msg in messages[:2]:
        await p.ledger.submit(p.ledger.claim, p.uid, CHAT, p.token(msg), p.ordered, a)
    await p.ledger.submit(p.ledger.set_dispatch, CHAT, p.token(messages[1]), True)
    await p.ledger.close()
    p = make_pool(tmp_path, 4)
    try:
        p.ledger.recover_reservations()
        history_reader(p.ordered[0], messages)
        await p.check_history()
        ids = sorted(r.reply_to.reply_to_msg_id for a in p.ordered for r in a.client.sent)
        assert ids == [101, 103]
    finally:
        await p.ledger.close()


@pytest.mark.asyncio
async def test_only_target_is_limited_to_99_characters(pool):
    promo = "Produk tersedia. " * 12
    price = "Paket satu hari 15k\n" * 6 + "Akhir"
    assert "disimpan" in await pool.commands.cmd_text(promo)
    assert promo in await pool.commands.cmd_text("")
    assert "disimpan" in await pool.commands.cmd_addprice("samsung " + price)
    assert price in await pool.commands.cmd_addprice("samsung")
    target = "samsung ".ljust(98, "x") + "🤙"
    await emit(pool, post(101, text=target))
    await emit(pool, post(102, text=target + "x"))
    replies = [r for account in pool.ordered for r in account.client.sent]
    assert [(r.reply_to.reply_to_msg_id, r.message) for r in replies] == [
        (101, price + "\n\n" + promo + "🤙")
    ]


@pytest.mark.asyncio
async def test_outgoing_telegram_limit_counts_whole_reply(pool):
    limit = 4096
    body = "x" * (limit - 5)
    await pool.commands.cmd_text(body)
    await pool.commands.cmd_addprice("samsung a")
    await emit(pool, post(101, text="samsung 🤙"))
    assert pool.ordered[0].client.sent[0].message == "a\n\n" + body + " 🤙"
    assert str(limit) in await pool.commands.cmd_addprice("samsung " + "a" * 4)
    assert str(limit) in await pool.commands.cmd_text("x" * (limit + 1))
    await pool.commands.cmd_text(body + "x|ok")
    await emit(pool, post(102, text="samsung 🤙"))
    assert pool.ordered[0].client.sent[-1].message == "a\n\nok 🤙"


@pytest.mark.asyncio
async def test_known_flood_rejection_returns_to_reserved_state(pool):
    pool.ordered[0].client.failures.append(errors.FloodWaitError(None, capture=10))
    job = asyncio.create_task(emit(pool, post(101)))
    try:
        async with asyncio.timeout(2):
            while True:
                record = pool.ledger.db.execute("SELECT state FROM claims").fetchone()
                if pool.retries and record == ("reserved",):
                    break
                await asyncio.sleep(0.001)
        assert not pool.sent
    finally:
        await pool.commands.cmd_close("")
        await asyncio.gather(job, return_exceptions=True)


@pytest.mark.asyncio
async def test_worker_update_builders_filter_ordinary_messages(pool):
    worker = pool.ordered[0]
    for _, builder in worker.client.handlers[:2]:
        await builder.resolve(worker.client)
        for is_forward in (False, True):
            msg = post().message
            if not is_forward:
                msg.fwd_from = None
            event = NS(message=msg, chat_id=CHAT)
            assert bool(builder.filter(event)) == is_forward
