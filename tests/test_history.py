import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from telethon import errors, types

from ruby_ubot.app import ChannelSyncWarning

from .support import CHAT, emit, history_reader, make_pool, post


@pytest.mark.asyncio
async def test_history_finds_missing_posts_once_with_four_workers(tmp_path):
    p = make_pool(tmp_path, 4)
    messages = [post(i).message for i in range(90, 106)]
    messages[-1].message = "noktel T.U"
    reader = history_reader(p.ordered[0], messages)
    try:
        await emit(p, post(101))
        await p.check_history()
        assert p.sent == 4 and p.recovered == 3
        assert sorted(r.reply_to.reply_to_msg_id for a in p.ordered for r in a.client.sent) == [
            101,
            102,
            103,
            104,
        ]
        await emit(p, post(103))
        await p.check_history()
        assert p.sent == 4 and p.recovered == 3 and len(reader) == 3
    finally:
        await p.stop()
        await p.ledger.close()


@pytest.mark.asyncio
async def test_concurrent_live_and_history_overlap(pool):
    gate = asyncio.Event()
    for a in pool.ordered:
        a.client.gate = gate
    history_reader(pool.ordered[0], [post(101).message])
    history = asyncio.create_task(pool.check_history())
    async with asyncio.timeout(2):
        while not pool.jobs:
            await asyncio.sleep(0.001)
    await emit(pool, post(101))
    gate.set()
    await history
    assert pool.sent == 1 and sum(len(a.client.calls) for a in pool.ordered) == 1


@pytest.mark.asyncio
async def test_history_drains_multiple_pages_in_one_scan(pool):
    records = [post(i, text="irrelevant").message for i in range(101, 606)]
    records[-1].message = "noktel"
    history_reader(pool.ordered[0], records)
    await pool.check_history()
    assert pool.history_cursors[0] == (100, 605) and pool.recovered == 1


@pytest.mark.asyncio
async def test_reconcile_retries_page_on_server_replication_error(pool):
    msg = [post(101).message]

    async def unavailable(peer, limit, **kw):
        if "offset_date" in kw:
            return [NS(id=100)]
        raise errors.PersistentTimestampOutdatedError(None)

    pool.ordered[0].client.get_messages = unavailable
    history_reader(pool.ordered[1], msg)
    await pool.check_history()
    assert pool.history_cursors[0] == (100, 100) and not pool.sent
    await pool.check_history()
    assert pool.recovered == 1 and pool.history_cursors[0] == (100, 101)


@pytest.mark.asyncio
async def test_history_honours_flood_across_all_workers(pool):
    first = pool.ordered[0].client
    first.get_messages = AsyncMock(side_effect=errors.FloodWaitError(None, capture=3))
    await pool.check_history()
    assert pool.ledger.remaining(pool.uid, CHAT) > 2
    await pool.check_history()
    first.get_messages.assert_awaited_once()
    assert not pool.sent


@pytest.mark.asyncio
async def test_history_with_unavailable_anchor_does_not_reply_to_old_ads(pool):
    from datetime import timedelta

    past = post(1, date=pool.history_since - timedelta(minutes=10)).message
    fresh = post(2, date=pool.history_since + timedelta(seconds=2)).message
    history_reader(pool.ordered[0], [past, fresh], 0)
    await pool.check_history()
    assert pool.recovered == 1 and pool.ordered[0].client.sent[0].reply_to.reply_to_msg_id == 2


@pytest.mark.asyncio
async def test_history_supplies_each_basic_group_accounts_local_id(pool):
    pool.store.settings(7)["targets"] = {"-23": "basic"}
    await pool.commit()
    fwd = NS(channel_post=8, from_id=types.PeerChannel(12))
    for a in pool.ordered:
        history_reader(
            a, [post(10 * a.slot, chat_id=-23, peer_id=types.PeerChat(23), fwd_from=fwd).message], 0
        )
    await asyncio.wait_for(pool.check_history(), 1)
    assert pool.recovered == 1 and pool.ordered[0].client.sent[0].reply_to.reply_to_msg_id == 10


@pytest.mark.asyncio
async def test_telethon_real_warning_format_only_wakes_target_and_coalesces(pool):
    import logging

    handler = ChannelSyncWarning(NS(workers={7: pool}))
    reads = history_reader(pool.ordered[0], [post(101).message])

    def warn(channel):
        handler.emit(
            logging.LogRecord(
                "telethon.client.updates",
                logging.WARNING,
                "",
                10,
                "Getting difference for channel updates %s caused %s; ending getting difference prematurely until server issues are resolved",
                (channel, "PersistentTimestampOutdatedError"),
                None,
            )
        )

    warn(42)
    await asyncio.sleep(0)
    assert not reads
    for _ in pool.ordered:
        warn(9)
    await asyncio.sleep(0)
    await pool.history_task
    assert pool.recovered == 1 and len(reads) == 2
    warn(9)
    await asyncio.sleep(0)
    assert len(reads) == 2
    handler.close()


@pytest.mark.asyncio
async def test_restart_recovers_outage_without_reanchoring(tmp_path):
    from datetime import timedelta

    first = make_pool(tmp_path, 4)
    since = first.history_since
    fresh = post(101, date=since + timedelta(seconds=1)).message
    history_reader(first.ordered[0], [fresh])
    await first.check_history()
    await first.stop()
    await first.ledger.close()

    second = make_pool(tmp_path, 4)
    try:
        assert second.history_since == since
        assert second.history_cursors[0] == (100, 101)
        records = [fresh, post(102, date=since + timedelta(seconds=2)).message]
        calls = history_reader(second.ordered[0], records, before_id=102)
        await second.check_history()
        assert all(offset is None for _, _, offset in calls)
        assert second.sent == second.recovered == 1
        assert [r.reply_to.reply_to_msg_id for a in second.ordered for r in a.client.sent] == [102]
    finally:
        await second.stop()
        await second.ledger.close()


@pytest.mark.asyncio
async def test_recovery_retries_unsent_record_before_checkpoint(pool):
    records = [post(i, text="no").message for i in range(101, 199)]
    records[0].message = "noktel"
    worker = pool.ordered[0]
    worker.peer = None
    worker.client.get_input_entity.side_effect = [
        ConnectionError(),
        ConnectionError(),
        types.InputPeerChannel(9, 1),
    ]
    history_reader(worker, records)
    await pool.check_history()
    assert pool.history_cursors[0] == (100, 100) and pool.sent == 0
    await pool.check_history()
    assert pool.history_cursors[0] == (100, 198) and pool.recovered == 1


@pytest.mark.asyncio
async def test_reopen_changes_window_and_ignores_old_push(pool):
    from datetime import timedelta

    history_reader(pool.ordered[0], [post(101).message])
    await pool.check_history()
    generation = pool.generation
    await pool.commands.cmd_close("")
    await pool.commands.cmd_open("")
    assert pool.generation != generation and not pool.history_cursors
    await emit(pool, post(102, date=pool.history_since - timedelta(seconds=30)))
    assert pool.sent == 1


@pytest.mark.asyncio
async def test_keyword_change_keeps_cursor(pool):
    history_reader(pool.ordered[0], [post(101).message])
    await pool.check_history()
    generation = pool.generation
    await pool.commands.cmd_keyword("noktel, samsung, apple")
    assert pool.generation == generation and pool.history_cursors[0] == (100, 101)
