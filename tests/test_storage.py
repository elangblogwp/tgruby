import asyncio
import time

import pytest
from telethon import errors

from ruby_ubot import app

from .support import CHAT, local_config, make_pool, post


@pytest.mark.asyncio
async def test_disk_contention_does_not_block_event_loop_or_leave_cancelled_claim(pool, tmp_path):
    import sqlite3

    writer = sqlite3.connect(tmp_path / "claims.sqlite")
    writer.execute("BEGIN IMMEDIATE")
    work = asyncio.create_task(pool.on_post(pool.ordered[0], post(2)))
    start = time.monotonic()
    try:
        await asyncio.sleep(0.035)
        assert not work.done() and time.monotonic() - start < 1
        await pool.commands.cmd_close("")
    finally:
        writer.rollback()
        writer.close()
    await asyncio.gather(work, return_exceptions=True)
    assert pool.ledger.db.execute("select count(*) from claims").fetchone() == (0,)
    assert not pool.sent


@pytest.mark.asyncio
async def test_two_services_cannot_recover_same_reservations(tmp_path, monkeypatch):
    gate = asyncio.Event()
    started = asyncio.Event()

    async def service(config):
        started.set()
        await gate.wait()

    monkeypatch.setattr(app, "run_service", service)
    first = asyncio.create_task(app.main(local_config(tmp_path)))
    await started.wait()
    try:
        with pytest.raises(SystemExit, match="sudah berjalan"):
            await app.main(local_config(tmp_path))
    finally:
        gate.set()
        await first


@pytest.mark.asyncio
async def test_shared_pause_under_locked_disk_survives_restart(tmp_path):
    import sqlite3

    p = make_pool(tmp_path)
    locked = sqlite3.connect(tmp_path / "claims.sqlite")
    locked.execute("BEGIN IMMEDIATE")
    task = asyncio.create_task(p.rate_limit(errors.FloodWaitError(None, capture=20), CHAT))
    try:
        await asyncio.sleep(0.02)
        assert p.ledger.remaining(7, CHAT) > 18 and not task.done()
    finally:
        locked.rollback()
        locked.close()
    await task
    await p.ledger.close()
    p = make_pool(tmp_path)
    assert p.ledger.remaining(7, CHAT) > 18
    await p.ledger.close()
