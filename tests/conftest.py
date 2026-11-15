import pytest_asyncio

from .support import make_pool


@pytest_asyncio.fixture
async def pool(tmp_path):
    p = make_pool(tmp_path)
    yield p
    await p.stop()
    await p.ledger.close()
