import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

from telethon import functions, types

from ruby_ubot.config import Config
from ruby_ubot.ledger import Ledger
from ruby_ubot.pool import SERVICE_ID, Pool
from ruby_ubot.store import Store


def local_config(directory):
    return Config(directory, 7, 1, "test", "test")


CHAT = -1000000000009


class Client:
    def __init__(self, n):
        self.disconnected = asyncio.get_running_loop().create_future()
        self.n = n
        self.get_me = AsyncMock(return_value=NS(id=n, bot=False))
        self.connect = AsyncMock()
        self.disconnect = AsyncMock()
        self.log_out = AsyncMock()
        self.send_code_request = AsyncMock(return_value=NS(phone_code_hash="hash"))
        self.sign_in = AsyncMock()
        self.catch_up = AsyncMock()
        self.get_input_entity = AsyncMock(return_value=types.InputPeerChannel(9, n))
        self.sent = []
        self.calls = []
        self.gate = None
        self.failures = []
        self.handlers = []

    def add_event_handler(self, cb, builder):
        self.handlers.append((cb, builder))

    def remove_event_handler(self, cb):
        self.handlers = [(f, b) for f, b in self.handlers if f != cb]

    async def __call__(self, req):
        if isinstance(req, functions.PingRequest):
            return
        self.calls.append(req)
        if self.gate:
            await self.gate.wait()
        if self.failures:
            raise self.failures.pop(0)
        self.sent.append(req)


def post(i=1, text="☝️need noktel, pakai 🤙 kalau bukan bot", **kw):
    msg = NS(
        id=i,
        chat_id=CHAT,
        peer_id=types.PeerChannel(9),
        grouped_id=None,
        message=text,
        out=False,
        post=False,
        sender_id=SERVICE_ID,
        edit_date=None,
        fwd_from=NS(channel_post=i, from_id=types.PeerChannel(99)),
    )
    msg.__dict__.update(kw)
    return NS(message=msg)


def make_pool(path, count=3):
    store = Store(path / "data.json", 7)
    store.settings(7).update(
        keywords=["noktel", "samsung"],
        texts=["ready stock!"],
        prices={"noktel": "1d\n4u\n15k"},
        targets={str(CHAT): "test"},
        worker_count=count,
    )
    pool = Pool(7, store, Ledger(path / "claims.sqlite"))
    for n in range(1, count + 1):
        pool.add(n, n, Client(n))
        pool.accounts[n].peer = types.InputPeerChannel(9, n)
    return pool


async def emit(pool, event):
    await asyncio.gather(
        *(account.client.handlers[0][0](event) for account in reversed(pool.ordered))
    )


def history_reader(account, messages, before_id=100):
    calls = []

    async def get(peer, limit, min_id=0, reverse=False, offset_date=None):
        calls.append((limit, min_id, offset_date))
        if offset_date is not None:
            return [NS(id=before_id)] if before_id else []
        assert reverse
        return [msg for msg in messages if msg.id > min_id][:limit]

    account.client.get_messages = get
    return calls
