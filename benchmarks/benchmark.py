import argparse
import asyncio
import gc
import importlib.util
import json
import math
import os
import resource
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from telethon.tl.types import InputPeerChannel, PeerChannel

ROOT = Path(__file__).resolve().parents[1]
CHAT = -1000000000023


def load_project(baseline):
    if baseline:
        for key, value in (
            ("API_ID", "1"),
            ("API_HASH", "test"),
            ("BOT_TOKEN", "test"),
            ("OWNER_ID", "7"),
        ):
            os.environ.setdefault(key, value)
        spec = importlib.util.spec_from_file_location("legacy_ruby", baseline)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module.Store, module.Pool, module.Ledger
    sys.path.insert(0, str(ROOT))
    from ruby_ubot.ledger import Ledger
    from ruby_ubot.pool import Pool
    from ruby_ubot.store import Store

    return Store, Pool, Ledger


class Client:
    def __init__(self, samples, seen):
        self.samples = samples
        self.seen = seen
        self.handlers = []

    def add_event_handler(self, callback, builder):
        self.handlers.append(callback)

    def remove_event_handler(self, callback):
        self.handlers.remove(callback)

    async def __call__(self, request):
        msg_id = request.reply_to.reply_to_msg_id
        if msg_id in self.seen:
            raise AssertionError(f"Balasan ganda untuk {msg_id}")
        self.seen.add(msg_id)
        self.samples[msg_id][1] = time.perf_counter_ns()

    async def disconnect(self):
        pass


def message(i, match):
    return SimpleNamespace(
        id=i,
        chat_id=CHAT,
        peer_id=PeerChannel(23),
        out=False,
        post=False,
        sender_id=777000,
        edit_date=None,
        grouped_id=None,
        message=f"need noktel {i}" if match else f"diskusi {i}",
        fwd_from=SimpleNamespace(channel_post=i, from_id=PeerChannel(42)),
    )


async def run(args):
    Store, Pool, Ledger = load_project(args.baseline)
    with TemporaryDirectory() as directory:
        root = Path(directory)
        store = Store(root / "data.json") if args.baseline else Store(root / "data.json", 7)
        store.settings(7).update(
            keywords=["noktel"],
            prices={"noktel": "15k"},
            texts=["ready stock!"],
            targets={str(CHAT): "test"},
            worker_count=4,
        )
        ledger = Ledger(root / "deliveries.sqlite3")
        pool = Pool(7, store, ledger)
        samples = {}
        seen = set()
        for slot in range(1, 5):
            client = Client(samples, seen)
            pool.add(slot, slot, client)
            pool.accounts[slot].peer = InputPeerChannel(23, slot)
        trials = args.posts

        async def burst():
            for batch in range(0, trials, args.concurrency):
                calls = []
                for i in range(batch + 1, min(batch + args.concurrency, trials) + 1):
                    match = args.case != "mixed" or i % 10 == 0
                    event = SimpleNamespace(message=message(i, match))
                    for account in pool.ordered:
                        calls.append(account.client.handlers[0](event))
                    if match:
                        samples[i] = [time.perf_counter_ns(), None]
                await asyncio.gather(*calls)

        async def history():
            async def get_messages(peer, limit, min_id=0, reverse=False, offset_date=None):
                if offset_date is not None:
                    return []
                items = []
                for i in range(min_id + 1, min(min_id + limit, trials) + 1):
                    items.append(message(i, i % 100 == 0))
                    if i % 100 == 0 and i not in samples:
                        samples[i] = [time.perf_counter_ns(), None]
                return items

            pool.ordered[0].client.get_messages = get_messages
            await pool.check_history()

        gc.collect()
        started_cpu = time.process_time_ns()
        started = time.perf_counter_ns()
        if args.case == "history":
            await history()
        else:
            await burst()
        await pool.stop()
        await ledger.close()
        elapsed = (time.perf_counter_ns() - started) / 1e6
        cpu_ms = (time.process_time_ns() - started_cpu) / 1e6
        if seen != samples.keys():
            raise AssertionError(f"Diharapkan {len(samples)} balasan, terkirim {len(seen)}")
        latencies = sorted((end - start) / 1e6 for start, end in samples.values())
        result = {
            "case": args.case,
            "posts": trials,
            "sent": len(seen),
            "cpu_ms": round(cpu_ms, 2),
            "elapsed_ms": round(elapsed, 2),
            "rss_peak_mib": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2),
        }
        for percentile in (50, 95, 99):
            result[f"p{percentile}_ms"] = (
                round(latencies[math.ceil(len(latencies) * percentile / 100) - 1], 3)
                if latencies
                else None
            )
        print(json.dumps(result))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--case", choices=("burst", "mixed", "history"), default="burst")
    parser.add_argument("--posts", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=100)
    args = parser.parse_args()
    if args.posts < 1 or args.concurrency < 1:
        parser.error("--posts dan --concurrency harus positif")
    asyncio.run(run(args))
