import asyncio
import json
import logging
import os
from copy import deepcopy

from .text import normalize

log = logging.getLogger(__name__)


class Store:
    def __init__(self, path, owner_id):
        self.owner_id = owner_id
        self._version = 0
        self._writer = None
        self.path = path
        self.data = {"users": [], "settings": {}}
        if path.exists():
            try:
                self.data.update(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, ValueError) as exc:
                raise RuntimeError(f"Konfigurasi tidak dapat dibaca: {path}") from exc

    def queue_save(self):
        self._version += 1
        if self._writer is None:
            self._writer = asyncio.create_task(self._flush())
        return self._writer

    async def save(self):
        await asyncio.shield(self.queue_save())

    async def drain(self):
        if self._writer is not None:
            await asyncio.shield(self._writer)

    async def _flush(self):
        try:
            while True:
                version = self._version
                snapshot = deepcopy(self.data)
                await asyncio.to_thread(self._write, snapshot)
                if version == self._version:
                    return
        finally:
            self._writer = None

    def _write(self, snapshot):
        tmp = self.path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as output:
            json.dump(snapshot, output, ensure_ascii=False, indent=2)
            output.flush()
            os.fsync(output.fileno())
        os.replace(tmp, self.path)

    def is_authorized(self, uid):
        return uid == self.owner_id or uid in self.data["users"]

    def all_users(self):
        return list(dict.fromkeys([self.owner_id, *self.data["users"]]))

    async def add_user(self, uid):
        if uid == self.owner_id or uid in self.data["users"]:
            return False
        self.data["users"].append(uid)
        await self.save()
        return True

    async def remove_user(self, uid):
        if uid not in self.data["users"]:
            return False
        self.data["users"].remove(uid)
        await self.save()
        return True

    def settings(self, uid):
        cfg = self.data["settings"].setdefault(str(uid), {})
        cfg.setdefault("keywords", [])
        cfg.setdefault("texts", [])
        cfg.setdefault("targets", {})
        cfg.setdefault("prices", {})
        legacy_prefix = cfg.pop("price_text", None)
        if legacy_prefix:
            words = list(
                dict.fromkeys(normalize(word) for word in cfg["keywords"] if normalize(word))
            )
            if len(words) == 1:
                if not any(normalize(key) == words[0] for key in cfg["prices"]):
                    cfg["prices"][words[0]] = legacy_prefix
            else:
                cfg["legacy_price_text"] = legacy_prefix
                log.warning("user %s: shared prefix archived; configure .addprice per keyword", uid)
        cfg.setdefault("worker_count", 2)
        if len(cfg["targets"]) > 1:
            cfg["previous_targets"] = cfg["targets"]
            cfg["targets"] = {}
        cfg.setdefault("active", True)
        return cfg
