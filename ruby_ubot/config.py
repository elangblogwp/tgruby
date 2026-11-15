import math
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

MAX_POST_LEN = 99
MAX_REPLY_LEN = 4096


@dataclass(frozen=True, slots=True)
class Config:
    root: Path
    owner_id: int
    api_id: int
    api_hash: str = field(repr=False)
    bot_token: str = field(repr=False)
    send_timeout: float = 30.0

    @classmethod
    def load(cls, root=None):
        root = Path(root or Path(__file__).resolve().parents[1])
        load_dotenv(root / ".env")
        missing = [
            key
            for key in ("API_ID", "API_HASH", "BOT_TOKEN", "OWNER_ID")
            if not os.getenv(key, "").strip()
        ]
        if missing:
            raise SystemExit("Lengkapi .env: " + ", ".join(missing))
        try:
            api_id = int(os.environ["API_ID"])
            owner_id = int(os.environ["OWNER_ID"])
            timeout = float(os.getenv("SEND_TIMEOUT", "30"))
            if min(api_id, owner_id) < 1 or not math.isfinite(timeout):
                raise ValueError
        except ValueError:
            raise SystemExit("Periksa angka API_ID, OWNER_ID, dan SEND_TIMEOUT di .env.") from None
        return cls(
            root,
            owner_id,
            api_id,
            os.environ["API_HASH"].strip(),
            os.environ["BOT_TOKEN"].strip(),
            max(1.0, timeout),
        )
