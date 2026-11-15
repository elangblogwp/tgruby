import re
import unicodedata
from functools import lru_cache

from telethon import utils
from telethon.tl.types import PeerChannel

CHUNK = 4000


def _class(spec):
    parts = []
    for token in spec.split():
        low, _, high = token.partition("-")
        parts.append(f"{chr(int(low, 16))}-{chr(int(high or low, 16))}")
    return "".join(parts)


_ALWAYS = _class(
    "231A-231B 23E9-23EC 23F0 23F3 25FD-25FE 2614-2615 2648-2653 267F 2693 26A1 26AA-26AB "
    "26BD-26BE 26C4-26C5 26CE 26D4 26EA 26F2-26F3 26F5 26FA 26FD 2705 270A-270B 2728 274C "
    "274E 2753-2755 2757 2795-2797 27B0 27BF 2B1B-2B1C 2B50 2B55 1F004 1F02C-1F02F "
    "1F094-1F09F 1F0AF-1F0B0 1F0C0 1F0CF-1F0D0 1F0F6-1F0FF 1F170-1F171 1F17E-1F17F 1F18E "
    "1F191-1F19A 1F1AE-1F1FF 1F201-1F20F 1F21A 1F22F 1F232-1F23A 1F23C-1F23F 1F249-1F25F "
    "1F266-1F321 1F324-1F393 1F396-1F397 1F399-1F39B 1F39E-1F3F0 1F3F3-1F3F5 1F3F7-1F4FD "
    "1F4FF-1F53D 1F549-1F54E 1F550-1F567 1F56F-1F570 1F573-1F57A 1F587 1F58A-1F58D 1F590 "
    "1F595-1F596 1F5A4-1F5A5 1F5A8 1F5B1-1F5B2 1F5BC 1F5C2-1F5C4 1F5D1-1F5D3 1F5DC-1F5DE "
    "1F5E1 1F5E3 1F5E8 1F5EF 1F5F3 1F5FA-1F64F 1F680-1F6C5 1F6CB-1F6D2 1F6D5-1F6E5 1F6E9 "
    "1F6EB-1F6F0 1F6F3-1F6FF 1F777-1F77A 1F7DA-1F7FF 1F80C-1F80F 1F848-1F84F 1F85A-1F85F "
    "1F888-1F88F 1F8AE-1F8AF 1F8B2-1F8FF 1F90C-1F93A 1F93C-1F945 1F947-1F9FF 1FA54-1FA5F "
    "1FA6E-1FAFF 1FC00-1FFFD"
)
_BARE = _class(
    "2600-2604 260E 2611 2618 261D 2620 2622-2623 2626 262A 262E-262F 2638-263A 2640 2642 "
    "265F-2660 2663 2665-2666 2668 267B 267E 2692 2694-2697 2699 269B-269C 26A0 26A7 "
    "26B0-26B1 26C8 26CF 26D1 26D3 26E9 26F0-26F1 26F4 26F7-26F9 2702 2708-2709 270C-270D "
    "270F 2712 2714 2716 271D 2721 2733-2734 2744 2747 2763-2764 27A1 2934-2935 2B05-2B07 "
    "3030 303D 3297 3299"
)
_NEEDS_VS16 = _class(
    "A9 AE 203C 2049 2122 2139 2194-2199 21A9-21AA 2328 23CF 23ED-23EF 23F1-23F2 "
    "23F8-23FA 24C2 25AA-25AB 25B6 25C0 25FB-25FC"
)
_VS16 = "\ufe0f"
_VS15 = "\ufe0e"
_BASE = f"(?:[{_ALWAYS}]|[{_BARE}]|[{_NEEDS_VS16}](?={_VS16}))(?!{_VS15})"
_ITEM = (
    "(?:[\U0001f1e6-\U0001f1ff]{2}"
    f"|[0-9#*]{_VS16}?\u20e3"
    "|\U0001f3f4[\U000e0020-\U000e007e]+\U000e007f"
    f"|{_BASE}(?:[\U0001f3fb-\U0001f3ff]|{_VS16})?)"
)
EMOJI_RE = re.compile(f"{_ITEM}(?:\u200d{_ITEM})*")


@lru_cache(maxsize=256)
def content_emojis(text):
    found = []
    pos = 0
    leading = True
    for match in EMOJI_RE.finditer(text):
        if leading and not text[pos : match.start()].strip():
            pos = match.end()
            continue
        leading = False
        found.append(match.group())
    return tuple(dict.fromkeys(found))


def build_reply(template, post_text):
    if post_text.isascii():
        return template
    extra = content_emojis(post_text)
    if not extra:
        return template
    present = (
        set()
        if template.isascii()
        else {match.group().replace(_VS16, "") for match in EMOJI_RE.finditer(template)}
    )
    suffix = []
    for item in extra:
        key = item.replace(_VS16, "")
        if key not in present:
            suffix.append(item)
            present.add(key)
    if not suffix:
        return template
    separator = "" if template[-1:].isspace() else " "
    return template + separator + "".join(suffix)


def parse_group_id(arg):
    try:
        value = int(arg.strip())
    except ValueError:
        return None
    return utils.get_peer_id(PeerChannel(value)) if value > 0 else value


def normalize(text):
    if text.isascii():
        return " ".join(text.lower().split())
    text = unicodedata.normalize("NFKC", text).casefold()
    return " ".join(re.sub(r"[\u200b\u200c\u200d\u2060\ufeff]", "", text).split())


@lru_cache(maxsize=256)
def match_keyword(pattern, text):
    value = normalize(text)
    return value if "t.u" not in value and pattern.search(value) else None


def split_text(text, limit=CHUNK):
    if text and len(text) * 2 <= limit:
        return [text]
    if text.isascii():
        return [text[i : i + limit] for i in range(0, len(text), limit)] or [" "]
    parts = []
    chunk = []
    size = 0
    for char in text:
        width = 2 if ord(char) > 0xFFFF else 1
        if size + width > limit:
            parts.append("".join(chunk))
            chunk = []
            size = 0
        chunk.append(char)
        size += width
    if chunk:
        parts.append("".join(chunk))
    return parts or [" "]
