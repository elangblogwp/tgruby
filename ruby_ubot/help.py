from telethon import Button

from .text import EMOJI_RE

COMMAND_RE = r"(?is)^\.(help|ping|listgc|addgc|rmgc|keyword|text|addprice|open|close)(?:\s+(.*))?$"

PAGES = {
    "work": ("Worker", "/login\n/logout"),
    "group": ("Grup target", ".listgc\n.addgc <id grup>\n.rmgc <id grup>"),
    "promo": (
        "Promosi",
        ".keyword <kata, kata>\n.text <variasi 1|variasi 2>\n.addprice <keyword> <harga>",
    ),
    "status": ("Status", ".ping\n.open\n.close"),
    "users": ("Pengguna", "/users\n/adduser <id>\n/deluser <id>"),
}

WORKER_HELP = (
    "Ruby UBot\n.help   .ping\n\n"
    "Aktivitas\n.open   .close\n\n"
    "Grup\n.listgc   .addgc   .rmgc\n\n"
    "Promosi\n.keyword   .text   .addprice"
)


def worker_help(username):
    if username:
        return f"{WORKER_HELP}\n\nMenu bantuan\nhttps://t.me/{username}?start=help"
    return WORKER_HELP


def admin_text(text):
    return text if text.isascii() else EMOJI_RE.sub("", text)


def display_value(value):
    clean = admin_text(value)
    return clean if clean.strip() else "(hanya emoji)"


def panel_page(page, owner=False, worker_only=False):
    allowed = ["group", "promo", "status"] if worker_only else ["work", "group", "promo", "status"]
    if owner:
        allowed.append("users")
    if page == "home":
        buttons = [Button.inline(PAGES[key][0], data=f"help:{key}") for key in allowed]
        return "Ruby UBot\nBantuan", [buttons[i : i + 2] for i in range(0, len(buttons), 2)]
    if page not in allowed:
        return None
    title, body = PAGES[page]
    return f"Ruby UBot · {title}\n\n{body}", [[Button.inline("Kembali", data=b"help:home")]]
