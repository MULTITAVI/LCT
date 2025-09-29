import datetime

# Simple in-memory task store for prototype. Replace with DB/Redis in prod.
TASKS = {}


def log(msg: str):
    ts = datetime.datetime.utcnow().isoformat()
    print(f"[{ts}] {msg}")
