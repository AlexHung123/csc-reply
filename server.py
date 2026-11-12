#!/usr/bin/env python3
"""CSC translate relay.

The tablet pushes one session. Every open watch page receives the same text.

  POST /sessions                 create a room
  POST /sessions/<id>            replace the current picture
  GET  /sessions/<id>            current picture
  GET  /sessions/<id>/events     live updates (server-sent events)
  GET  /watch/<id>               the shared page

Run: python3 server.py
"""

from __future__ import annotations

import json
import queue
import re
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

HOST = "0.0.0.0"
PORT = 8443
ROOT = Path(__file__).resolve().parent
LOGO_FILE = ROOT / "static" / "logo.svg"
IDLE_KEEP_S = 10 * 60
MAX_BODY = 256_000
MAX_TEXT = 100_000
ROOM_ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789"

PHASES = {"connecting", "live", "finishing", "idle"}
DIRECTIONS = {"en-zh", "zh-en"}
DISPLAY_MODES = {"split", "translation", "source"}
TYPE_SCALES = {"small", "medium", "large"}

lock = threading.Lock()
sessions: dict[str, "Session"] = {}
gates: dict[str, tuple[str, str]] = {}


class Session:
    def __init__(self, room: str, state: dict):
        self.room = room
        self.state = state
        self.passcode = ""
        self.grant = ""
        self.watchers: list[queue.Queue] = []
        self.updated_at = time.time()
        self.idle_since: float | None = None

    def touch(self) -> None:
        self.updated_at = time.time()
        if self.state.get("phase") == "idle":
            if self.idle_since is None:
                self.idle_since = self.updated_at
        else:
            self.idle_since = None

    def public(self) -> dict:
        body = dict(self.state)
        body["id"] = self.room
        body["updatedAt"] = int(self.updated_at * 1000)
        return body


def default_state() -> dict:
    return {
        "phase": "connecting",
        "direction": "en-zh",
        "displayMode": "split",
        "typeScale": "medium",
        "sourceText": "",
        "targetText": "",
        "paused": False,
        "status": "",
    }


def apply_patch(state: dict, patch: dict) -> None:
    if not isinstance(patch, dict):
        raise ValueError("body must be a JSON object")
    if "phase" in patch:
        phase = patch["phase"]
        if phase not in PHASES:
            raise ValueError("phase must be connecting, live, finishing, or idle")
        state["phase"] = phase
    if "direction" in patch:
        direction = patch["direction"]
        if direction not in DIRECTIONS:
            raise ValueError("direction must be en-zh or zh-en")
        state["direction"] = direction
    if "displayMode" in patch:
        mode = patch["displayMode"]
        if mode not in DISPLAY_MODES:
            raise ValueError("displayMode must be split, translation, or source")
        state["displayMode"] = mode
    if "typeScale" in patch:
        scale = patch["typeScale"]
        if scale not in TYPE_SCALES:
            raise ValueError("typeScale must be small, medium, or large")
        state["typeScale"] = scale
    for key in ("sourceText", "targetText", "status"):
        if key not in patch:
            continue
        value = patch[key]
        if not isinstance(value, str):
            raise ValueError(f"{key} must be a string")
        state[key] = value[:MAX_TEXT]
    if "paused" in patch:
        state["paused"] = bool(patch["paused"])


ROOM_RE = re.compile(rf"^[{ROOM_ALPHABET}]{{8}}$")
PASS_RE = re.compile(r"^[A-Za-z0-9]{4,12}$")


def canonical_passcode(raw: object) -> str:
    if not isinstance(raw, str):
        raise ValueError("passcode must be a string")
    cleaned = raw.strip()
    if cleaned == "":
        return ""
    if PASS_RE.fullmatch(cleaned) is None:
        raise ValueError("passcode must be 4–12 letters or numbers")
    return cleaned


def same_secret(left: str, right: str) -> bool:
    if len(left) != len(right):
        return False
    return secrets.compare_digest(left, right)


def apply_passcode(session: Session, code: str | None) -> None:
    if code is None:
        return
    changed = code != session.passcode
    if not changed and gates.get(session.room) == (session.passcode, session.grant):
        return
    session.passcode = code
    if not code:
        session.grant = ""
        gates.pop(session.room, None)
    else:
        stored = gates.get(session.room)
        if stored is not None and stored[0] == code:
            session.grant = stored[1]
        else:
            session.grant = secrets.token_urlsafe(18)
        gates[session.room] = (code, session.grant)
    if not changed:
        return
    for watcher in session.watchers:
        try:
            watcher.put_nowait(None)
        except queue.Full:
            pass
    session.watchers.clear()


def take_passcode(patch: dict) -> str | None:
    if "passcode" not in patch:
        return None
    return canonical_passcode(patch.pop("passcode"))


def canonical_room(raw: object) -> str | None:
    if not isinstance(raw, str):
        return None
    cleaned = raw.strip().lower().replace("-", "")
    if ROOM_RE.fullmatch(cleaned) is None:
        return None
    return cleaned


def new_room() -> str:
    while True:
        room = "".join(secrets.choice(ROOM_ALPHABET) for _ in range(8))
        if room not in sessions:
            return room


def notify(watcher: queue.Queue, payload: str | None) -> None:
    try:
        watcher.put_nowait(payload)
        return
    except queue.Full:
        pass
    try:
        watcher.get_nowait()
    except queue.Empty:
        pass
    try:
        watcher.put_nowait(payload)
    except queue.Full:
        pass


def publish(session: Session) -> None:
    payload = json.dumps(session.public(), ensure_ascii=False, separators=(",", ":"))
    for watcher in list(session.watchers):
        notify(watcher, payload)


def retire(session: Session) -> None:
    """Drop a stopped room so the watch page can no longer enter it."""
    sessions.pop(session.room, None)
    note = json.dumps({"phase": "idle"}, separators=(",", ":"))
    for watcher in list(session.watchers):
        notify(watcher, note)
        notify(watcher, None)
    session.watchers.clear()
    print(f"closed {session.room}", flush=True)


def drop_expired() -> None:
    now = time.time()
    with lock:
        stale = [
            room
            for room, session in sessions.items()
            if session.idle_since is not None and now - session.idle_since >= IDLE_KEEP_S
        ]
        for room in stale:
            session = sessions.pop(room)
            for watcher in session.watchers:
                try:
                    watcher.put_nowait(None)
                except queue.Full:
                    pass
            if stale:
                print(f"expired {room}", flush=True)


def expiry_loop() -> None:
    while True:
        time.sleep(30)
        drop_expired()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        print(f"{self.address_string()} {fmt % args}", flush=True)

    def do_GET(self) -> None:
        path = urlparse(self.path).path.rstrip("/") or "/"
        if path == "/health" or path == "/":
            self.send_json({"ok": True, "service": "csc-relay"})
            return
        if path == "/logo.svg":
            self.send_file(LOGO_FILE, "image/svg+xml")
            return
        if path == "/watch" or path.startswith("/watch/"):
            room = canonical_room(path.removeprefix("/watch/")) if path.startswith("/watch/") else None
            if room is None:
                self.send_html(MISSING_HTML, status=404)
                return
            self.send_html(WATCH_HTML)
            return
        if path.startswith("/sessions/"):
            rest = path.removeprefix("/sessions/")
            if rest.endswith("/events"):
                room = canonical_room(rest[: -len("/events")])
                if room is None:
                    self.send_error_json(404, "not found")
                    return
                self.stream_events(room)
                return
            room = canonical_room(rest)
            if room is not None:
                self.send_session(room)
                return
        self.send_error_json(404, "not found")

    def do_POST(self) -> None:
        path = urlparse(self.path).path.rstrip("/") or "/"
        try:
            patch = self.read_json()
        except ValueError as error:
            self.send_error_json(400, str(error))
            return
        if path == "/sessions":
            self.create_session(patch)
            return
        if path.startswith("/sessions/") and path.endswith("/join"):
            room = canonical_room(path.removeprefix("/sessions/").removesuffix("/join"))
            if room is None:
                self.send_error_json(404, "not found")
                return
            self.join_session(room, patch)
            return
        if path.startswith("/sessions/"):
            room = canonical_room(path.removeprefix("/sessions/"))
            if room is not None:
                self.update_session(room, patch)
                return
        self.send_error_json(404, "not found")

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def create_session(self, patch: dict) -> None:
        requested = patch.pop("id", None)
        room = canonical_room(requested) if requested is not None else None
        if requested is not None and room is None:
            self.send_error_json(400, "bad meeting id")
            return
        try:
            passcode = take_passcode(patch)
        except ValueError as error:
            self.send_error_json(400, str(error))
            return
        state = default_state()
        try:
            apply_patch(state, patch)
        except ValueError as error:
            self.send_error_json(400, str(error))
            return
        with lock:
            if room is None:
                room = new_room()
            session = sessions.get(room)
            if session is not None and state.get("phase") == "idle" and session.state.get("phase") != "idle":
                apply_passcode(session, passcode)
                session.touch()
                publish(session)
                body = session.public()
                closed = False
            else:
                if session is None:
                    session = Session(room, state)
                    sessions[room] = session
                    print(f"created {room}", flush=True)
                else:
                    session.state = state
                    print(f"reused {room}", flush=True)
                apply_passcode(session, passcode)
                if session.state.get("phase") == "idle":
                    retire(session)
                    closed = True
                    body = {"ok": True, "id": room, "phase": "idle"}
                else:
                    session.touch()
                    publish(session)
                    body = session.public()
                    closed = False
        if not closed:
            host = self.headers.get("Host") or f"127.0.0.1:{PORT}"
            body["watchUrl"] = f"http://{host}/watch/{room}"
        self.send_json(body, status=201)

    def update_session(self, room: str, patch: dict) -> None:
        try:
            passcode = take_passcode(patch)
        except ValueError as error:
            self.send_error_json(400, str(error))
            return
        with lock:
            session = sessions.get(room)
            if session is None:
                self.send_error_json(404, "session not found")
                return
            try:
                apply_patch(session.state, patch)
            except ValueError as error:
                self.send_error_json(400, str(error))
                return
            apply_passcode(session, passcode)
            if session.state.get("phase") == "idle":
                retire(session)
                body = {"ok": True, "id": room, "phase": "idle"}
            else:
                session.touch()
                publish(session)
                body = session.public()
        self.send_json(body)

    def join_session(self, room: str, patch: dict) -> None:
        try:
            code = canonical_passcode(patch.get("passcode", ""))
        except ValueError:
            self.send_json({"locked": True}, status=401)
            return
        with lock:
            session = sessions.get(room)
            if session is not None and session.state.get("phase") == "idle":
                retire(session)
                session = None
            if session is None:
                stored = gates.get(room)
                if stored is None or not stored[0]:
                    self.send_error_json(404, "session not found")
                    return
                if not same_secret(code, stored[0]):
                    self.send_json({"locked": True}, status=401)
                    return
                grant = stored[1]
                locked = True
            elif session.passcode and not same_secret(code, session.passcode):
                self.send_json({"locked": True}, status=401)
                return
            else:
                grant = session.grant
                locked = bool(session.passcode)
        if locked:
            self.send_empty(
                204,
                cookie=f"csc_{room}={grant}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800",
            )
            return
        self.send_empty(204)

    def send_session(self, room: str) -> None:
        with lock:
            session = sessions.get(room)
            if session is not None and session.state.get("phase") == "idle":
                retire(session)
                session = None
            if session is None:
                self.send_gate_or_missing(room)
                return
            if not self.viewer_allowed(session):
                self.send_json({"locked": True}, status=401)
                return
            body = session.public()
        self.send_json(body)

    def stream_events(self, room: str) -> None:
        watcher: queue.Queue = queue.Queue(maxsize=32)
        with lock:
            session = sessions.get(room)
            if session is not None and session.state.get("phase") == "idle":
                retire(session)
                session = None
            if session is None:
                self.send_gate_or_missing(room)
                return
            if not self.viewer_allowed(session):
                self.send_json({"locked": True}, status=401)
                return
            session.watchers.append(watcher)
            initial = json.dumps(session.public(), ensure_ascii=False, separators=(",", ":"))
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        try:
            self.wfile.write(f"event: state\ndata: {initial}\n\n".encode())
            self.wfile.flush()
            while True:
                try:
                    payload = watcher.get(timeout=15)
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    continue
                if payload is None:
                    break
                self.wfile.write(f"event: state\ndata: {payload}\n\n".encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass
        finally:
            with lock:
                current = sessions.get(room)
                if current is not None and watcher in current.watchers:
                    current.watchers.remove(watcher)

    def send_gate_or_missing(self, room: str) -> None:
        stored = gates.get(room)
        if stored is None or not stored[0]:
            self.send_error_json(404, "session not found")
            return
        token = self.cookies().get(f"csc_{room}", "")
        if not stored[1] or not same_secret(token, stored[1]):
            self.send_json({"locked": True}, status=401)
            return
        body = default_state()
        body["phase"] = "idle"
        body["status"] = ""
        body["sourceText"] = ""
        body["targetText"] = ""
        body["paused"] = False
        body["id"] = room
        body["updatedAt"] = int(time.time() * 1000)
        self.send_json(body)

    def viewer_allowed(self, session: Session) -> bool:
        if not session.passcode:
            return True
        token = self.cookies().get(f"csc_{session.room}", "")
        return bool(session.grant) and same_secret(token, session.grant)

    def cookies(self) -> dict[str, str]:
        found: dict[str, str] = {}
        raw = self.headers.get("Cookie") or ""
        for part in raw.split(";"):
            if "=" not in part:
                continue
            name, value = part.split("=", 1)
            found[name.strip()] = value.strip()
        return found

    def send_empty(self, status: int, cookie: str | None = None) -> None:
        self.send_response(status)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", "0")
        if cookie is not None:
            self.send_header("Set-Cookie", cookie)
        self.end_headers()

    def read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or "0")
        if length < 0 or length > MAX_BODY:
            raise ValueError("body is too large")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("body must be JSON") from error
        if not isinstance(data, dict):
            raise ValueError("body must be a JSON object")
        return data

    def send_json(self, body: dict, status: int = 200) -> None:
        raw = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(raw)

    def send_file(self, path: Path, content_type: str) -> None:
        try:
            raw = path.read_bytes()
        except OSError:
            self.send_error_json(404, "not found")
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "public, max-age=86400")
        self.end_headers()
        self.wfile.write(raw)

    def send_html(self, html: str, status: int = 200) -> None:
        raw = html.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def send_error_json(self, status: int, message: str) -> None:
        self.send_json({"error": message}, status=status)


MISSING_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CSC - Translate</title>
<link rel="icon" href="/logo.svg">
<style>
  :root {
    --ink: #111111;
    --mute: #8a8178;
    --line: rgba(90, 62, 40, 0.12);
    --cream: #fefbf3;
    --band: #fdf4e9;
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }
  body {
    display: flex;
    color: var(--ink);
    background-color: var(--cream);
    background-image: repeating-linear-gradient(
      45deg,
      var(--band) 0 200px,
      var(--cream) 200px 780px
    );
    font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Noto Sans SC", sans-serif;
  }
  body::before {
    content: "";
    position: fixed;
    z-index: 1;
    top: 0;
    right: 0;
    width: 64px;
    height: 64px;
    background: linear-gradient(to bottom left, #fdf5eb 50%, transparent 50%);
    pointer-events: none;
  }
  main {
    position: relative;
    z-index: 2;
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px;
  }
  .notice {
    width: min(420px, 100%);
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 14px;
  }
  .brand { display: flex; align-items: center; gap: 10px; }
  .logo { height: 36px; width: auto; display: block; }
  .brand-name { font-size: 16px; font-weight: 600; letter-spacing: 0.01em; }
  h1 { margin: 8px 0 0; font-size: 22px; font-weight: 600; }
  p { margin: 0; color: var(--mute); font-size: 16px; line-height: 1.5; }
</style>
</head>
<body>
<main>
  <div class="notice">
    <div class="brand">
      <img class="logo" src="/logo.svg" alt="">
      <span class="brand-name">CSC - Translate</span>
    </div>
    <h1>Meeting not found</h1>
    <p>This meeting ID does not exist. Check the link and try again.</p>
  </div>
</main>
</body>
</html>
"""

WATCH_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CSC - Translate</title>
<link rel="icon" href="/logo.svg">
<style>
  :root {
    --ink: #111111;
    --mute: #8a8178;
    --line: rgba(90, 62, 40, 0.12);
    --cream: #fefbf3;
    --band: #fdf4e9;
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }
  body {
    display: flex;
    flex-direction: column;
    color: var(--ink);
    background-color: var(--cream);
    background-image: repeating-linear-gradient(
      45deg,
      var(--band) 0 200px,
      var(--cream) 200px 780px
    );
    font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Noto Sans SC", sans-serif;
  }
  body::before {
    content: "";
    position: fixed;
    z-index: 3;
    top: 0;
    right: 0;
    width: 64px;
    height: 64px;
    background: linear-gradient(to bottom left, #fdf5eb 50%, transparent 50%);
    pointer-events: none;
  }
  header {
    position: relative;
    z-index: 1;
    display: flex;
    align-items: center;
    gap: 12px;
    height: 68px;
    padding: 0 76px 0 20px;
    border-bottom: 1px solid var(--line);
    font-size: 16px;
    font-weight: 500;
  }
  .brand, .lock-brand {
    display: flex;
    align-items: center;
    gap: 10px;
    flex: 0 0 auto;
    min-width: 0;
  }
  .logo { height: 36px; width: auto; display: block; }
  .brand-name, .lock-name {
    color: var(--ink);
    font-weight: 600;
    letter-spacing: 0.01em;
    white-space: nowrap;
  }
  .brand-name { font-size: 16px; }
  .lock-brand { margin-bottom: 8px; }
  .lock-name { font-size: 22px; }
  @media (max-width: 720px) {
    body::before { width: 44px; height: 44px; }
    header { height: 58px; gap: 8px; padding: 0 52px 0 12px; font-size: 14px; }
    .logo { height: 30px; }
    .brand-name { font-size: 14px; }
    .brand, .lock-brand { gap: 8px; }
  }
  .tabs { display: flex; align-items: center; gap: 4px; min-width: 0; }
  .tab {
    font: inherit;
    font-size: 15px;
    font-weight: 500;
    color: var(--mute);
    background: transparent;
    border: 0;
    border-radius: 999px;
    padding: 6px 12px;
    cursor: pointer;
    white-space: nowrap;
  }
  .tab.on {
    color: var(--ink);
    background: rgba(255, 255, 255, 0.78);
    box-shadow: inset 0 0 0 1px var(--line);
  }
  header .phase { margin-left: auto; color: var(--mute); font-size: 14px; font-weight: 400; white-space: nowrap; }
  @media (max-width: 720px) {
    header { height: auto; flex-wrap: wrap; padding: 8px 48px 8px 12px; }
    .tabs { flex: 1 0 100%; }
    .tab { font-size: 14px; padding: 6px 10px; }
  }
  main { position: relative; z-index: 1; flex: 1; display: flex; flex-direction: column; min-height: 0; }
  section { flex: 1; display: flex; flex-direction: column; min-height: 0; padding: 12px 20px; }
  section[hidden] { display: none; }
  body.view-pair section + section { box-shadow: inset 0 1px 0 var(--line); }
  .lang, .status { color: var(--mute); font-size: 14px; }
  .text {
    flex: 1;
    overflow: auto;
    margin-top: 12px;
    white-space: pre-wrap;
    word-break: break-word;
    color: #111111;
  }
  .text .dim { color: #8a9099; }
  .text .caret {
    display: inline-block;
    width: 0.12em;
    min-width: 2px;
    height: 0.92em;
    margin-left: 0.06em;
    background: #111111;
    vertical-align: text-bottom;
    animation: caret-blink 1.06s linear infinite;
  }
  .text.settled .caret { display: none; }
  @keyframes caret-blink {
    0%, 49% { opacity: 1; }
    50%, 100% { opacity: 0; }
  }
  body.small .text { font-size: 18px; line-height: 28px; }
  body.medium .text { font-size: 22px; line-height: 34px; }
  body.large .text { font-size: 32px; line-height: 46px; }
  .missing {
    margin: auto;
    color: var(--mute);
    font-size: 18px;
  }
  .lock {
    position: fixed;
    z-index: 2;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    background-color: var(--cream);
    background-image: repeating-linear-gradient(
      45deg,
      var(--band) 0 200px,
      var(--cream) 200px 780px
    );
  }
  .lock[hidden] { display: none; }
  .lock form {
    width: min(360px, calc(100% - 48px));
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .lock label { color: var(--mute); font-size: 14px; }
  .lock input {
    font: inherit;
    font-size: 18px;
    padding: 12px 14px;
    border: 1px solid var(--line);
    border-radius: 12px;
  }
  .lock button {
    font: inherit;
    font-size: 16px;
    padding: 12px;
    border: 0;
    border-radius: 12px;
    background: var(--ink);
    color: #fff;
  }
  .lock .error { min-height: 20px; color: #ff3b30; font-size: 14px; }
</style>
</head>
<body class="medium view-en">
<header>
  <div class="brand">
    <img class="logo" src="/logo.svg" alt="">
    <span class="brand-name">CSC - Translate</span>
  </div>
  <nav class="tabs" id="tabs">
    <button type="button" class="tab on" data-tab="en">English</button>
    <button type="button" class="tab" data-tab="zh">Chinese</button>
    <button type="button" class="tab" data-tab="pair" id="pairTab">English → Chinese</button>
  </nav>
  <span id="phase" class="phase"></span>
</header>
<main>
  <section id="sourcePane">
    <div class="lang" id="sourceLang">English</div>
    <div class="text" id="sourceText"></div>
    <div class="status" id="sourceStatus"></div>
  </section>
  <section id="targetPane" hidden>
    <div class="lang" id="targetLang">Chinese</div>
    <div class="text" id="targetText"></div>
    <div class="status" id="targetStatus"></div>
  </section>
</main>
<div id="lock" class="lock" hidden>
  <form id="lockForm">
    <div class="lock-brand">
      <img class="logo" src="/logo.svg" alt="">
      <span class="lock-name">CSC Translate</span>
    </div>
    <label for="code">Passcode</label>
    <input id="code" autocomplete="off" autocapitalize="off" spellcheck="false">
    <button type="submit">Enter</button>
    <p id="lockError" class="error"></p>
  </form>
</div>
<script>
const room = location.pathname.split("/").filter(Boolean).pop();
let tab = "en";
let direction = "en-zh";
let scale = "medium";

function labels(next) {
  return next === "zh-en" ? ["Chinese", "English"] : ["English", "Chinese"];
}

function showTab() {
  const pair = labels(direction);
  const englishIsSource = direction !== "zh-en";
  document.getElementById("pairTab").textContent = pair[0] + " → " + pair[1];
  document.getElementById("sourcePane").hidden = tab !== "pair" && tab !== (englishIsSource ? "en" : "zh");
  document.getElementById("targetPane").hidden = tab !== "pair" && tab !== (englishIsSource ? "zh" : "en");
  document.body.className = scale + " view-" + tab;
  for (const button of document.querySelectorAll(".tab")) {
    button.classList.toggle("on", button.dataset.tab === tab);
  }
}

const FOCUS_WINDOW_MS = 10000;
const ENGLISH_SETTLE_MS = 500;

function typeDelayMs(ch, backlog, english) {
  const base = /\\s/.test(ch) ? 10 : ch.charCodeAt(0) < 128 ? 16 : 32;
  if (english) return base;
  const paced = backlog > 48 ? 4 : backlog > 24 ? Math.floor(base / 2) : base;
  return Math.max(4, paced);
}

function commonPrefixLength(left, right) {
  const limit = Math.min(left.length, right.length);
  let i = 0;
  while (i < limit && left[i] === right[i]) i++;
  return i;
}

function integrateSpans(spans, next, nowMs) {
  const current = spans.map((span) => span.text).join("");
  if (next === current) return spans;
  if (current && next.startsWith(current)) {
    return spans.concat([{ text: next.slice(current.length), arrivedAtMs: nowMs }]);
  }
  const keep = commonPrefixLength(current, next);
  const kept = [];
  let consumed = 0;
  for (const span of spans) {
    if (consumed >= keep) break;
    const end = consumed + span.text.length;
    if (end <= keep) {
      kept.push(span);
      consumed = end;
    } else {
      kept.push({ text: span.text.slice(0, keep - consumed), arrivedAtMs: span.arrivedAtMs });
      consumed = keep;
    }
  }
  if (keep < next.length) kept.push({ text: next.slice(keep), arrivedAtMs: nowMs });
  return kept;
}

function focusPieces(spans, revealed, nowMs) {
  if (revealed <= 0) return [];
  const out = [];
  let seen = 0;
  for (const span of spans) {
    if (seen >= revealed) break;
    const take = Math.min(span.text.length, revealed - seen);
    if (take > 0) {
      const focused = nowMs - span.arrivedAtMs <= FOCUS_WINDOW_MS;
      const piece = span.text.slice(0, take);
      const last = out[out.length - 1];
      if (last && last.focused === focused) last.text += piece;
      else out.push({ text: piece, focused });
    }
    seen += span.text.length;
  }
  return out;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function followUpdate(state, text, english, now) {
  const next = text || "";
  if (!state.primed) {
    return {
      spans: next ? [{ text: next, arrivedAtMs: now }] : [],
      revealed: next.length,
      primed: true,
    };
  }
  const previous = state.spans.map((span) => span.text).join("");
  if (next === previous) return state;
  const prefix = commonPrefixLength(previous, next);
  const spans = integrateSpans(state.spans, next, now);
  let revealed = state.revealed;
  if (english) {
    revealed = Math.min(revealed, next.length);
  } else {
    if (prefix < revealed) revealed = prefix;
    if (revealed > next.length) revealed = next.length;
  }
  return { spans: spans, revealed: revealed, primed: true };
}

function stableEnd(spans, now, english) {
  const total = spans.reduce((count, span) => count + span.text.length, 0);
  if (!english) return total;
  let count = 0;
  for (const span of spans) {
    if (now - span.arrivedAtMs < ENGLISH_SETTLE_MS) break;
    count += span.text.length;
  }
  return count;
}

function createFollow(el, isEnglish) {
  let spans = [];
  let revealed = 0;
  let primed = false;
  let typing = false;
  let alive = true;
  let signature = "";

  function full() {
    return spans.map((span) => span.text).join("");
  }

  function paint() {
    const pieces = focusPieces(spans, revealed, Date.now());
    const nextSig = pieces.map((piece) => (piece.focused ? "1" : "0") + piece.text).join("\u0000");
    let caret = el.querySelector(":scope > .caret");
    if (!caret) {
      caret = document.createElement("span");
      caret.className = "caret";
      caret.setAttribute("aria-hidden", "true");
      el.appendChild(caret);
    }
    if (nextSig === signature) return;
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 48;
    signature = nextSig;
    const nodes = [...el.children].filter((node) => node !== caret);
    const sameShape = nodes.length === pieces.length && nodes.every((node, i) => {
      return node.className === (pieces[i].focused ? "focus" : "dim");
    });
    if (sameShape) {
      pieces.forEach((piece, i) => {
        if (nodes[i].textContent !== piece.text) nodes[i].textContent = piece.text;
      });
    } else {
      for (const node of nodes) node.remove();
      for (const piece of pieces) {
        const span = document.createElement("span");
        span.className = piece.focused ? "focus" : "dim";
        span.textContent = piece.text;
        el.insertBefore(span, caret);
      }
    }
    if (nearBottom) el.scrollTop = el.scrollHeight;
  }

  function limitAt(now) {
    return Math.min(full().length, stableEnd(spans, now, isEnglish()));
  }

  async function typeLoop() {
    if (typing) return;
    typing = true;
    try {
      while (alive) {
        const text = full();
        if (revealed >= limitAt(Date.now())) break;
        const ch = text[revealed];
        if (ch === undefined) break;
        await sleep(typeDelayMs(ch, text.length - revealed, isEnglish()));
        if (revealed >= limitAt(Date.now())) break;
        revealed += 1;
        paint();
      }
    } finally {
      typing = false;
      if (alive && revealed < limitAt(Date.now())) typeLoop();
    }
  }

  function setText(next) {
    const now = Date.now();
    const state = followUpdate({ spans: spans, revealed: revealed, primed: primed }, next, isEnglish(), now);
    spans = state.spans;
    revealed = state.revealed;
    primed = state.primed;
    paint();
    typeLoop();
  }

  function setLive(live) {
    el.classList.toggle("settled", !live);
  }

  const timer = setInterval(() => {
    if (!alive) return;
    if (spans.length) paint();
    if (revealed < limitAt(Date.now())) typeLoop();
  }, 250);

  return { setText: setText, setLive: setLive, stop() { alive = false; clearInterval(timer); } };
}

const sourceFollow = createFollow(document.getElementById("sourceText"), () => direction !== "zh-en");
const targetFollow = createFollow(document.getElementById("targetText"), () => direction === "zh-en");

function statusOf(state) {
  if (state.status) return state.status;
  if (state.phase === "connecting") return "Connecting…";
  if (state.phase === "finishing") return "Finishing…";
  if (state.paused) return "Paused";
  if (state.phase === "idle") return "Ended";
  return "Listening";
}

function apply(state) {
  if (!state || state.phase === "idle") {
    waiting();
    return;
  }
  direction = state.direction || "en-zh";
  scale = state.typeScale || "medium";
  const pair = labels(direction);
  document.getElementById("sourceLang").textContent = pair[0];
  document.getElementById("targetLang").textContent = pair[1];
  const status = statusOf(state);
  document.getElementById("sourceStatus").textContent = status;
  document.getElementById("targetStatus").textContent = status;
  document.getElementById("phase").textContent = status === "Listening to system" ? "" : status;
  sourceFollow.setLive(true);
  targetFollow.setLive(true);
  sourceFollow.setText(state.sourceText);
  targetFollow.setText(state.targetText);
  showTab();
}

let events = null;

function waiting() {
  document.getElementById("phase").textContent = "Waiting for this session to start";
  document.getElementById("sourceStatus").textContent = "";
  document.getElementById("targetStatus").textContent = "";
  hideLock();
  sourceFollow.setText("");
  targetFollow.setText("");
  sourceFollow.setLive(false);
  targetFollow.setLive(false);
}

function showLock(wrong) {
  document.getElementById("lock").hidden = false;
  document.getElementById("lockError").textContent = wrong ? "Wrong passcode" : "";
  document.getElementById("code").focus();
}

function hideLock() {
  document.getElementById("lock").hidden = true;
  document.getElementById("lockError").textContent = "";
}

async function load() {
  const response = await fetch("/sessions/" + room);
  if (response.status === 401) {
    if (events) {
      events.close();
      events = null;
    }
    showLock(false);
    return;
  }
  if (!response.ok) {
    hideLock();
    waiting();
    setTimeout(load, 2000);
    return;
  }
  hideLock();
  const state = await response.json();
  if (!state || state.phase === "idle") {
    if (events) {
      events.close();
      events = null;
    }
    waiting();
    setTimeout(load, 2000);
    return;
  }
  apply(state);
  if (events) events.close();
  events = new EventSource("/sessions/" + room + "/events");
  events.addEventListener("state", (event) => apply(JSON.parse(event.data)));
  events.onerror = () => {
    if (events && events.readyState === EventSource.CLOSED) {
      events = null;
      load();
    }
  };
}

document.getElementById("lockForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const passcode = document.getElementById("code").value.trim();
  const response = await fetch("/sessions/" + room + "/join", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ passcode: passcode }),
  });
  if (response.status === 401) {
    showLock(true);
    return;
  }
  if (!response.ok) {
    hideLock();
    waiting();
    setTimeout(load, 2000);
    return;
  }
  hideLock();
  load();
});

document.getElementById("tabs").addEventListener("click", (event) => {
  const button = event.target.closest(".tab");
  if (!button) return;
  tab = button.dataset.tab;
  showTab();
});

load();
</script>
</body>
</html>
"""


def main() -> None:
    threading.Thread(target=expiry_loop, name="expiry", daemon=True).start()
    ThreadingHTTPServer.allow_reuse_address = True
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    server.daemon_threads = True
    print(f"csc-relay listening on http://{HOST}:{PORT}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("stopping", flush=True)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
