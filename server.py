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
from urllib.parse import urlparse

HOST = "0.0.0.0"
PORT = 8091
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
    if code is None or code == session.passcode:
        return
    session.passcode = code
    session.grant = secrets.token_urlsafe(18) if code else ""
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


def publish(session: Session) -> None:
    payload = json.dumps(session.public(), ensure_ascii=False, separators=(",", ":"))
    for watcher in list(session.watchers):
        try:
            watcher.put_nowait(payload)
        except queue.Full:
            try:
                watcher.get_nowait()
            except queue.Empty:
                pass
            try:
                watcher.put_nowait(payload)
            except queue.Full:
                pass


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
        if path.startswith("/watch/"):
            room = canonical_room(path.removeprefix("/watch/"))
            if room is None:
                self.send_error_json(404, "not found")
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
            if session is None:
                session = Session(room, state)
                sessions[room] = session
                print(f"created {room}", flush=True)
            else:
                session.state = state
                print(f"reused {room}", flush=True)
            apply_passcode(session, passcode)
            session.touch()
            publish(session)
            body = session.public()
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
            session.touch()
            body = session.public()
            publish(session)
        self.send_json(body)

    def join_session(self, room: str, patch: dict) -> None:
        try:
            code = canonical_passcode(patch.get("passcode", ""))
        except ValueError:
            self.send_json({"locked": True}, status=401)
            return
        with lock:
            session = sessions.get(room)
            if session is None:
                self.send_error_json(404, "session not found")
                return
            if session.passcode and not same_secret(code, session.passcode):
                self.send_json({"locked": True}, status=401)
                return
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
            if session is None:
                self.send_error_json(404, "session not found")
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
            if session is None:
                self.send_error_json(404, "session not found")
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

    def send_html(self, html: str) -> None:
        raw = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def send_error_json(self, status: int, message: str) -> None:
        self.send_json({"error": message}, status=status)


WATCH_HTML = """<!DOCTYPE html>
<html lang="zh-Hans">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CSC Translate</title>
<style>
  :root {
    --ink: #111111;
    --mute: #9ca3af;
    --pane: #f3f4f6;
    --line: #e6e6e6;
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }
  body {
    display: flex;
    flex-direction: column;
    color: var(--ink);
    background: #fff;
    font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", "Noto Sans SC", sans-serif;
  }
  header {
    display: flex;
    align-items: center;
    gap: 10px;
    height: 56px;
    padding: 0 20px;
    border-bottom: 1px solid var(--line);
    font-size: 16px;
    font-weight: 500;
  }
  header .arrow { color: var(--mute); }
  header .phase { margin-left: auto; color: var(--mute); font-size: 14px; font-weight: 400; }
  main { flex: 1; display: flex; flex-direction: column; min-height: 0; }
  section { flex: 1; display: flex; flex-direction: column; min-height: 0; padding: 12px 20px; }
  section.pane { background: var(--pane); }
  body.translation #sourcePane, body.source #targetPane { display: none; }
  .lang, .status { color: var(--mute); font-size: 14px; }
  .text {
    flex: 1;
    overflow: auto;
    margin-top: 12px;
    white-space: pre-wrap;
    word-break: break-word;
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
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    background: #fff;
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
<body class="medium split">
<header>
  <span id="sourceLabel">English</span>
  <span class="arrow">→</span>
  <span id="targetLabel">Chinese</span>
  <span id="phase" class="phase"></span>
</header>
<main>
  <section id="sourcePane">
    <div class="lang" id="sourceLang">English</div>
    <div class="text" id="sourceText"></div>
    <div class="status" id="sourceStatus"></div>
  </section>
  <section id="targetPane" class="pane">
    <div class="lang" id="targetLang">Chinese</div>
    <div class="text" id="targetText"></div>
    <div class="status" id="targetStatus"></div>
  </section>
</main>
<div id="lock" class="lock" hidden>
  <form id="lockForm">
    <label for="code">Passcode</label>
    <input id="code" autocomplete="off" autocapitalize="off" spellcheck="false">
    <button type="submit">Enter</button>
    <p id="lockError" class="error"></p>
  </form>
</div>
<script>
const room = location.pathname.split("/").filter(Boolean).pop();

function labels(direction) {
  return direction === "zh-en" ? ["Chinese", "English"] : ["English", "Chinese"];
}

function statusOf(state) {
  if (state.status) return state.status;
  if (state.phase === "connecting") return "Connecting…";
  if (state.phase === "finishing") return "Finishing…";
  if (state.paused) return "Paused";
  if (state.phase === "idle") return "Ended";
  return "Listening";
}

function setText(el, text) {
  const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 48;
  el.textContent = text || "";
  if (nearBottom) el.scrollTop = el.scrollHeight;
}

function apply(state) {
  const pair = labels(state.direction);
  document.getElementById("sourceLabel").textContent = pair[0];
  document.getElementById("targetLabel").textContent = pair[1];
  document.getElementById("sourceLang").textContent = pair[0];
  document.getElementById("targetLang").textContent = pair[1];
  const status = statusOf(state);
  document.getElementById("sourceStatus").textContent = status;
  document.getElementById("targetStatus").textContent = status;
  document.getElementById("phase").textContent = state.phase === "idle" ? "这场已结束" : status;
  document.body.className = (state.typeScale || "medium") + " " + (state.displayMode || "split");
  setText(document.getElementById("sourceText"), state.sourceText);
  setText(document.getElementById("targetText"), state.targetText);
}

let events = null;

function waiting() {
  document.getElementById("phase").textContent = "等待这场开始";
}

function showLock(wrong) {
  document.getElementById("lock").hidden = false;
  document.getElementById("lockError").textContent = wrong ? "密码不对" : "";
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
  apply(await response.json());
  if (events) events.close();
  events = new EventSource("/sessions/" + room + "/events");
  events.addEventListener("state", (event) => apply(JSON.parse(event.data)));
  events.onerror = () => {
    if (events && events.readyState === EventSource.CLOSED) {
      events = null;
      setTimeout(load, 2000);
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
