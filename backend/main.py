import asyncio
import json
import os
import socket
import struct
import time
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from db import init_db, insert_record, query_records

# ====== 配置 ======
MC_HOST = "YOUR SERVER"      # ← 改成你的服务器地址
MC_PORT = 25565
PING_INTERVAL = 30              # 秒（前后端共用）
MC_MAX_PLAYERS = 35             # 真实人数上限（忽略 MOTD，如果不需要此项可以设置为与服务器人数同样大小）

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_FRONTEND_DIR = os.path.join(_BASE_DIR, "..", "frontend")
_STATIC_DIR = os.path.join(_BASE_DIR, "..", "static")

# ========== MC Java 版 Server List Ping ==========

def _pack_varint(num: int) -> bytes:
    out = b""
    while True:
        b = num & 0x7F
        num >>= 7
        if num:
            out += bytes([b | 0x80])
        else:
            out += bytes([b])
            return out


def _pack_string(s: str) -> bytes:
    data = s.encode("utf-8")
    return _pack_varint(len(data)) + data


def _pack_handshake(host: str, port: int) -> bytes:
    payload = (
        _pack_varint(0x00)
        + _pack_varint(767)
        + _pack_string(host)
        + struct.pack(">H", port)
        + _pack_varint(0x01)
    )
    return _pack_varint(len(payload)) + payload


def _pack_status_request() -> bytes:
    payload = _pack_varint(0x00)
    return _pack_varint(len(payload)) + payload


def _read_varint(sock: socket.socket) -> int:
    result = 0
    shift = 0
    while True:
        b = sock.recv(1)
        if not b:
            raise ConnectionError("连接被关闭")
        byte = b[0]
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result
        shift += 7
        if shift > 35:
            raise ValueError("VarInt 过长")


def _read_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("连接被关闭")
        buf += chunk
    return buf


def ping_mc(host: str, port: int, timeout: float = 5.0):
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(_pack_handshake(host, port))
        sock.sendall(_pack_status_request())
        _ = _read_varint(sock)
        packet_id = _read_varint(sock)
        if packet_id != 0x00:
            raise ValueError(f"意外的 packet id: {packet_id}")
        json_len = _read_varint(sock)
        data = json.loads(_read_exact(sock, json_len).decode("utf-8"))
        players = data.get("players", {})
        return int(players.get("online", 0)), int(players.get("max", 0))


# ========== SSE 广播器 ==========

_subscribers: set[asyncio.Queue] = set()


async def broadcast(payload: dict):
    dead = []
    for q in _subscribers:
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        _subscribers.discard(q)


# ========== 采集任务 ==========

async def record_once():
    ts = int(time.time())
    try:
        online, motd_max = await asyncio.to_thread(ping_mc, MC_HOST, MC_PORT)
        insert_record(ts, online, MC_MAX_PLAYERS, True)
        print(f"[{time.strftime('%H:%M:%S')}] {MC_HOST} online={online}/{MC_MAX_PLAYERS} (MOTD max={motd_max})")
        await broadcast({"ts": ts, "online": online, "max": MC_MAX_PLAYERS, "ok": True})
    except Exception as e:
        insert_record(ts, 0, MC_MAX_PLAYERS, False)
        print(f"[{time.strftime('%H:%M:%S')}] ping 失败: {e}")
        await broadcast({"ts": ts, "online": 0, "max": MC_MAX_PLAYERS, "ok": False})


# ========== FastAPI ==========

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    loop = asyncio.get_running_loop()
    scheduler = AsyncIOScheduler(event_loop=loop)
    scheduler.add_job(
        record_once, "interval", seconds=PING_INTERVAL,
        next_run_time=datetime.now(), id="mc_ping",
        replace_existing=True, max_instances=1, coalesce=True,
    )
    scheduler.start()
    print(f"[scheduler] 已启动，每 {PING_INTERVAL}s ping {MC_HOST}:{MC_PORT}")
    yield
    scheduler.shutdown(wait=False)
    print("[scheduler] 已停止")


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# 静态资源：/static/style.css
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.get("/api/records")
def get_records(hours: int = Query(6, ge=1, le=24 * 30)):
    end_ts = int(time.time())
    start_ts = end_ts - hours * 3600
    rows = query_records(start_ts, end_ts)
    return {
        "host": MC_HOST,
        "interval": PING_INTERVAL,
        "now": end_ts,
        "data": [
            {"ts": r[0], "online": r[1], "max": r[2], "ok": bool(r[3])}
            for r in rows
        ],
    }


@app.get("/api/latest")
def latest():
    end_ts = int(time.time())
    rows = query_records(end_ts - 300, end_ts)
    base = {"interval": PING_INTERVAL, "now": end_ts}
    if not rows:
        return {**base, "online": None, "max": None, "ok": False, "ts": None}
    r = rows[-1]
    return {**base, "online": r[1], "max": r[2], "ok": bool(r[3]), "ts": r[0]}


@app.get("/api/stream")
async def stream():
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)

    async def event_gen():
        _subscribers.add(queue)
        try:
            end_ts = int(time.time())
            rows = query_records(end_ts - 300, end_ts)
            if rows:
                r = rows[-1]
                init = {"ts": r[0], "online": r[1], "max": r[2], "ok": bool(r[3])}
                yield f"data: {json.dumps(init)}\n\n"
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=20)
                    yield f"data: {json.dumps(item)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            _subscribers.discard(queue)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/")
def index():
    return FileResponse(os.path.join(_FRONTEND_DIR, "index.html"))