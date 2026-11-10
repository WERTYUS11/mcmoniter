import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "mc_monitor.db")

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS player_count (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,          -- unix 时间戳（秒）
                online INTEGER NOT NULL,       -- 在线人数
                max_players INTEGER NOT NULL,  -- 最大人数
                success INTEGER NOT NULL       -- 1 成功 / 0 失败
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ts ON player_count(ts)")
        conn.commit()

def insert_record(ts: int, online: int, max_players: int, success: bool):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO player_count (ts, online, max_players, success) VALUES (?, ?, ?, ?)",
            (ts, online, max_players, 1 if success else 0),
        )
        conn.commit()

def query_records(start_ts: int, end_ts: int):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "SELECT ts, online, max_players, success FROM player_count "
            "WHERE ts BETWEEN ? AND ? ORDER BY ts ASC",
            (start_ts, end_ts),
        )
        return cur.fetchall()