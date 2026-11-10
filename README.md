# MC 服务器人数监控系统文档

## 项目简介

一个基于 FastAPI + ECharts 的 Minecraft 服务器在线人数监控系统。后端定时通过 Minecraft Server List Ping 协议采集在线人数并存入 SQLite，前端通过 SSE 实时推送 + 定时全量拉取的方式展示人数变化曲线。

## 目录结构

```text
project/
├── backend/
│   ├── main.py            # FastAPI 应用入口，含 ping 协议实现
│   ├── db.py              # SQLite 数据访问层
│   └── requirements.txt
├── frontend/
│   └── index.html         # 页面结构 + 前端逻辑
└── static/
    ├── style.css          # 样式（含深色模式、移动端适配）
    └── echarts.min.js     # ECharts 5.5.0
```

## 技术栈

| 层级 | 技术 |
|------|------|
| 后端框架 | FastAPI 0.115 |
| ASGI 服务器 | Uvicorn 0.30 |
| 定时任务 | APScheduler 3.10（AsyncIOScheduler） |
| 数据库 | SQLite（标准库 `sqlite3`） |
| 实时推送 | Server-Sent Events (SSE) |
| 前端图表 | ECharts 5.5.0 |
| 前端通信 | Fetch API + EventSource |

## 后端设计

### 1. Minecraft Server List Ping

手写实现 MC Java 版状态查询协议，无需第三方库：

​```text
Handshake → Status Request → 读取 VarInt 长度 → 读取 JSON → 解析 players.online
​```

核心函数：
- `_pack_varint` / `_pack_string` / `_pack_handshake`：协议编码
- `_read_varint` / `_read_exact`：协议解码
- `ping_mc(host, port)`：同步 ping，返回 `(online, motd_max)`

协议版本号硬编码为 `767`（1.21），实践上大多数服务器不校验版本，仅用于握手。

### 2. 数据采集任务

​```python
async def record_once():
    ts = int(time.time())
    try:
        online, motd_max = await asyncio.to_thread(ping_mc, MC_HOST, MC_PORT)
        insert_record(ts, online, MC_MAX_PLAYERS, True)
        await broadcast({...})
    except Exception:
        insert_record(ts, 0, MC_MAX_PLAYERS, False)
        await broadcast({...})
​```

- 使用 `asyncio.to_thread` 避免同步 socket 阻塞事件循环
- 失败也落库（`success=0`），便于前端区分"离线"与"0 人"
- 通过 `broadcast` 向所有 SSE 订阅者推送

### 3. 调度器

在 `lifespan` 中启动 `AsyncIOScheduler`：

​```python
scheduler.add_job(
    record_once, "interval", seconds=PING_INTERVAL,
    next_run_time=datetime.now(), id="mc_ping",
    replace_existing=True, max_instances=1, coalesce=True,
)
​```

- `max_instances=1`：防止任务重叠
- `coalesce=True`：错过多次只执行一次
- `next_run_time=datetime.now()`：启动即采集

### 4. SSE 广播机制

​```python
_subscribers: set[asyncio.Queue] = set()

async def broadcast(payload):
    for q in _subscribers:
        q.put_nowait(payload)   # 队列满则视为死连接
​```

`/api/stream` 端点：
- 连接时先推一条最新记录（300s 内）
- 每 20s 无数据推 `: keepalive\n\n` 防止代理断连
- 响应头 `X-Accel-Buffering: no` 禁用 Nginx 缓冲

### 5. API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/records?hours=N` | 拉取 N 小时历史（1~720） |
| GET | `/api/latest` | 最近 300s 的最新一条 |
| GET | `/api/stream` | SSE 实时推送 |
| GET | `/` | 返回前端页面 |
| GET | `/static/*` | 静态资源 |

### 6. 数据库

单表 `player_count`：

​```sql
CREATE TABLE player_count (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,           -- unix 秒
    online INTEGER NOT NULL,
    max_players INTEGER NOT NULL,
    success INTEGER NOT NULL       -- 1/0
);
CREATE INDEX idx_ts ON player_count(ts);
​```

时间戳索引保证区间查询效率。

## 前端设计

### 1. 数据流

​```text
初始 → loadFullData() → 渲染
     ↓
     restartTimer() 每 interval 秒全量刷新（兜底）
     ↓
     connectSSE() 实时 pushPoint() 追加（主路径）
​```

双重保障：SSE 挂掉时仍有定时轮询；SSE 正常时体验接近实时。

### 2. 图表配置要点

- **主曲线**：`smooth: 0.35` 平滑，渐变面积填充，线条带 glow 阴影
- **最大人数**：虚线参考线
- **变化点**：scatter 标记在线人数发生变化的时刻（`extractChangePoints`）
- **dataZoom**：`inside` 类型，支持滚轮缩放、拖动平移，并记住缩放状态

### 3. 主题系统

三态循环：`auto → light → dark → auto`

- 颜色全部从 CSS 变量读取（`readColors()`），保证图表与 UI 一致
- 监听 `prefers-color-scheme` 变化，auto 模式下自动切换
- 状态持久化到 `localStorage`

### 4. 移动端适配

`@media (max-width: 640px)` 断点下：
- header 换行布局，stats 横向滚动
- 分段按钮均分整行
- 图表边距、字号、线宽、点大小全部缩小
- 隐藏右下角提示

## 配置项

`main.py` 顶部：

​```python
MC_HOST = "YOUR SERVER"   # 服务器地址
MC_PORT = 25565
PING_INTERVAL = 30        # 采集间隔（秒），前后端共用
MC_MAX_PLAYERS = 35       # 显示的人数上限（忽略 MOTD）
​```

前端 `refreshInterval` 由后端 `/api/records` 响应中的 `interval` 字段同步，修改 `PING_INTERVAL` 后前端自动适配。

## 启动方式

​```bash
pip install -r requirements.txt
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000
​```

访问 `http://localhost:8000`。

## 已知限制与可扩展方向

| 限制 | 建议 |
|------|------|
| SQLite 无自动清理，长期运行会膨胀 | 增加定期删除 N 天前记录的定时任务 |
| `MC_MAX_PLAYERS` 为固定值 | 可改为使用 MOTD 返回的真实 `max` |
| SSE 订阅者集合无鉴权 | 生产环境建议加限流 / 鉴权 |
| 单服务器监控 | 可扩展为多服务器（host 作为参数） |
| 无告警机制 | 可加 Webhook / 邮件在离线时通知 |
| 时间戳秒级精度 | 若需更细粒度可改毫秒 |

## 附：数据记录示例

​```json
{
  "ts": 1730000000,
  "online": 12,
  "max": 35,
  "ok": true
}
​```

`ok: false` 表示本次 ping 失败，前端在 header 显示"离线"，曲线该点为 0。
