# event-agg — 事件时间聚合服务

零依赖 Node.js 服务，处理**乱序、迟到、撤回、窗口重开**，窗口关闭后到达的极晚事件进入**修订队列**而不是静默丢弃。

## 运行

```bash
npm start          # http://localhost:8080 （PORT、DATA_FILE 环境变量可覆盖）
npm test           # 17 个测试：重复事件 / 迟到 / 撤回 / 重开 / 重算边界 / 时区切换 / 任务恢复
```

## 核心语义

- **水位策略**（可配置，`POST /api/config`）：
  - `bounded-out-of-orderness`：水位 = 最大事件时间 − `maxOutOfOrdernessMs`，单调递增；
  - `punctuated`：由 `{type:"watermark", value}` 事件推进。
- **允许迟到** `allowedLatenessMs`：水位越过 `窗口结束 + 允许迟到` 时窗口关闭（OPEN → CLOSED，落终版）。
  迟到但窗口未关的事件正常并入，记 `LATE_EVENT` 版本。
- **修订队列**：水位已越过 `窗口结束 + 允许迟到` 后到达的事件（或对已关闭窗口的撤回）进入修订队列，
  可「应用」（自动重开窗口 → 重算 → 重新关闭）或「丢弃」，绝不静默丢失。
- **撤回**：`{type:"retract", retractOf:<eventId>}` 按 eventId 精确撤销贡献，记 `RETRACTION` 版本。
- **去重**：同一 `eventId` 全局只计一次（含重启后）。
- **重算边界** `recomputeBoundary`：
  - `window`：只重算受影响窗口，同 key 后续窗口的累计值标记 `stale`；
  - `key`：级联重算同 key 所有后续窗口的累计值，记 `CASCADE` 版本。
- **窗口重开**：修订应用自动重开，或 `POST /api/windows/:id/reopen` 手动重开。
- **时区**：窗口按配置时区的墙钟对齐（跨 DST 的"天"窗口为 23/25 小时）；切换时区只影响新事件，历史窗口保留原时区。
- **任务恢复**：每次变更后写检查点（`data/checkpoint.json`，原子替换），重启自动恢复全部状态（窗口、版本历史、去重表、修订队列、水位）。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/events` | 写入事件（单个或数组）：`{eventId,key,eventTime,value}` / `{type:"retract",retractOf}` / `{type:"watermark",value}` |
| GET | `/api/state` | 配置、水位、全部窗口、修订队列 |
| GET | `/api/windows/:id` | 窗口详情：版本历史（含 diff 与重算原因）、事件列表 |
| POST | `/api/config` | 更新水位策略 / 允许迟到 / 重算边界 / 时区 / 窗口大小 |
| POST | `/api/revisions/:id/apply` `…/discard` | 应用或丢弃修订 |
| POST | `/api/windows/:id/reopen` | 手动重开已关闭窗口 |
| POST | `/api/checkpoint` | 立即落盘 |

## 页面

`GET /` 单页 UI：窗口状态表（OPEN/CLOSED/REOPENED、当前值、累计值、stale 标记）、
窗口详情（版本历史：每版值、较上一版 diff、与当前值差异、重算原因）、修订队列（应用/丢弃）、
策略配置表单、事件写入/撤回/水位推进表单，3 秒自动刷新。

## 版本历史中的重算原因

`ON_TIME` · `LATE_EVENT` · `RETRACTION` · `WINDOW_CLOSE` · `REOPEN` · `MANUAL_REOPEN` · `REVISION_APPLIED` · `CASCADE`
