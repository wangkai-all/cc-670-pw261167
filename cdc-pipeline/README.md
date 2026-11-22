# CDC 变更日志配对发布管线

把带**更新前像 / 更新后像**的变更日志，转换成可直接入分析仓的完整变更。三个**独立进程、独立端口、独立 SQLite 库**的服务，配套零依赖网页控制台。

| 服务 | 端口 | 库 | 职责 |
| --- | --- | --- | --- |
| 接收服务 receiver | 8071 | `data/receiver.db` | 网页导入日志片段、调整投递顺序、重复/篡改投递、向配对服务提供拉取流 |
| 配对服务 pairing | 8072 | `data/pairing.db` | 按来源事务+语句标识配对半像、事务状态机、异常区、冲突留痕、按提交序号发布 |
| 目标仓服务 target | 8073 | `data/target.db` | 整事务原子提交、主键迁移撤旧加新、表/行/提交/审计查询 |

仅依赖 Python 3.11 标准库（`http.server` + `sqlite3`），无需安装任何第三方包。

## 快速开始

```bash
cd cdc-pipeline
./run_all.sh                 # 一键启动三服务，Ctrl-C 一起停止
# 浏览器打开任一地址（页面同源托管，三个端口都能访问全部页面）
#   http://127.0.0.1:8071/  总览
#   http://127.0.0.1:8071/receiver.html  接收/投递控制台
#   http://127.0.0.1:8072/pairing.html   配对/异常/发布控制台
#   http://127.0.0.1:8073/target.html    目标仓核对
./stop_all.sh                # 停止
```

也可以分别启动（每个都是独立进程）：

```bash
python3 -m cdc_pipeline.target.service   --port 8073 --data-dir data
python3 -m cdc_pipeline.receiver.service --port 8071 --data-dir data
python3 -m cdc_pipeline.pairing.service  --port 8072 \
    --receiver-url http://127.0.0.1:8071 --target-url http://127.0.0.1:8073
```

## 页面演示流程（对应验收要求）

1. **接收页**点「生成项目演示日志」→ 生成 10 个片段文件（`data/generated_logs/`）并自动进入投递流。
2. **配对页**观察轮询消费（默认 1 秒一轮，消费位置持久化）：
   - `T2` 的前像、后像、事务结束分三批到达，最终按 `txn_id + stmt_id` 配平；
   - `T4` 只有后像就收到 COMMIT → 整笔进入**异常区**，产生 `INCOMPLETE_TXN` 冲突；
   - `T5` 回滚 → `ROLLED_BACK`，永不发布。
3. 接收页对 `F01` 点「重传（原内容）」→ 事件级幂等，不新增配对/冲突/目标数据；
   点「重传（篡改内容）」→ 配对页出现 `CONTENT_MISMATCH`，明细列出首次投递与重传的字段差异，篡改副本被拒绝。
4. 接收页可用 ↑/↓ 调整**未消费**投递的顺序；已消费投递拒绝移动（不能越过消费位置）。
5. 配对页「重新核验」或等待 `F08`（迟到前像）被轮询到 → `T4` 自动恢复 `COMPLETE`，旧冲突核销。
6. 配对页「发布台」严格按来源 `commit_seq` 逐笔发布；异常事务会挡住后续序号，不跳过未完成事务。
7. **目标仓页**核对：
   - `T3` 主键 `300→301` 表现为同提交内 `D(300) + I(301)`，关联表 `order_items(301,1)` 同事务可见；
   - 审计日志保留全部撤旧/加新留痕；回滚与异常事务均无数据入仓。

## 事件协议

片段支持 JSON 数组或 NDJSON（每行一个事件）。

```json
{
  "event_id": "E201",
  "source_id": "SRC1",
  "txn_id": "T2",
  "stmt_id": "S1",
  "seq": 1,
  "table": "orders",
  "op": "U",
  "key_columns": ["id"],
  "before": {"row": {"id": 200, "cust": "李四", "amount": 50, "status": "PAID"}},
  "after":  {"row": {"id": 200, "cust": "李四", "amount": 88, "status": "DONE"}}
}
```

事务边界事件：

```json
{"event_id": "E202", "source_id": "SRC1", "txn_id": "T2",
 "kind": "TXN_END", "txn_status": "COMMIT", "commit_seq": 2}
```

- `op=I` 只带 `after`；`op=D` 只带 `before`；`op=U` 的 `before/after` 可在不同批次、不同片段中到达。
- 同一 `event_id` 的两个半像按槽位（before/after）分别幂等，允许跨批；同槽位重复且内容不同 → `CONTENT_MISMATCH`。

## 核心设计规则（对应题目约束）

1. **配对键 = `source_id + txn_id + stmt_id`**，绝不按主键找最近一条；不同事务更新同一主键也不会串配（见单元测试 [1]）。
2. 同事务多次更新按 `seq` 排序，保留真实先后与中间态。
3. 缺一半的 U 保持 `WAITING_BEFORE/WAITING_AFTER`，**不擅自当新增或删除**；收到明确 `TXN_END/COMMIT` 仍缺失 → 整事务进异常区；迟到半像到达后自动重算（也可手动「重新核验」）恢复。
4. 发布时主键值变化的 U 展开为 `D(旧键) + I(新键)`；同一事务的所有变更在目标仓**一个本地事务内原子提交**，关联表一起可见，冲突则整笔回滚。
5. 三层幂等：接收端投递留痕 → 配对端「事件+槽位」内容指纹去重 → 目标仓按 `(source_id, txn_id)` 唯一约束去重。
6. 同标识内容改变不覆盖首次内容，写入冲突表并给出字段级差异。
7. 配对状态与消费位置（`meta.last_attempt_id`）持久化；进程重启后从检查点继续，未完成/异常事务不会被跳过。
8. `ROLLBACK` 事务只做状态留痕，不进入发布队列。

## 测试

```bash
python3 tests/unit_test.py     # 19 项进程内状态机/事务语义测试
python3 tests/e2e_test.py      # 48 项三服务端到端测试（自动起停、含重启恢复）
python3 tests/manual_walkthrough.py  # 分阶段手工演示（需先启动三服务并关闭自动轮询）
```

## 目录结构

```
cdc-pipeline/
├── cdc_pipeline/
│   ├── common.py              # SQLite 封装、内容指纹、片段解析
│   ├── httpapp.py             # 零依赖 HTTP 路由/静态托管框架
│   ├── receiver/              # 接收服务（含项目日志生成器 generator.py）
│   ├── pairing/               # 配对服务（store 状态机 / poller 拉取 / target_client）
│   ├── target/                # 目标仓服务
│   └── web/                   # 四个原生 HTML/JS/CSS 页面
├── tests/
├── run_all.sh / stop_all.sh
└── README.md
```
