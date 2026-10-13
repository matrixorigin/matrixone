# INSERT IGNORE 有序 FK：有界生产入口性能资格

此目录只提供**测试工具**，不改变生产算法、wire 或默认策略。对应 #28238 / #30107 的性能 review。必须在隔离的、可丢弃的单 CN/TN/LOG 服务上运行；不得把插桩服务发布到生产。

## 方法与边界

- 同一个 `benchmark.py` 通过真实 MySQL frontend 执行 SQL，含解析、优化、事务、allocator、FK/锁、存储 reader 和结果返回；不是 mock reader 或只测独立函数。
- baseline 和 head 使用同一当前 main 基线、工具链、native 选项、`GOEXPERIMENT`、GOMAXPROCS/GOGC、fixture、连接/服务配置。每个服务全新 data directory，串行运行，避免端口/缓存混用。记录精确 revision、二进制 SHA-256、`go version -m`、启动命令、原始 JSONL/退出码与主机负载。
- `ordinary` 保持向量 FK 对照；`auto_pk` 引用父 PK；`auto_nonpk` 引用父非主 UNIQUE；`auto_composite` 引用父非主复合 UNIQUE；`self_composite` 无自增、复合 self-FK 指向先前提交的父行，刻意不触发 current-row/accepted-row 快捷命中。
- 默认矩阵 child 16/64/256，parent 256/4096，三次独立建表/提交，F=1；最多 300 个计量窗口、20 秒 SQL socket deadline、30 秒计量窗口、5 秒锁/取消 deadline。不是容量极限或 p99 研究。
- `INSERT IGNORE` 为完整矩阵；`--modes load local` 可复用同一数据 oracle 测显式 `LOAD IGNORE` / `LOAD LOCAL IGNORE`。LOAD fixture 在本机共享文件系统，不能据此声称跨机器/对象存储吞吐。
- 默认 `--storage persisted` 在计量前显式 flush 父表并确认成功。它是**已持久化、允许缓存**的读，非冷缓存实验；`memory` 不显式 flush，但后台可能持久化，仍以实际块计量为准。
- DDL、fixture、flush、HTTP start/stop、oracle 不计入 SQL 执行时间；单独记录 execute、commit 与 committed throughput。内存采样和计量 hook 的开销存在于两端，不能冒充无插桩的生产绝对吞吐。
- oracle 校验接受行数、无孤儿及自增最终 `(min,max,sum)`。只有计量结束后运行 oracle，避免 join 污染扫描数据。

## 可复核计量（不要混淆）

`instrument.py install ROOT` 只给 disposable 产物安装对称观察点，并为每次 reader.read 的 ctx 附加同一块 CounterSet，覆盖原有 filter.Valid 条件外的读；不是修改过滤/发号算法。manifest 保存原文及前后 SHA-256；**结束必须 remove**。它不使用 `git checkout`，检测到后续独立修改会拒绝覆盖。发布前 `git diff -- pkg/incrservice/allocator.go pkg/vm/engine/readutil/reader.go` 必须无差异，两个 `CLAUDE_fkbench.go` 必须不存在。

| JSON 字段 | 含义 / 不能声称的内容 |
| --- | --- |
| `reader_calls` | 目标父表的真实 `readutil.reader.read` 次数，含 EOF；并非 relation/range 构造数 |
| `reader_returned_rows` | 返回给执行层的父行；在 ordered 路径中是完整 residual FK 判定前的逻辑扫描行。PK/storage pruning 已发生，不是所有底层 row predicate 的求值次数 |
| `reader_materialized_bytes` | 上述返回 batch 的 `Size()` 累积；不是磁盘/S3 字节、网络字节或瞬时存活内存 |
| `selected_persisted_block_rows` | 被 reader 选中的持久块原始行数累计；PK 只命中一行也可能选中整个块，不代表所有列都被解码 |
| `process_fileservice_read_bytes` | `objectio.BlkReadStats.CounterSet.FileService.ReadSize` 窗口差，包含同进程其它 I/O，允许缓存命中；不是父表精确独占磁盘读取量 |
| `allocator_store_calls` / `allocator_requested_values` | 目标 child table 的真实 `store.Allocate` 调用/申请数，在窗口内开始计入；DDL 预分配与窗口外异步预取不算，零不代表 allocator 从未工作 |
| `sampled_peak_heap_objects_bytes` / `sampled_peak_rss_bytes` | 起/止及每 10ms 的进程绝对采样峰值，包含 CN/TN/LOG/native/cache，不是语句独占内存、精确高水位或 native-only 分解 |

锁实验先让 child 事务完成 INSERT、**不提交**，然后在另一个连接更新未被引用的 parent 行。只有 `mo_locks()` 指定表的非空 `lock_wait` 才构成等待 barrier；非阻塞完成亦记录。分别观察 holder commit 后完成与 `KILL QUERY` 后错误返回；还反向持有 parent 写事务，取消被其阻塞的 child INSERT 并确认零提交。不以 sleep 判定锁等待，5 秒内不结束即失败。取消错误必须可识别为 cancel/interrupted/killed，不能把 socket timeout 当成功。

## 执行

前提：按仓库规范用 `gwt-add` 准备 baseline/head；用户 WIP 不可覆盖。Go/native 构建遵循 `Makefile` 与当前 `.agents/skills/mo-dev`。以下操作每个 worktree 分别执行，**不能同时更改正在 build/test 的源文件**：

```zsh
python3 /path/to/head/optools/insert_ignore_fk_bench/instrument.py install "$PWD"
GOTOOLCHAIN=go1.27.1 GOMAXPROCS=2 make build BIN_NAME=CLAUDE_mo_fkprofile GOBUILD_OPT=-p=1
# 使用当前仓库的 launch 配置副本，指向全新隔离 data directory/端口。
# 显式启用 localhost debug HTTP；不要使用现有业务服务/数据库。
GOMAXPROCS=2 GOGC=50 ./CLAUDE_mo_fkprofile -debug-http 127.0.0.1:43651 -launch CLAUDE_fkbench_launch.toml
```

独立虚拟环境：

```zsh
python3 -m venv /tmp/CLAUDE_fkbench_venv
/tmp/CLAUDE_fkbench_venv/bin/pip install PyMySQL==1.1.2
MO_PASSWORD=111 /tmp/CLAUDE_fkbench_venv/bin/python /path/to/head/optools/insert_ignore_fk_bench/benchmark.py \
  --port 43650 --probe http://127.0.0.1:43651 \
  --database claude_fkbench_head_unique --label head --revision "$(git rev-parse HEAD)" \
  --storage persisted --repeats 3 --contention > CLAUDE_fkbench_head.jsonl
# 在 source worktree 运行，命令中 benchmark.py 使用本目录的绝对路径。
# 另用唯一 database 测 --children 64 --parents 4096 --modes load local。
```

`MO_PASSWORD` 不写入结果或命令；默认账号 dump，仅 localhost。原始 JSONL 每条即时 flush，任何 SQL/oracle/barrier/cleanup 失败均非零。运行虚拟环境中的 `python -m unittest discover -s optools/insert_ignore_fk_bench -p 'test_*.py'` 验证插桩 roundtrip、重复安装、锚点失败不部分修改、WIP 保护及 SQL fixture/flush ACK/最终 ID oracle。

结束先停止本次 owned 服务，再执行：

```zsh
python3 /path/to/head/optools/insert_ignore_fk_bench/instrument.py remove "$PWD"
```

保留原始 JSONL、manifest 副本、服务/driver 日志和命令/二进制哈希。latency 只报告原始样本及 min/median/max；少量样本不声称 tail percentile、长期稳定性、多 CN 或旧 binary upgrade 资格。具体接受限制与尚未通过的 gate 由版本化设计/测量文档记录，而不是本脚本自行批准。
