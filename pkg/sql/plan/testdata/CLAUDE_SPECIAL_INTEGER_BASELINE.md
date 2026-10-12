# 特殊整数消费者：独立旧版 catalog fixture

- **生成源代码：** `matrixorigin/matrixone@b1b68925d7f6fb32153e788b5285f424222e85ca`，即本 PR 尚未合入的 mo/main（包含 v97 decimal division 语义及 DUMP/LOAD 校验逻辑）。不能使用原始 `3f0a68bd80`：彼时 DIV 类型契约仍是 v96，不能与升级后的 DUMP/LOAD 仅比较本 PR 变动。
- **生成流程：** 在独立 `gwt-add issue-28981-baseline` worktree，按 Go 1.26.4 和 `make cgo` 正规构建，经仓库 `mo-cgo-test` 执行临时导出 UT。UT 在基线版本以 `NewMockCompilerContext(false)`、`compiler.tables["t"] = &planpb.TableDef{Name: "t"}`、默认 `div_precision_increment=4`，对下列每个调用构建 `CREATE TABLE t(a DECIMAL(10,2), b DECIMAL(10,2), g VARCHAR(100) GENERATED ALWAYS AS (<call>) STORED, CHECK (<call> IS NOT NULL))`；直接 `proto.Marshal` 其 TableDef，JSON `map[string][]byte` 将二进制 protobuf 编码为 base64。导出程序不是在 PR 版本重新绑定获得的“旧树”。
- **原始调用：** `format(a/b,2)`、`format(a,b/2)`、`format(a,b/2,'en_US')`、`makedate(a/b,1)`、`makedate(2024,a/2)`、`cast(maketime(a/2,1,1.25) as varchar)`、`cast(maketime(1,a/2,1.25) as varchar)`、`cast(maketime(a/2,1,1.25) as varchar)`。名称与测试 `TestTableDumpExactBaseSpecialIntegerBindings` 一致。
- **fixture：** `CLAUDE_special_integer_baseline.json`；生成文件 SHA-256：`a624286d794db4371b0a43af016d3c989a6e7420ccd5e0a4b51659688956cfd1`。
- **使用：** head UT 只做 JSON decode 和 protobuf unmarshal；将旧 TableDef 分别当 source/target，与从同一 SQL 构建的新 target 校验，并构造 forged bound tree 负例。不要用 head 的历史模式合成这个 fixture。
