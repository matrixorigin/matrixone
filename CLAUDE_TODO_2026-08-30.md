# PR #27841 冲突与第四轮 review 修复

1. fetch 并 merge 最新 `mo/main`，按 source ownership 解决冲突，先恢复可编译 head。
2. 撤回 REGEXP_SUBSTR/REGEXP_REPLACE 的 checker/result-domain 扩张，完整 REGEXP runtime 留给 #27217。
3. 修正 LPAD/RPAD binary literal 的多字节安全 byte bound。
4. 引入 MPool-owned direct varlena writer；QUOTE、REPEAT、REPLACE、INSERT、LPAD/RPAD 精确 sizing 后直接写最终 area。
5. INSERT 按实际 position/removal、PAD 按实际 invalid UTF-8 copy/encode 路径计算结果，避免拒绝合法结果。
6. 补逐函数 low-cap、大输入小输出、多字节/invalid UTF-8 typed tests，跑 owning packages/BVT/self-review，commit 并 push。

# PR #27841 第五轮 review 与 CI 修复

1. merge 语义相关的最新 `mo/main@f3a1dc5256`，确认冲突解决后保留 PAD_CHAR_TO_FULL_LENGTH 语义。
2. 修复空 pad 除零、普通文本 REPLACE/INSERT 大结果误 NULL、常量 PAD binary 类型过度提升。
3. 将 direct varlena writer 的 descriptor 内存 admission 前移到 writer 回调之前，并改用 `moerr` 满足 SCA。
4. 补 focused UT，覆盖 panic、text/binary domain 边界及 writer 失败前不发布/不越过 mpool 限额。
5. 补 prepared statement 与 binary protocol metadata 公共路径证据；修正 CHAR、system view、DESC 的 BVT 期望。
6. 跑 focused/owning package、SCA、目标 BVT 与 self-review，检查 diff 后 commit、push，并处理可 resolve 的 review threads。

# PR #27841 第六轮 review 修复

1. 恢复 QUOTE 对 `IgnoreAllRow` 与 partial select-list mask 的短路语义，masked row 只发布 NULL，不读取或分配 payload。
2. 修正 derived/convert text 返回域使用字符宽度，binary 返回域才使用 encoded byte width。
3. 恢复已批准的动态 text VARCHAR metadata 契约；运行时容量检查按输入 text/binary 能力处理，避免大 text REPLACE/INSERT 误 NULL。
4. 补 QUOTE partial/all mask、VARCHAR(20000) derived 函数、CONVERT utf8mb4、text runtime 大结果的 focused UT。
5. 跑 owning packages、protocol/CTAS controls 与 self-review，commit 并 push。

# PR #27841 第七轮 review 与 CI 修复

1. 以核心 lossless invariant 为准，将未知/可超限的动态 text 扩张结果声明为 TEXT，使 metadata/CTAS 容纳合法 runtime value。
2. 更新设计审批记录，明确本轮 user correction 废止动态 text 固定 VARCHAR 的实现偏差。
3. 删除 exact-head SCA 报告的三个未使用 helper。
4. 按 Proxy BVT 实际输出修正 system view VARCHAR(6) 与 DESC 末尾空列分隔符。
5. 更新相关 UT，跑 focused/owning package、SCA 静态检查与 self-review，commit 并 push。

# PR #27841 第八轮 review 与 CI 修复

1. 为 REPEAT/PAD/REPLACE/INSERT 建立可证明 bound 的返回类型：类型级保守 bound + binder literal refinement，避免小表达式无条件 TEXT/BLOB。
2. REGEXP_REPLACE 保留 TEXT 输入，按 source/replacement 推导可容纳扩张结果的静态域；其余 regexp 函数不扩张范围。
3. 补普通/binary 小表达式、REGEXP_REPLACE 4-byte 与 80,000-byte TEXT、prepared/CTAS metadata focused tests。
4. 拉取并分类 exact-head SCA、Proxy/Pessimistic BVT、Coverage 失败，修复所有 PR-caused golden/check 问题。
5. 跑 owning package、targeted golangci-lint、相关 BVT/self-review，commit 并 push。

# PR #27841 第九轮 review 修复

1. runtime 容量以 authoritative result OID 为准，TEXT/BLOB 均允许 MaxBlobLen，补 VARCHAR source + TEXT result 大值 UT。
2. 按既有 ownership 决议撤回 REGEXP_REPLACE checker/runtime-domain 扩张，match-memory Q3 与 direct writer 留在 #27217。
3. 删除随 REGEXP 扩张新增的 protocol/CTAS/runtime 测试与专用 checker，只保留本 PR 有 ownership 的普通 REPLACE/INSERT bounds。
4. 按 exact-head Proxy 输出恢复两个 DESC golden 的尾部空 Extra/Comment 分隔符。
5. 跑 focused/owning packages、targeted golangci-lint 与 self-review，commit 并 push。
