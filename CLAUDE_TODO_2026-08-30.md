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

# PR #27841 第十轮 review 与 CI 修复

1. 让 `+` 的 string concat rewrite 与 REVERSE/LEFT/RIGHT/TRIM 等 consumers 接受并保留 CHAR/VARCHAR/TEXT domain。
2. VARCHAR text admission 按字符宽度折算 UTF-8 最大 bytes；TEXT/BLOB 继续以 MaxBlobLen 为上限。
3. REGEXP_REPLACE 仅修正现有 VARCHAR operand 的 result bound，不扩大 TEXT operand/runtime ownership。
4. Binary INSERT 使用 raw byte range，避免 invalid UTF-8 被 RuneError 重编码并突破静态 bound。
5. 修正 LTRIM/RTRIM bounded metadata、7 个 exact-head BVT golden，补 focused/consumer tests 后跑 owning packages/lint/self-review 并 push。

# PR #27841 第十一轮 review 修复

1. 将 consumer domain preservation 收窄到 CHAR/VARCHAR/TEXT；binary inputs 继续走既有 overload cast，避免 rune kernel 扩张非法 UTF-8。
2. 为 REGEXP_REPLACE 恢复零宽匹配专用 checked bound：`source + (source + 1) * replacement`。
3. 回退 binary INSERT byte-position 改动及对应测试，保持 #27216 ownership 与当前设计范围。
4. 按 review 修正两处 DESC 空 Comment 分隔符，运行 focused/owning tests、lint 后 commit/push。

# PR #27841 第十二轮 review 修复

1. string-domain matcher 先选择精确 overload，再按普通 matcher 的 minimum cast cost 选择候选。
2. binary INSERT 静态 bound 纳入 invalid UTF-8 最坏三倍 source 重编码膨胀，但不改变 rune-position runtime。
3. 补齐 DESC 最后空 Comment payload；按 exact-head 结果修正 REPEAT length 的五行 golden。
4. 添加精确 BLOB overload 与 INSERT bound focused UT，运行 owning packages/lint 后 commit/push。

# PR #27841 第十三轮 review 修复

1. LOWER/UPPER 使用 collated-text matcher，保留大型 TEXT 输入及返回域。
2. binary-charset CHAR/VARCHAR 的非 literal byte/rune bound 按 UTF-8 每字符最多 4 bytes 推导；原生 binary 类型仍按 byte width。
3. REPLACE/INSERT/LPAD/RPAD 的结果 domain 合并所有实际写入 payload 的 source/replacement/pad 参数。
4. 补 focused expression/CTAS/runtime 类型测试，运行 owning packages/lint 后 commit/push。

# PR #27841 第十四轮 review 修复

1. 为 LOWER/UPPER 增加独立返回类型推导：CHAR/VARCHAR 转为同字符宽度 VARCHAR，TEXT 不再原样返回。
2. 有界 TEXT 按 Unicode case mapping 与 invalid UTF-8 重编码的最坏 3x byte bound 扩容；无界 TEXT 保持无界。
3. 补 TINYTEXT(255)、CHAR(4) metadata 与 254→381 bytes runtime focused UT。
4. 运行 owning packages、targeted lint、diff check 后 commit/push。

# PR #27841 第十五轮 review 修复

1. 禁止 LOWER/UPPER 生成任意非标准 TEXT width，也禁止将理论 bound 饱和到 LONGTEXT marker。
2. TINYTEXT/其他 ≤VARCHAR 上限的有界 TEXT 按不变字符数返回 VARCHAR(width)，由 VARCHAR 字符契约容纳 Unicode byte expansion。
3. MEDIUMTEXT/LONGTEXT 等无法以标准 bounded VARCHAR 表达的输入退化为 TEXT(0)。
4. 更新 metadata/runtime focused UT，运行 owning packages/lint/diff check 后 commit/push。

# PR #27841 第十六轮 review 修复

1. LOWER/UPPER 仅将真实 `MaxTinyTextLen` marker 映射为 VARCHAR(255)。
2. 任意 legacy TEXT positive width 与 width 0、MEDIUMTEXT、LONGTEXT 一样退化为 TEXT(0)。
3. 添加 TEXT(32) regression UT，运行 owning packages/lint/diff check 后 commit/push。
