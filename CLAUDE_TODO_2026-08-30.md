# PR #27841 冲突与第四轮 review 修复

1. fetch 并 merge 最新 `mo/main`，按 source ownership 解决冲突，先恢复可编译 head。
2. 撤回 REGEXP_SUBSTR/REGEXP_REPLACE 的 checker/result-domain 扩张，完整 REGEXP runtime 留给 #27217。
3. 修正 LPAD/RPAD binary literal 的多字节安全 byte bound。
4. 引入 MPool-owned direct varlena writer；QUOTE、REPEAT、REPLACE、INSERT、LPAD/RPAD 精确 sizing 后直接写最终 area。
5. INSERT 按实际 position/removal、PAD 按实际 invalid UTF-8 copy/encode 路径计算结果，避免拒绝合法结果。
6. 补逐函数 low-cap、大输入小输出、多字节/invalid UTF-8 typed tests，跑 owning packages/BVT/self-review，commit 并 push。
