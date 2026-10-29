# IVF 检索参数

IVFFLAT 的 `lists` 是建索引时的聚类数，探测数决定搜索访问多少聚类。
增加探测通常增加工作量并改善召回；有限探测的结果即使满页也可能遗漏真实近邻。
`lists=1` 会增加搜索工作量；不能把某个数据集的结果推广为所有量化方式和数据的精确召回保证。
用同样的过滤条件和 `mode=force` 全表距离排序结果作为召回率对照。

`probe_limit` 的默认值为 5，范围为 1–80000。`SET SESSION probe_limit = 4` 只改变当前连接；
请在同一连接用 `SELECT @@session.probe_limit` 读回，连接池中其他连接保持自己的值。
查询级 `nprobe` 范围相同，优先于当前会话值，不会修改会话：

```sql
SELECT id FROM t ORDER BY l2_distance(v, '[0,0]') LIMIT 5
BY RANK WITH OPTION 'mode=post', 'nprobe=10';
SELECT @@session.probe_limit;
```

`nprobe` 可以不指定 `mode`。IVFFLAT AUTO 可根据统计信息在这个基础上扩大探测，
因此它不是 AUTO 的硬上限；`force` 不使用索引探测。POST 在有限候选中过滤，可能短页。
AUTO 按该版本的策略选择执行方式；满页不保证精确召回，短页行为取决于查询形状和策略。
未知参数、非字符串选项和非法 `nprobe` 会报错。

IVFPQ 是 GPU 索引，需要支持 CUDA 的构建和兼容设备；CPU 构建不提供 GPU 执行能力。
