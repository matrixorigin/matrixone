# Lock Retry 问题修复总结

## 1. 背景

本次问题的核心现象是：在 `lockWithRetry` 遇到可重试错误时，即使调用方 `ctx` 已经超时或取消，底层仍可能继续按固定间隔重试，导致请求从外部看已经“应该结束”，但内部仍在继续进行 lock retry。

这个问题在涉及 `mo_increment_columns` 的多 CN 场景下尤其容易放大，因为自增列路径会依赖远端锁和元数据访问，一旦远端返回被归类为“可重试”的错误，而调用方上下文又已经结束，就会出现“不该继续但仍在 retry”的行为。

---

## 2. 关键结论

本次修复采用了**最小影响范围**的方案：

- 不修改 `lockservice` 更深层的远端错误清洗逻辑
- 只在 `pkg/sql/colexec/lockop/lock_op.go` 的 retry 边界修复
- 让 `lockWithRetry` 在 `ctx` 已结束时停止重试
- 保证成功结果和非重试错误不会被后续的 `ctx.Err()` 误覆盖

最终结论是：

- **问题已经在 `lockop` 边界被有效修复**
- **修复范围小，风险低**
- **回归测试已覆盖关键语义**

---

## 3. 根因分析

### 3.1 原始行为

旧逻辑中，`lockWithRetry` 在遇到部分错误时会继续重试，而 `canRetryLock` 主要依据错误类型决定是否重试。

问题在于：

- 原实现没有在 retry 前优先检查 `ctx.Err()`
- retry 等待使用 `time.Sleep(defaultWaitTimeOnRetryLock)`，无法被 `ctx.Done()` 打断
- 某些底层超时/连接错误会在上层表现为“仍可重试”的错误类型

因此在外部调用已经超时后，内部仍可能继续：

1. 判断错误可重试
2. 固定 sleep
3. 再次发起 lock 请求

这就是“请求已结束但内部仍在 retry”的根源。

### 3.2 为什么会在 incrservice 场景下稳定暴露

本次定位确认，普通用户表的常规行锁冲突并不是最稳定的正常代码复现方式。  
真正更稳定的路径来自 `incrservice` 对 `mo_increment_columns` 的访问：

- `doAllocate` 使用 **3 分钟**超时
- `doUpdate` 使用 **10 秒**超时

相关路径包括：

- `pkg/incrservice/allocator.go`
- `pkg/incrservice/column_cache.go`
- `pkg/incrservice/store_sql.go`

其中：

- 普通自增插入会走 `doAllocate`
- 显式写入较大的自增值会走 `updateTo()` -> `updateMinValue()` -> `doUpdate`

---

## 4. 正常代码复现方式

本次最终确认的纯 SQL 复现方式如下。

### 4.1 准备表

```sql
drop database if exists lock_retry_regress;
create database lock_retry_regress;
use lock_retry_regress;

create table ai_t (
    id bigint unsigned not null auto_increment,
    v int,
    primary key (id)
);
```

### 4.2 在一个 CN 上持住 `mo_increment_columns` 的目标行锁

```sql
use lock_retry_regress;
begin;

select offset, step
from mo_catalog.mo_increment_columns
where table_id = (
    select rel_id
    from mo_catalog.mo_tables
    where reldatabase = 'lock_retry_regress'
      and relname = 'ai_t'
)
and col_name = 'id'
for update;
```

### 4.3 在另一个 CN 上触发超时路径

#### 触发 `doUpdate` 路径（约 10 秒）

```sql
use lock_retry_regress;
insert into ai_t(id, v) values (1000000000000, 1);
```

#### 触发 `doAllocate` 路径（约 3 分钟）

```sql
use lock_retry_regress;
insert into ai_t(v) values (1);
```

### 4.4 释放锁

```sql
rollback;
```

### 4.5 预期现象

修复前，CN1 上的 SQL 可能在超时后仍持续 retry，不及时退出。  
修复后，应在对应上下文超时后返回，而不是无限继续重试。

---

## 5. 修复方案

本次正式修复点位于：

- `pkg/sql/colexec/lockop/lock_op.go`

核心改动有三类。

### 5.1 在 retry 判断前先看 `ctx`

当前 `canRetryLock` 会先判断：

- `ctx.Err() != nil`
- 当前错误是否属于 retryable lock error

也就是说，只要上下文已经结束，就不会再进入下一轮重试。

### 5.2 将不可打断等待改为可打断等待

原来的固定 `time.Sleep` 被替换为基于 `timer + select` 的等待：

- 等到 retry 间隔到期时才继续
- 如果等待期间 `ctx.Done()` 先到，则立即退出

这保证了“等待阶段”本身也会尊重上下文结束。

### 5.3 保持退出语义正确

修复同时确保：

- 如果最终是因为 `ctx` 结束而停止 retry，返回 `ctx.Err()`
- 如果是成功结果，不能因为随后 `ctx` 被取消而误报错
- 如果是非重试错误，也不能被 `ctx.Err()` 覆盖

这避免了“为了 stop retry 而破坏返回语义”的副作用。

---

## 6. 为什么选择修在 `lockop` 边界

本次没有进一步修改 `lockservice` 的远端错误清洗或孤儿事务判断逻辑，主要基于以下考虑：

- 当前问题的直接根因在于 `lockop` 的 retry 边界没有尊重 `ctx`
- 在 `lockop` 修复可以直接阻断“无意义继续 retry”
- 修改范围小，blast radius 更可控
- 避免把本次问题扩展成对更深层远端错误分类语义的大范围调整

换句话说，这次修复优先保证：

- 先把“错误的继续重试”修掉
- 再把范围控制在最小必要集

---

## 7. 测试与验证

本次新增并验证了以下关键回归测试：

1. `ctx` 在首次进入 retry 判断前已经 `deadline exceeded`
2. `ctx` 在首次进入 retry 判断前已经 `canceled`
3. 第一次返回可重试错误后，在 `waitToRetryLock` 等待期间 `ctx` 被取消，不会发起第二次 `Lock`
4. 成功结果不会被后续的 `ctx` 取消覆盖
5. 非重试错误不会被后续的 `ctx` 取消覆盖

验证命令包括：

```bash
go test ./pkg/sql/colexec/lockop -run 'TestLockWithRetry' -count=1
make err-check
```

---

## 8. 风险评估

### 8.1 主修复风险

风险整体较低，原因是：

- 修改只集中在 `lockop`
- 不涉及更深层 `lockservice` 协议语义调整
- 行为变化只发生在“调用方上下文已经结束，但旧逻辑还想继续 retry”的路径上

### 8.2 后续补充优化风险

后续又补了两点低风险优化：

1. 新增“等待期间取消”的测试
2. 将 retryable error 分类收敛到一份逻辑，避免后续双份维护漂移

这两点都属于低风险增强：

- 前者只补测试
- 后者是等价重构，不改变对外策略

---

## 9. 当前结论

截至当前工作树，本次问题可以认为已经完成闭环：

- 找到了稳定的正常代码复现路径
- 明确了真正的根因和修复边界
- 采用小范围方案修复了 `lockWithRetry` 的退出问题
- 增补了关键回归测试
- 风险可控，语义清晰

本次修复的核心价值可以概括为：

- **该停的时候能停**
- **等待过程可中断**
- **返回结果不被误覆盖**
- **修复范围尽量小**

---

## 10. 后续建议

当前没有必须立即处理的 blocker。

如果后续还想继续优化，可以考虑单独评估：

- 对长期 backend 不可用场景引入 retry budget 或指数退避

但这已经属于性能/策略优化，而不是本次 bug 修复的必要部分。
