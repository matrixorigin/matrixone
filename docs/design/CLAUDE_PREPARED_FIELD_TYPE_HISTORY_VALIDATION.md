# FIELD 历史：第二次授权后的实施与验证

> 最新验收见第7节：固定上游79298b10e2合并至e89a5a65edf396d85ec5aa2e4ae6d1ff9e69b165后，Go验证、公共SQL、计数、性能及所需race已完成；另精确同步四个上游BOOL预期metadata字段。1–6节保留此前4658阶段证据，未覆盖历史失败。本记录不代替独立reviewer批准；冻结的D2/D3不回写。

## 1. 版本、决定与可观察行为

- 设计输入：[D2](CLAUDE_PREPARED_FIELD_TYPE_HISTORY_DESIGN_D2.md)、[D3](CLAUDE_PREPARED_FIELD_TYPE_HISTORY_DESIGN_D3.md)，发布版本`f2b418d1bfe604a1825419b3abf6affe0f53d09b`。
- 后续用户第二次`go ahead`选择R1-A及R2优化；来源和范围见[DECISIONS](CLAUDE_PREPARED_FIELD_TYPE_HISTORY_DECISIONS.md)，不将第一次验证授权倒签为已接受后来发现的问题。
- 实施：`76ecbfcdbf`；合并上游`309f618995af4ee23e3b9149365f4af77264090d`后的验证代码：`4658ff3eee033c9f1285f37a63ecb23a91cf289d`。
- 相对该上游完整PR为39个文件、15个production Go文件；本轮实施10文件、593行新增/2行删除（包含设计决定及测试），不是仅审查单行clear。
- 新的可见边界：带H的source-bound retry如果捕获的catalog对象/版本/数据库/发布身份改变，返回既有definition-change错误。`RewriteError`映射为**20631 / HY000**，并非MySQL1615；不假定驱动自动reprepare。版本变化但列类型未变的DDL也保守拒绝。
- 无H路径不增加该验证；未变化依赖保留原H。重试不发布新H，错误终端使旧generation失效；下一正常EXECUTE由原owner重建并发布。没有第二个handle发布者。

## 2. 永久回归的独立判断

| 对象 | 永久测试与判断 |
|---|---|
| 声明代际 | `pkg/frontend/prepared_field_retry_test.go::TestPreparedFieldRetryDeclarationGeneration`：11格；版本/对象/数据库/发布身份、缺失、解析错误、修改原事实、嵌套Snapshot/PubInfo复制、nil依赖、无H控制；ctx恢复、旧H身份及错误映射独立断言 |
| 后续EXECUTE | 上述version格经过真实session compiler的PREPARE seed重建及普通initExecute路径，新H/新schema与旧capture分离，实际结果从TEXT语义1变为VARBINARY语义2，结果metadata仍相同 |
| 冻结与错误终端 | `pkg/sql/compile/compile_retry_lifecycle_test.go::TestDefinitionRetryBuildFailureInvalidatesGeneration`：metadata冻结前/后分别调用真实retry构建入口，callback失败不发布plan，旧generation仍被标记重建 |
| clear与缓存对照 | 同文件`TestCompileClearReleasesBuildPlanClosure`：cached prepared Release仍可调用捕获；私有Compile走真实clear后callback必须nil；无Put后访问或GC/pool指针巧合 |
| 普通TEXT证明 | `pkg/sql/plan/prepared_field_test.go::TestPlainTextStringSourceProof`：32层透传零分配；明确叶、NULL/参数/变量/cast/来源witness/无builder/prepared等控制；TABLE_SCAN投影CAST3反例在增加guard前红、之后绿；别名退回完整resolver |
| DEFAULT消费者 | `pkg/sql/plan/expression_default_test.go::TestExpressionDefaultNullIfStringSource`：原始Expr先与DeepCopyPlan完整比较，再wire roundtrip/copy/Eval八格；保留全Type/Expr及执行后不变性，不用相同丢字段的两个副本相互证明 |

R1 frontend判断与Compile冻结判断是分层确定性证据；没有把它们冒充“真实服务正在Run时并发DDL”的端到端注入测试。客户端普通DDL对照见第5节。

## 3. owning与静态检查

环境：darwin/arm64 Apple M1 Pro，Go1.27.1，匹配gofmt，`GOWORK=off GOMAXPROCS=2 GOFLAGS=-p=1 GOMEMLIMIT=2GiB`；使用`mo-cgo-test`验证当前worktree native来源。下表计数包含subtest。

| package | PASS / 既有skip | 终端秒数 |
|---|---:|---:|
| pkg/sql/plan | 17767 / 2 | 16.958 |
| pkg/frontend | 6161 / 1 | 21.618 |
| pkg/sql/compile | 3679 / 0 | 13.169 |
| pkg/sql/plan/function | 11387 / 0 | 15.225 |
| pkg/sql/parsers/tree | 149 / 0 | 0.838 |

命令：

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test \
  ./pkg/sql/plan ./pkg/frontend ./pkg/sql/compile \
  ./pkg/sql/plan/function ./pkg/sql/parsers/tree \
  -count=1 -timeout=900s -covermode=atomic \
  -coverprofile=CLAUDE_owning.cover -json
```

诚实保留首轮失败：frontend的既有`TestConnMeasuresOnlyPhysicalOutputWrite`有一次instant fake Write计时0ns，违反Positive断言。相关生产/fixture与固定上游相同；精确100次诊断当前头100通过，上游完整PR-Go闭包overlay为97通过/3次相同失败。未改计时、断言或skip。仅frontend全包按相同参数完整重跑一次通过；其余4包沿用同源PASS。组合profile**丢弃了失败frontend的全部覆盖计数**，不能将原5包命令exit=1改写为0。

相对固定上游的完整PR变更可执行行覆盖：**977/1051 = 93.0%**。这是通过包组合的同源atomic profile；不是旧头47/47或仅本轮增量。包内默认coverage未对所有跨包consumer统一instrument，不将其宣称为所有调用路径覆盖。

同五包`go vet`通过；`golangci-lint --new-from-rev=309f618...`为`0 issues`。molint命令完成但输出22条既有recover/unsafe诊断，不能称零诊断；每条告警的源片段与固定上游相同，唯一非整文件等价的routine.go也核对了完整告警所属函数。未冒称执行了baseline molint。

## 4. R2三方配对benchmark

永久输入：`pkg/sql/plan/string_source_bench_test.go::BenchmarkStringSourceDeclarationBinding`。预解析同一SQL AST，然后真实BuildPlan；不计SQL parsing，不混用cache hit、单行执行或服务吞吐指标。

```sql
select reverse(...reverse(n_name)...) from nation
-- 嵌套深度1、8、32；fixture中n_name为VARCHAR
```

三方同一toolchain/native/SQL/fixture，`-benchmem -benchtime=200ms -count=5`：

1. **B**：固定上游`309f618...`，overlay恢复全部PR变更Go文件，显式保留同一benchmark。不能只替换base_binder.go或让新增benchmark被baseline置空。
2. **pre-opt**：同一合并源，只移除`bindStringSourceDeclaration`中调用`stringSourceIsPlainText`后continue的快路；保留既有完整resolver，模拟同集成条件下优化前的热路径，不伪称旧D3提交原样运行。
3. **final**：验证代码`4658ff3...`，包括QueryBuilder/PREPARE和scan ProjectList guard。

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/sql/plan -run '^$' \
  -bench '^BenchmarkStringSourceDeclarationBinding$' \
  -benchmem -benchtime=200ms -count=5
# B / pre-opt使用相应 -overlay；没有覆盖率或计数instrumentation。
```

| 深度 | B ns/op中位数 | pre-opt | final | B→pre-opt→final B/op | allocs/op |
|---:|---:|---:|---:|---|---|
| 1 | 28667 | 28756 | 28041 | 27176→27752→27176 | 333→338→333 |
| 8 | 52023 | 61037 | 53715 | 37360→41968→37360 | 519→559→519 |
| 32 | 205282 | 320601 | 214694 | 96137–96138→160011→96137–96138 | 1209→1485→1209 |

32层额外约63.9KB/276 allocations已消除，当前中位数相对pre-opt下降约33%；但相对B仍约**+4.6%**。共享机器未CPU隔离，不声称时间完全相等或全工作负载零回归。窄证明仍会重复沿透传链检查；不把去掉完整resolver/maps等同为全语句线性。DEFAULT、PREPARE、未知projection/provenance等继续走完整解析，不以牺牲声明语义换性能。

首次绑定/类型进展/数值模式、多引用CTE和依赖增长的计数及capture成本已在后续新头补齐，见第7节；不拿历史12格或上表代替全部轴。

## 5. 支持的构建与公共SQL

- `make -j2 build-with-prebuilt-native BIN_NAME=CLAUDE_mo_review`：Go1.27.1，默认支持的SIMD路径；仅`-w`省去DWARF；native provenance verify成功。正式Go源的build/运行前后hash一致。
- 自有单CN服务，mo-tester `method: run`、`rate: 100`；dtype和history各执行两次，FIELD/variadic各一次，六次exit=0。源/预期hash保持；没有genrs。最后持久报告history **492/492**，error.txt为空；其他轮次报告被tester覆盖，不杜撰其本轮终端计数。
- `-g`按既有issue标记跳过；`-n`仅设置全局不比较metadata，dtype原有文档级`--- @metacmp(true)`覆盖该默认值，因此dtype实际严格比较metadata。其余三文件无覆盖标记，不能将其成功当作wire metadata证明；完整Type/metadata还由永久UT断言。dtype仅在本地副本去掉既有MULTICN专属中段，该阶段正式SQL/results未改。
- 独立外部MySQL3306及自有MO对同一最小DDL控制均为`1 / 1 / 2 / 2`：VARCHAR两次EXECUTE，ALTER VARBINARY后EXECUTE，再fresh直接SQL。临时database经存在性检查创建并finally drop，未停止外部MySQL。
- 这是正常两次EXECUTE间DDL控制，不是物理重试中途DDL/实际20631线协议注入。服务已terminate/wait且检查自有端口释放；data/evidence保留。

### 5.1 配对overlay的可复现构造

在由`gwt-add`创建的隔离worktree检出验证代码后，保持永久benchmark不变；以下只生成本地诊断文件，不修改生产源：

```python
from pathlib import Path
import json, subprocess
r = Path.cwd()
base = '309f618995af4ee23e3b9149365f4af77264090d'
replace = {}
for name in subprocess.check_output(
    ['git', 'diff', '--name-only', base, '--', '*.go'], text=True
).splitlines():
    if name == 'pkg/sql/plan/string_source_bench_test.go':
        continue
    old = subprocess.run(['git', 'show', base + ':' + name], capture_output=True)
    data = old.stdout if old.returncode == 0 else (
        next(x for x in (r/name).read_text().splitlines()
             if x.startswith('package ')) + '\n'
    ).encode()
    saved = r/'CLAUDE_artifacts'/'.baseline'/(name + '.txt')
    saved.parent.mkdir(parents=True, exist_ok=True)
    saved.write_bytes(data)
    replace[str(r/name)] = str(saved)
(r/'CLAUDE_B.json').write_text(json.dumps({'Replace': replace}))
p = r/'pkg/sql/plan/base_binder.go'
s = p.read_text()
fast = '\t\tif kind == fieldStringText && b.stringSourceIsPlainText(arg) {\n\t\t\tcontinue\n\t\t}\n'
assert s.count(fast) == 1
saved = r/'CLAUDE_preopt.go.txt'
saved.write_text(s.replace(fast, ''))
(r/'CLAUDE_preopt.json').write_text(json.dumps({'Replace': {str(p): str(saved)}}))
```

三次第4节命令分别附`-overlay="$PWD/CLAUDE_B.json"`、`-overlay="$PWD/CLAUDE_preopt.json"`及不附overlay。每组必须实际出现3个depth×5条benchmark记录和PASS终端，不能用“无测试选择”的exit=0冒充测量。

<details>
<summary>原始数值及package终端摘录（各命令exit=0；单位ns/op、B/op、allocs/op）</summary>

```text
B / depth 1:  28422 27176 333; 29016 27176 333; 27741 27176 333; 28667 27176 333; 28882 27176 333
B / depth 8:  52769 37360 519; 51725 37360 519; 51611 37360 519; 52023 37360 519; 52046 37360 519
B / depth 32: 206427 96138 1209; 206417 96138 1209; 205216 96137 1209; 205282 96138 1209; 205143 96137 1209
PASS
ok github.com/matrixorigin/matrixone/pkg/sql/plan 5.646s

pre-opt / depth 1:  29387 27752 338; 28605 27752 338; 28756 27752 338; 28954 27752 338; 28551 27752 338
pre-opt / depth 8:  61436 41968 559; 61037 41968 559; 60245 41968 559; 62118 41968 559; 61015 41968 559
pre-opt / depth 32: 318850 160011 1485; 320601 160011 1485; 320823 160011 1485; 321235 160011 1485; 316457 160011 1485
PASS
ok github.com/matrixorigin/matrixone/pkg/sql/plan 6.103s

final / depth 1:  28785 27176 333; 27832 27176 333; 28041 27176 333; 27873 27176 333; 28397 27176 333
final / depth 8:  52944 37360 519; 56397 37360 519; 53715 37360 519; 52775 37360 519; 53965 37360 519
final / depth 32: 213040 96137 1209; 214694 96138 1209; 216845 96137 1209; 217658 96138 1209; 213000 96138 1209
PASS
ok github.com/matrixorigin/matrixone/pkg/sql/plan 6.072s
```

</details>

## 6. 生命周期与范围限度

4658阶段精确race均完成：RetryDeclarationGeneration/CandidateHistory/HistoryCacheAndMigration的测试终端分别0.06/0.04/0.03s，clear/definition-retry测试终端均显示0s（输出精度下界，不声称零实际耗时）。按N=clamp(floor(30s/T),1,100)，终端为0时取上限100，五个精确选择各100次通过；frontend/compile完整race分别34.142s/17.092s通过。

计数overlay不改变正式源或正常性能样本。首轮numeric case由于fixture追加ParamTypes未清旧类型失败，不能把同轮type-progress标签当有效证据；后续修正为每轮完整类型数组并新增实际FIELD结果oracle。失败fixture/工具超时/基线诊断均与验收结果分开。

新增catalog捕获是O(D)个事实对象；嵌套/未知protobuf字节按实际大小复制，不是O(D)常数字节或history配额保证。cached Release仍非引用终点；true clear清callback，不声称所有内存立即归OS或证明原问题无限增长。

本PR没有新增history wire字段；之后上游#30191的ColDef.ResultColumnKind是独立BOOL结果metadata，不能作为旧FIELD行为兼容证明。D2/D3的协议和部署限度不变：没有新旧FIELD执行器混合部署语义一致性保证；无Proxy/MULTICN、旧版本重启、全仓race或性能普适性结论。未擅自关闭独立CHANGES_REQUESTED，未等待或触发post-push CI。

## 7. 最新上游合并后的验证代（D4）

代码源为`e89a5a65edf396d85ec5aa2e4ae6d1ff9e69b165`，固定上游`79298b10e2114246c6ef41bde709dbbb4a20bca4`。两次合并无冲突；本次含#30191 BOOL结果metadata及#30632空flush快照/异步CI同步。native输入未变。不能把前述4658证据直接改标成新头通过。

### 7.1 正常包、覆盖率、静态和构建

同第3节的五包原命令一次完整exit=0，不需要拼接失败profile：

| 包 | PASS（含subtest） | 原有skip | package终端秒 |
|---|---:|---:|---:|
| plan | 17922 | 2 | 19.134 |
| frontend | 6263 | 1 | 20.792 |
| compile | 3679 | 0 | 12.473 |
| function | 11387 | 0 | 10.899 |
| tree | 149 | 0 | 0.963 |

相对79298的完整PR变更可执行行仍为**977/1051=93.0%**；这里使用独立的新头atomic profile，逐文件如下（均省略`pkg/`前缀）：

| production文件 | 已覆盖/变更可执行行 |
|---|---:|
| frontend/computation_wrapper.go | 102/105 |
| frontend/prepared_execution.go | 15/15 |
| frontend/routine.go | 1/2 |
| frontend/types.go | 2/2 |
| sql/compile/compile.go | 1/1 |
| sql/parsers/tree/expr.go | 0/2 |
| sql/parsers/tree/format.go | 0/5 |
| sql/plan/base_binder.go | 240/253 |
| sql/plan/collation_expression.go | 2/2 |
| sql/plan/function/func_binary.go | 10/10 |
| sql/plan/prepared_binding.go | 14/15 |
| sql/plan/prepared_field.go | 363/378 |
| sql/plan/prepared_field_types.go | 226/260 |
| sql/plan/query_builder.go | 1/1 |
| sql/plan/types.go | 0/0（仅声明） |

仍是默认包内instrumentation，不将跨包parser consumer行为冒称全部已覆盖。`go vet`通过；golangci-lint `--new-from-rev=79298…`零issue；molint命令exit=0但仍22条诊断，owner/片段与固定新上游等价，不声称零诊断或实际跑了baseline molint。支持的prebuilt-native服务构建通过，保留默认SIMD路径；`-w`仅省略DWARF。Go源manifest贯穿测试/构建/服务使用。

### 7.2 优化次数、CTE引用和依赖capture

临时overlay只对`BaseOptimizer.Optimize`、`BuildPreparedExecutionPlanWithMetadata`入口及该入口的两处source Optimize计数。没有正式hook/cache/flag；不带计数的三方benchmark另跑。单包非parallel诊断`TestClaudeDesignRemainingCountMatrix`通过，fixture每轮完整重设二进制参数类型，并执行真实FIELD结果oracle。

`total/source/bind`分别表示全部Optimize调用、source Optimize调用及source绑定入口次数；外层PREPARE seed与source阶段不能混为一谈。

| 输入轴/阶段 | total/source/bind | 独立结果约束 |
|---|---|---|
| 首次整数→字符串类型进展→重复 | 2/2/1 → 1/1/1 → 1/1/1 | FIELD=0/1/1 |
| DECIMAL(10,2)，div_precision_increment 4→8 | 2/2/1 → 1/1/1 → 3/2/1 | NULL种子→文本→mode切换，FIELD=0/2/1；新H |
| DECIMAL(65,30)封顶，同一mode切换 | 2/2/1 → 1/1/1 → 4/3/2 | FIELD=0/2/2；保持旧H指针 |
| 同一CTE源引用1/4/8次，各首次→进展 | 各2/2/1 → 1/1/1 | 源CASE-map=1、param-map=2，保持H指针 |
| 不同表依赖D=1/8/16，各首次→未改schema的retry | 各2/2/1 → 1/1/1 | capture个数=D，不是同一表的D个alias |

CTE SQL为`WITH c AS (SELECT CASE WHEN ? THEN ? ELSE NULL END AS x)`，再从c的1/4/8个引用分别取`FIELD(ci.x,'A','a')`；这量化逻辑源/引用轴，未将其称为所有物化/递归拓扑的测试。普通类型进展使用`FIELD(CASE WHEN ? THEN ? ELSE NULL END,'1.9')`，值123→`1.9tail`；数值模式两个轴分别附对应DECIMAL除法结果，并使用NULL→`a`的FIELD历史。

| 不同表D | 首次Resolve / retry Resolve | capture ns/op | B/op | allocs/op |
|---:|---:|---:|---:|---:|
| 1 | 2 / 2 | 250 | 424 | 5 |
| 8 | 16 / 16 | 1164 | 1712 | 12 |
| 16 | 32 / 32 | 2212 | 3184 | 20 |

capture用`testing.Benchmark`和`testing.AllocsPerRun(100)`交叉计数；测的是基本ObjectRef fixture，未包含大ExtraInfo/Snapshot/PubInfo payload。此样本与O(D)复制一致，但不是字节上限；Resolve次数也不能排除原有依赖收集去重中的D²工作。不是整个statement线性复杂度或全部参数×CASE×依赖笛卡尔矩阵的证明。

### 7.3 严格metadata BVT的真实集成失败与修复

#30191已把上游dtype的计算BOOL协议头改为`[-6,8,0]`。本PR新增的两条LIKE对照在无冲突merge后仍保留四处`[-7,1,0]`。先运行未改预期，得到**91/93、两项失败**；两组数据值逐项相同，差别仅四个协议metadata字段。保留失败exit/report，不把它改写成通过。

只精确同步这四个字段（正式result两行）到上游既有契约，未改SQL、值、`@metacmp(true)`或生产Go源；没有genrs、没有关闭metadata比较。`RSSet.shouldCompareMeta`优先级是SQL标记>文档标记>全局；因此前文关于`-n`的过宽描述已纠正。这里不是放宽语义期待来获得绿色。

使用同一新头已验证二进制，在独立D4B数据目录重新启动单CN；逐轮保留report，不覆盖取数证据：dtype **93/93两次**（严格metadata），history **492/492两次**、FIELD **97/97**、variadic **104/104**。六次`method:run/rate:100`均exit=0，所有FAILED/IGNORED/ABNORAML均0、error.txt为空。MySQL与MO普通DDL控制均**1/1/2/2**；自有服务已停止、端口释放，两个数据目录及失败证据保留。仍不将普通DDL当作真实并发物理retry或wire20631注入证明。

### 7.4 新头三方性能与race终端

三方输入、命令和overlay构造同第4节，只把B的固定SHA改为79298、最终源改为e89a5a65；仍保留同一永久benchmark并恢复全部PR Go闭包，不带counter/coverage。三组各depth×5样本，全部exit=0：

| 深度 | B中位数ns/op | pre-opt | final | B→pre-opt→final B/op | allocs/op |
|---:|---:|---:|---:|---|---|
| 1 | 28614 | 28766 | 28350 | 27176→27752→27176 | 333→338→333 |
| 8 | 53817 | 61467 | 53660 | 37360→41968→37360 | 519→559→519 |
| 32 | 206127 | 319042 | 214939 | 96137–96138→160011→96137–96138 | 1209→1485→1209 |

32层消除额外约63.9KB/276次分配；时间中位数比pre-opt降低约32.6%，但比B仍约**+4.3%**。这与前一代+4.6%样本一起保留，不挑选较好的样本宣称时间完全相等；剩余只读链检查仍有CPU工作。没有新增缓存或扩大证明边界来追逐数字，也未宣称全场景性能预算已由独立reviewer批准。

<details>
<summary>新头原始数值及package终端摘录（ns/op、B/op、allocs/op）</summary>

```text
B / depth 1:  28614 27176 333; 28164 27176 333; 28577 27176 333; 29102 27176 333; 28868 27176 333
B / depth 8:  53817 37360 519; 53559 37360 519; 53987 37360 519; 54067 37360 519; 52306 37360 519
B / depth 32: 207173 96138 1209; 208221 96138 1209; 206127 96138 1209; 204940 96137 1209; 204861 96137 1209
PASS
ok github.com/matrixorigin/matrixone/pkg/sql/plan 5.561s

pre-opt / depth 1:  29109 27752 338; 28773 27752 338; 28584 27752 338; 28766 27752 338; 28517 27752 338
pre-opt / depth 8:  60017 41968 559; 61467 41968 559; 62909 41968 559; 61554 41968 559; 61198 41968 559
pre-opt / depth 32: 322744 160011 1485; 312984 160011 1485; 317456 160011 1485; 319042 160011 1485; 319489 160011 1485
PASS
ok github.com/matrixorigin/matrixone/pkg/sql/plan 5.771s

final / depth 1:  28478 27176 333; 28494 27176 333; 28163 27176 333; 28026 27176 333; 28350 27176 333
final / depth 8:  53660 37360 519; 53931 37360 519; 53929 37360 519; 53042 37360 519; 52885 37360 519
final / depth 32: 215029 96137 1209; 214939 96137 1209; 214536 96138 1209; 216603 96138 1209; 214445 96138 1209
PASS
ok github.com/matrixorigin/matrixone/pkg/sql/plan 5.494s
```

</details>

新头race全部完成；不是沿用4658的终端。精确测试首先`-race -count=1 -timeout=120s -json`计时，然后按N=clamp(floor(30s/T),1,100)压力重复，输出0s时取上限100：

| 精确选择 | 单次测试终端秒 | stress根测试PASS事件数 |
|---|---:|---:|
| frontend/TestPreparedFieldRetryDeclarationGeneration | 0.05 | 100 |
| frontend/TestPreparedFieldCandidateHistory | 0.04 | 100 |
| frontend/TestPreparedFieldHistoryCacheAndMigration | 0.03 | 100 |
| compile/TestCompileClearReleasesBuildPlanClosure | 0（显示精度） | 100 |
| compile/TestDefinitionRetryBuildFailureInvalidatesGeneration | 0（显示精度） | 100 |

五个测量与五个压力命令均exit=0，选择数逐一检查，不能用package PASS掩盖零选择。随后同wrapper执行：

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test \
  ./pkg/frontend ./pkg/sql/compile -race -count=1 -timeout=900s -json
```

两个完整owning包分别PASS，终端**28.064s / 16.777s**，命令exit=0。没有扩展成全仓race或C/native线程race证明。

### 7.5 交付核对与证据位置

- 完整PR相对固定上游为**40文件、15个production Go文件、1952行非生成生产增删**（包含新验证文档）；不是只审查91行本轮生产增量。完整图覆盖保留H、发布/失败/失效、cache、retry、迁移、普通声明consumer、DEFAULT及真实pool终点。
- D1/D2/D3 SHA256、正式Go source manifest、精确race选择数、五包profile来源、所有BVT独立report和MySQL/MO控制输出均经最终脚本复核。匹配Go1.27.1的gofmt检查和`git diff --check`通过。
- 本地完整原始日志/exit/命令/overlay/profile保留在当前worktree的`CLAUDE_D4_*`、`CLAUDE_D4B_*`及`CLAUDE_validation_artifacts/`；不将临时probe、TODO、服务数据或cache提交。本文提供固定版本、复现构造、样本与终端摘录；不声称这些作者证据已被reviewer独立重跑。
- 本地自审无未关闭的实现/验证阻塞项；独立GitHub设计/实现review仍由reviewer决定。未dismiss CHANGES_REQUESTED，不等待/触发post-push CI。第6节部署、并发DDL和拓扑限度继续适用。

## 8. Review20：CASE/IF 的强 TEXT 声明与公共验证

本节验证的是本次提交的源码，工作区基座为 `5a1cb56e762054dfede52d5d0c1e866bdbcf30c9`，合入的固定 `mo/main` 为 `743e749a1e33d6c845bb73999e8c82d297a2b657`。此前第7节的 race、benchmark 和计数结果保留为历史证据，不能改标为本轮新测量。完整 PR 仍为 **40 文件、15 个非生成 production Go 文件、2027 行生产增删**；本轮生产修改仅在 `base_binder.go`，没有改变 history/retry/cache/migration 的状态格式或生命周期。

### 8.1 契约纠正与实现边界

历史 DEFAULT 测试把 `VARCHAR txt` 与弱 `_binary ''` 配对命名为 “genuine CASE owner”，并期待逐字节反转。这一结论错误；旧 PR 描述将 genuine-CASE 差异一概排除也不成立。本轮纠正该结论，而非把 CASE/IF 一律当透明节点。冻结 D1/D2/D3 的字节和历史状态不改。

独立 MySQL 与当前 MO 的普通 SQL 控制包括：

```sql
create table t(txt varchar(16), bin varbinary(16));
insert into t values ('你a', '你a');
select char_length(if(txt is not null, txt, _binary '')),
       hex(reverse(case when txt is not null then txt else _binary '' end)),
       char_length(if(txt is not null, txt, bin)) from t;
-- 2 | 61E4BDA0 | 4
```

更强的 VARCHAR 声明不能被较弱 binary literal 的公共传输类型降为 byte 语义；真实 binary column、同等级 binary literal、显式 BINARY CAST（包括 NULL CAST）和 binary COLLATE 仍可拥有声明。显式 TEXT COLLATE 的 rank 0 也不能被保留下来的 binary literal 拼写覆盖。LPAD 等独立结果所有者、关系边界及 FIELD/REGEXP 等声明消费者不能借此被穿透。

实现复用已有 `stringDomainWitnessCollector`：两个 rank 标量与一个 binding-local `opaqueDeclaration`，沿原有遍历收集声明强度和独立结果所有权；不保存 AST/Expr 图、不新增持久化标志或函数白名单、不增加逐行图遍历。普通列及数值格式化可以证明 TEXT，但不把 allocating producer 的物理 TEXT 误当完整声明证明；显式 rank 0 TEXT 保持优先。CAST0 仅继承 rank 不构成新 COLLATE 所有者，实际 metadata 改变才构成边界。缺 Child 的 scalar 仍按未知双域保守处理。

另一处复用 `function.IntegerArgumentTarget`，让数值参数继续由原有数值规范化负责；不能为字符串声明消费提前截断 TIMESTAMPADD SECOND 的小数。没有针对 TIMESTAMPADD 名称另设例外。新增指针身份/全 Expr 不变控制，合并上游的两个 SECOND binary-source 回归和公共 TimestampAdd 文件均通过。

### 8.2 DEFAULT 两类失败必须分开

23 个永久 DEFAULT 控制使用实际 CREATE TABLE、完整 plan copy、wire roundtrip、Expr copy 和 Eval，并检查 original→copy 相等及执行后完整 Expr 不变；覆盖 VARBINARY/BIGINT 目标、CASE/IF/NULLIF、真实 binary/显式 CAST/COLLATE、空值和 NULL。保留 `NULLIF retains allocated owner`，另补 CASE 包含 allocating binary owner 的控制。

数值目标的原始复现：

```sql
create table t(txt varchar(16), n bigint default
  (char_length(if(txt is not null, txt, _binary ''))));
insert into t(txt) values ('你a');
select n from t;
insert into t(txt) values (NULL);
```

| 执行代 | 非 NULL 文本行 | NULL 选择 empty binary fallback |
|---|---:|---|
| MySQL 实际 DML | 2 | 成功，n=0 |
| 固定上游743完整 Go overlay，CREATE TABLE/实际 Eval | 2 | empty string 转 int 失败 |
| 本轮修正前的 feature/合并基座路径 | 4 | 公共 INSERT 报20203 |
| 当前 G 实际 DML | 2 | 仍报20203，未称已修复 |

上游 overlay 将所有 PR 不同的 Go 文件恢复到743（上游不存在的文件为 package-only stub），仅追加三项诊断及 batch/vector/colexec imports；不是只恢复 binder、保留其余 PR 代码的伪基线。最终实际执行为 IF_text PASS、IF_NULL/COALESCE_NULL FAIL，错误均为空串转 int；早先路径/缺 imports 的 build failure 单独保留，不作语义证据。

因此，G 修正的是所选 VARCHAR 的字符消费，不扩展为 DEFAULT 目的数值类型对 binary literal 的通用提前强转/警告兼容重设计。永久 UT 和当前实际 DML 都保留原 `_binary ''` 的非 NULL 行为；公共混合 NULL 表的数值 DEFAULT 使用独立核实的 `_binary '0'`，期望为 **2/0/1**。原 empty fallback SQL、MySQL 的 **2/0/0**、MO 失败及当前负向复现完整保留，不把换字面量当作原失败已通过。

### 8.3 最终源码验证与失败分代

环境仍为 Go1.27.1、匹配 GOROOT 的 gofmt、darwin/arm64、经过 provenance 校验的 native 依赖；`GOMAXPROCS=2`、`GOFLAGS=-p=1`、`GOMEMLIMIT=2GiB`。通过支持的 `make -j2 build-with-prebuilt-native BIN_NAME=CLAUDE_mo_review_G` 构建服务，保留默认 native/SIMD，省略 DWARF。测试、构建、公共运行和交付前的 Go SHA256 清单一致。

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test \
  ./pkg/sql/plan ./pkg/frontend ./pkg/sql/compile \
  ./pkg/sql/plan/function ./pkg/sql/parsers/tree \
  -count=1 -timeout=900s -covermode=atomic \
  -coverprofile=CLAUDE_owning.cover -json
```

同一次完整成功运行，没有拼接失败 profile：

| 包 | PASS（含 subtest） | 原有 skip | package 终端秒 |
|---|---:|---:|---:|
| plan | 18212 | 2 | 85.477 |
| frontend | 6266 | 1 | 17.400 |
| compile | 3680 | 0 | 12.441 |
| function | 11439 | 0 | 11.656 |
| tree | 149 | 0 | 0.849 |

相对合并基座，本轮变更可执行行 **35/36=97.2%**；相对固定上游，完整 PR **1011/1086=93.1%**。均超过75%，使用当前 G 的 atomic profile，不用 F 的旧 profile。完整 PR 逐文件分子/分母与第7.1节相比，仅 binder 改为 **274/288**；其余相同。这是普通包 instrumentation，不是统一 `-coverpkg` 的全仓覆盖率。

五包 vet 通过；golangci-lint `--new-from-rev=743e749a1e33d6c845bb73999e8c82d297a2b657` 为0 issues。Molint exit0，仍有22条与固定上游相同 owner/source 的诊断；没有执行独立基线 molint，不称零诊断。

失败代保留并分类：

- 初始三个 DEFAULT 语义红；A–E 的 typed/COLLATE/LPAD 反例；两个 SECOND 集成失败及合并基座对照。没有降低 genuine binary 或 allocating owner 的断言来放行。
- F focused、五包、静态、构建曾成功，但公共数值 DEFAULT 暴露漏路，故不能作为 G 验收。G focused 覆盖全部23项 DEFAULT、声明与 TimestampAdd 控制。
- G 首轮五包中 compile 的 `TestUnionNestedTopClearsAllocationOwners` 失败于 `group spill disk budget exceeded (requested=68, used=68, limit=128)`；当时磁盘约15GiB，低于16GiB自动 reserve。暂停父 launcher 未暂停其已运行子测试，这次失败完整保留。
- 有活动用户/FD 的 cache 清理均被预检拒绝；确认默认 Go cache 用户和打开文件均为0后执行 `go clean -cache`，恢复约53GiB。原资源 case 不改预算/断言重跑通过（0.01s）；之后重新执行上表完整五包，作为唯一当前验收 profile。未删除模块、native、源码、数据库、WIP 或证据，未停止无关进程。

本轮无新的性能或 race 数字：纯 TEXT 快路径仍先于新增判断，没有新增遍历、缓存或跨执行状态；第7节性能/生命周期结论保持其原源码和测量范围，不改称当前源码的完整性能等价或全仓 race 证明。

### 8.4 公共 SQL、结果审阅和范围

独立端口/全新服务数据用于 G，旧 F 数据保留；先以原始期望正常执行 dtype **93/93**。新增 SQL 的 **21 行** MySQL/MO TSV 逐字节一致后才生成候选；审阅仅两处结果变化：新增条件/嵌套/prepared 控制，以及 DEFAULT 表追加两列。旧列的值/metadata 和原 MULTICN 片段保留。

初次生成的 prepared 全 false 输出末列为空，mo-tester 格式产生行尾空格。保存原生成字节后，仅把新 SELECT 的 HEX/CHAR_LENGTH 两列交换顺序，独立重跑并核实21行中恰好三次 EXECUTE 的列置换；再生成、审阅并正常比较。没有手写期望值、关闭 metadata 或豁免 whitespace 检查。最终 `git diff --check` 通过。

正常 `run`、rate100 的独立报告：

| 文件 | 首轮 | 同实例第二轮 |
|---|---:|---:|
| dtype（本地单 CN 部分） | 104/104 | 104/104 |
| prepared_field_history | 492/492 | 492/492 |
| func_field | 97/97 | — |
| prepared_variadic_comparison | 104/104 | — |
| func_datetime_timestampadd | 99/99 | — |

七份报告均 FAILED/IGNORED/ABNORAML=0，error.txt 为空；dtype 保留 `@metacmp(true)`，优先级仍为 SQL > document > global。其他文件沿用原全局设置。测试库清理完成，所拥有服务退出、端口释放；外部 MySQL 与其他任务的 MO 未停止。生成报告不是正常比较的替代品。

本节仍不宣称 CASE 完整 MySQL parity：选择非空 binary literal 时的 charset/result-domain 规范化、更多 materialized conditional 情况、已有 CONCAT/explicit-CHAR/cross-explicit/unbounded-BINARY 差异仍有限制。不能把这些限制用于重新排除本节已明确修正的所选 VARCHAR+弱 binary literal 场景。未扩展到 partition、stored procedure、UDF 或警告兼容重设计。第6节的混合版本、restart、Proxy/MULTICN、实际并发 DDL physical retry、wire20631 注入和全仓 race 限度不变。

本地证据在 review20 worktree 的 `CLAUDE_*` 和 `CLAUDE_validation_artifacts/`，包括各候选失败、上游 overlay、资源失败/恢复、当前 profile、逐轮报告、独立 oracle 与源码清单；不提交临时工具/数据/TODO。D1/D2/D3 哈希和完整40文件变更图复核不变。作者自审及实施授权不代表独立 reviewer 批准；不 dismiss CHANGES_REQUESTED，不等待或请求 post-push CI。

## 9. review22：变量、独立 TEXT 结果、query-block 声明及显式 CAST（2026-10-21）

### 9.1 反例与共同所有者

起点为 `1ec4f5c5085c637c132c4b9cea9d0487a47736a5`，合并固定 `mo/main=76e152a1dd02b7875829027117a6f6344d1f8d33` 后基点为 `70b1bf3559b2841666175bf0c96d2e8541277e68`。本节不重写 D2/D3/DECISIONS 或前轮历史证据。

独立 MySQL 与保留的旧 G 二进制重现了三个 P2。G→1ec 的 planner/frontend/vector 闭包源码一致；该复用只归属于旧行为，不是当前构建验收。以下均选择 VARCHAR `你a` 或其纯 TEXT LPAD 结果：

| 场景 | 旧结果 | MySQL 与最终结果 |
|---|---|---|
| CASE 的 peer 为 binary 用户变量 | 长度2、`61E4BDA0` | 长度4、`61A0BDE4` |
| CASE 的结果为 `LPAD(txt,3,'z')`、peer 为弱 binary literal | 长度5、`61A0BDE47A` | 长度3、`61E4BDA07A` |
| CASE 经 LIMIT/HAVING 派生表 | 长度4、`61A0BDE4` | 长度2、`61E4BDA0` |

变量仍保留冻结 row domain，但其声明必须参与 coercibility 竞争，不能等同于弱 literal。LPAD 仍拥有结果声明；使用已有 resolver 和唯一的函数契约映射区分真实 TEXT/BINARY，不把 allocating producer 一概 opaque，也不将其透明化。缺失声明的 scalar 保持保守。真实 binary LPAD、显式 BINARY/COLLATE、同级 binary 列等对照未削弱。

LIMIT/HAVING 仍是边界。普通 CASE/IF 和无结果参数依赖的 prepared CASE 共用逻辑声明证明，不能将公共 binary transport 升格为最终声明。已有 H 和参数推导模式保持原优先级；无 H 分支不得因读取声明而建立 history。没有 SQL 形状例外、新 producer 白名单、持久化字段、跨执行图引用或逐行遍历。

同头独立 review 还指出 P1：

```sql
prepare s from 'select field(case when ? then ? else null end,''A'',''a''),cast(? as signed)';
```

依次执行 `(1,NULL,"1")`、`(1,"a","1")`、`(1,"a",1)`，旧结果为 `0/2/1`，MySQL/最终结果为 `0/2/2`。真实 PREPARE 产生的 CAST1 可以没有 `SyntaxExplicitCast`；参数 seeding 现复用既有 `isExplicitPreparedCast`。SIGNED、DECIMAL、CHAR、嵌套 CHAR→SIGNED 均通过实际 PREPARE、`initExecuteStmtParam` 和 Eval 验证，最后一次执行保持 H 指针及 revision。没有新增历史代际或放宽普通参数的 reprepare 契约。

### 9.2 保存的失败与最终源码验收

- 初始 typed/真实 frontend 回归均先失败；公开 SQL 亦独立确认，不使用实现自身生成 oracle。
- B 轮普通 SQL、五包和静态通过，但追加真实 PREPARE 后，LIMIT 文本 CASE 仍为4/4，HAVING 文本 LPAD CASE 仍为5/5。失败来自无 H 分支直接 `common(values)`、绕过共同声明证明。B 的 profile/build/生成结果全部留存，未当作最终验收。
- 补齐后，C 的 HAVING UT 暴露 fixture 未安装 process prepare-params（`get prepare params error, index 0 not exists`）。按真实执行环境设置参数并立即登记清理，不改变 SQL、断言或产品逻辑；最终 D 全部通过。LIMIT/HAVING 各含 TEXT 列和 TEXT LPAD，重复绑定且 H 始终为 nil；公开 PREPARE/EXECUTE 另行验证。
- 新上游改变 native 输入，旧产物预检被正确拒绝，当前 worktree 重新 `make -j2 cgo` 并验证 provenance。低空间时拒绝清理有活动 lint/1131个FD的默认 cache；确认私有缓存无用户/FD后 `go clean -cache`，随后默认缓存也仅在无用户/FD时清理。源码、模块、native、数据、WIP、失败证据均保留。

最终 D 的一份完整 atomic profile（Go1.27.1、Darwin/arm64、支持的 CGo wrapper）：

| 包 | PASS事件（含子测试） | SKIP | 秒 |
|---|---:|---:|---:|
| sql/plan | 18253 | 2 | 88.996 |
| frontend | 6274 | 1 | 17.296 |
| sql/compile | 3700 | 0 | 13.352 |
| sql/plan/function | 11439 | 0 | 10.962 |
| sql/parsers/tree | 149 | 0 | 0.862 |

增量可执行行覆盖 **42/43=97.7%**，全 PR **1037/1111=93.3%**。这是普通 owning-package instrumentation，不是全仓 `-coverpkg`；seeding 行由真实 frontend 回归执行，但不冒称计入 plan 包独立 profile。gofmt/vet 通过；molint exit0 且20条诊断与固定 main 的 owner/源码等价（不是执行了 baseline molint）；匹配 CI 工具链的增量 golangci-lint **0 issues**。支持的 `build-with-prebuilt-native` 及源码清单校验通过。

### 9.3 公共验收与边界

最终服务使用独立新数据与端口。先按原期望执行 dtype **104/104**，随后34行新 SQL 的 MySQL/MO TSV 逐字节一致，才用 mo-tester 生成结果。结果差异仅一个新增 hunk，旧值/metadata/MULTICN 片段不变；生成模式留下的旧 report 不作为生成或正常比较证据。正式验收均为随后独立的正常比较：

| 文件 | 首轮 | 同实例第二轮 |
|---|---:|---:|
| dtype（本地单 CN 部分） | 139/139 | 139/139 |
| prepared_field_history | 492/492 | 492/492 |
| func_field | 97/97 | — |
| prepared_variadic_comparison | 104/104 | — |
| func_datetime_timestampadd | 99/99 | — |

七份正常报告 FAILED/IGNORED/ABNORAML=0，error.txt 均为空。dtype 的 `@metacmp(true)` 不变。另一个独立连接重复执行 prepared LIMIT/HAVING，分别得到2/2、3/3及正确 reverse bytes，与 MySQL 一致。所有自有库已清理、服务退出、端口释放；未停止外部服务。

原数字 DEFAULT 的 `_binary ''` NULL fallback 在最终实际 DML 中仍报20203，非 NULL 文本返回2；MySQL为2/0。该既有边界没有修复，也未改成成功计数。本节不宣称一般 CASE charset/结果 metadata、选择非空 binary literal 时的规范化、全部物化/聚合拓扑均与 MySQL 等价。第6—8节的其他明确限度继续有效，但不能再排除本节已验证的变量、纯 TEXT LPAD、普通及 prepared LIMIT/HAVING 场景。

没有新的 race/benchmark 数字；纯 TEXT 快路径不变，新解析限于绑定期既有声明路径，没有跨执行状态或新同步。历史性能/race保留原源码归属，不改称当前全仓、Linux/XFS、Proxy/MULTICN、restart、混合版本或真实并发DDL physical retry证明。最终证据在 review22 worktree 的 `CLAUDE_*`、`CLAUDE_phase_B/` 和 `CLAUDE_validation_artifacts/`，本地 TODO/工具/数据不提交。自审不是独立 reviewer 批准，不 dismiss CHANGES_REQUESTED，不等待 post-push CI。
