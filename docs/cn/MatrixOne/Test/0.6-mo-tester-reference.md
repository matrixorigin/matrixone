# MO-Tester 规范要求

## 编写测试用例规范 - case

|规范|详情|
|---|---|
|文件命名|<br>1. 以 *.sql* 或 *.test* 作为后缀。</br><br>2. 文件名称有实际含义。例如，需要编写测试索引用例，可以将测试索引用例命名为 *index.sql* 或者*index_xxxx.sql*。</br>|
|测试用例|<br>1. 测试用例，即 case 内的具体示例内容，所有的空行在测试时都自动忽略，你可以通过添加空行来是整个文件内容更易读。</br><br>2. 每条 SQL 语句只写一行，如果必须要多行书写，那么每行必须顶格书写，且 SQL 结尾不能有空格，否则将造成 *case* 文件 和 *result* 文件中 SQL 不能完全匹配。</br><br>3. 添加注释，注明当前所写测试用例的目的。</br><br>4. 为需要增加 Tag 标签测试用例添加 Tag。</br>|

### 测试用例注解说明 - Tag

|注解|起始|结束|说明|
|---|---|---|---|
|-- @bvt:issue|-- @bvt:issue#{issueNO.}]|-- @bvt:issue|带有此注解标记的 SQL 将不会被执行|
|-- @sessio|-- @session:id=X{|-- @session}|带有此主键标记的所有 SQL 将在 id=X 的新会话中执行|
|-- @separator|/|/| 指定 SQL 语句在解析其 result 以及生成 result 文件的时候，使用的列分隔符，有两个取值|
|-- @separator:table|/|/|表示结果中列分隔符为制表符 \t|
|-- @separator:space|/|/|表示结果中列分隔符为4个空格|
|-- @sortkey|-- @sortkey:1,2,3|/|表示该SQL语句的结果是有序的，排序键为第1，2，3列（从0开始）；正常情况下，被测试系统返回的结果顺序是不固定的，所以工具在比对的时候，会把实际结果和预期结果都进行排序后比对，但是对于某些 SQL，其预期的结果就应该是有序的，比如存在 `order by` 的 SQL 语句，那么需要把类似这种 SQL 添加上这样的 Tag，工具在比对的时候，不会对 *sortkey* 中的这些列进行重新排序|

## 编写测试结果规范 - result

|规范|详情|
|---|---|
|普通测试用例生成测试结果|<br>1. 如果新增了测试 case 文件，首先确保所有 SQL 都调试通过，使用 MO-Tester 工具自动生成测试结果。</br><br>2. 如果是在已有的 case 内新增一些 SQL，首先确保所有 SQL 都调试通过，使用 MO-Tester 工具自动生成测试结果。</br>|
|含有Tag 的测试用例生成测试结果|<br>1. 如果新增测试 case 文件，且case 文件内含有带 `--bvt:issue` 标签 SQL，使用 MO-Tester 工具自动生成测试结果，带 `--bvt:issue` 标签 SQL 在所生成的 result 文件中结果为 `unknown result because it is related to issue#XXX"`。</br><br>2. 如果是已有测试 case 文件，已通过 MO-Tester 工具自动生成测试结果，再次使用 MO-Tester 工具自动生成测试结果时，带 `--bvt:issue` 标签 SQL 在所生成的 result 文件中结果将保留原有值，不再更新。</br>|
|手动编写测试结果|<br>1. 如果是手动编写 result，不可有空行，否则结果将产生解析错误。</br><br>2. 如果预期某条 SQL 的执行结果中，存在制表符或者超过连续的4个空格，则在测试 case 文件中对应的SQL语句，必须增加 Tag 标签 `-- @separator:`，否则将解析失败。例如，若 case 文件中对应的 SQL 语句含连续4个空格，则需要指定 Tag 标签 `-- @separator:table`；若含含有制表符，则指定 Tag 标签 `-- @separator:space`。</br>|
