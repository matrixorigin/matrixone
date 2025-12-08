**sql**
* snapshot diff 
 objectlist [database d] [table t] snapshot sp2 [against snapshot sp1]
返回值:db name, table name, object list
for table,
get table,
get snapshot ts(from,to)
scan partition state

* create database/table from cluster(show subscribe的结果,检查上游是否发布)

* get snapshot ts

* drop database/table(删除mo_sync_configs)

* get object

**subscribe**
update mo_sync_configs

**sql builder**
create snapshot
query mo databases
query mo tables
query mo columns

**iteration**
* new txn(engine, client)txn
* 0 lock table(input engine, txn)

* 1.1 请求上游snapshot(sinker, table info/ db info)

* 1.2 查询上游三表 -> ddl(table info,sinker,原始id)
    查询上游，查询下游

* 2 snapshot diff->object list*cn
    下游snapshot diff

* 3 get object
* 4 write(filter) object
aobj排序，删除ts abort，truncate

** tn apply object(需要覆盖旧值)

* 5 drop snapshot

* iterationcontext
upstream sinker
query executor
source info(id映射)
prev aobj

**sinker**
start txn
send sql
end txn

**init executor**

**executor**
* apply system table
  
* check state and gen iteration

**snapshot meta diff** ?
collect change scan object

**get object**
复制文件

**优化alter不删表？**

**检查权限**