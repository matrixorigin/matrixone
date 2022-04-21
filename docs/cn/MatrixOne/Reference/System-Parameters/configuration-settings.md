# **通用配置参数**

MatrixOne的配置文件位于[matrixone/pkg/config/test/system_vars_config.toml](https://github.com/matrixorigin/matrixone/blob/main/pkg/config/test/system_vars_def.toml)。

### 常规设置
| 参数  | 数据类型  |  默认值   | 取值范围  | 作用 | 
|  ----  | ----  |  --------  |  --- | --- |
| rootpassword  | string | 	""  | string value |  用户的密码|
| dumpdatabase  | string | 	default  | string value |  用于备份的转储数据库名|
| port  | int64 | 	6001  | [0 - 65536] | 定义了MO服务器监听以及客户端连接的端口|
| host  | string | 	0.0.0.0  | [0.0.0.0 - 255.255.255.255]  | 监听IP|
| sendRow  | bool | false  | [true, false] | send data row while producing  |
| dumpEnv  | bool | false  | [true, false] | dump Environment with memEngine Null nodes for testing  |


### Debug设置


| 参数  | 数据类型  |  默认值   | 取值范围  | 作用 | 
|  ----  | ----  |  --------  |  --- | --- |
| level  | string | debug  | [debug, info, warn, error, fatal] | 日志输出级别 |
| format  | string | 	json  | [json, console] |  输出的日志文件的格式 |
| filename  | string | 	""  | string value | 输出的日志文件名 |
| max-size  | int64 | 	512  |  [0 - 314572800] | 最大日志文件大小|
| max-days  | int64 | 	0  |  [0 - 314572800] | 日志文件保存的最大天数|
| max-backups  | int64 | 	0  |  [0 - 314572800] | 要保留的旧日志文件的最大数量|


### 内存设置

| 参数  | 数据类型  |  默认值   | 取值范围  | 作用 | 
|  ----  | ----  |  --------  |  --- | --- |
| hostMmuLimitation  | int64 | 1099511627776  | [0 - 1099511627776] | 主机的mmu限制，默认值: 1 << 40 = 1099511627776  |
| guestMmuLimitation  | int64 | 1099511627776  | [0 - 1099511627776] | 虚拟机的mmu限制默认值: 1 << 40 = 1099511627776  |
| mempoolMaxSize  | int64 | 1099511627776  | [0 - 1099511627776] | 内存最大容量 默认值: 1 << 40 = 1099511627776  |
| mempoolFactor  | int64 | 8  | [0 - TBD] | mempool factor，默认值: 8   |
| processLimitationSize  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.Size，默认值: 10 << 32 = 42949672960  |
| processLimitationBatchRows  | i | 42949672960  | [0 - 42949672960] | process.Limitation.BatchRows，默认值: 10 << 32 = 42949672960  |nt64
| processLimitationBatchRows  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.BatchRows，默认值: 10 << 32 = 42949672960  |
| processLimitationPartitionRows  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.PartitionRows，默认值: 10 << 32 = 42949672960  |


