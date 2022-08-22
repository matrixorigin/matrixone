# **通用配置参数**

启动 MatrixOne 实例时，配置文件需要设置相关参数。

执行 MatrixOne 的编译命令 `make config` 时，在 *matrixone* 目录下自动产生一个配置文件 *matrixone/system_vars_config.toml*。

相关参数设置如下:

### 常规设置

| 参数  | 数据类型  |  默认值   | 取值范围  | 作用 |
|  ----  | ----  |  --------  |  --- | --- |
| rootpassword  | string | 	""  | string value |  用户的密码|
| dumpdatabase  | string | 	default  | string value |  用于备份的转储数据库名|
| port  | int64 | 	6001  | [0 - 65536] | 定义了MO服务器监听以及客户端连接的端口|
| host  | string | 	0.0.0.0  | [0.0.0.0 - 255.255.255.255]  | 监听IP|

### Log 设置

| 参数  | 数据类型  |  默认值   | 取值范围  | 作用 |
|  ----  | ----  |  --------  |  --- | --- |
| logLevel  | string | debug  | [debug, info, warn, error, fatal] | 日志输出级别 |
| logFormat  | string | 	json  | [json, console] |  输出日志样式 |
| logFilename  | string | 	""  | string value | 输出日志文件名称 |
| logMaxSize  | int64 | 	512  |  [0 - 314572800] | 最大日志文件大小|
| logMaxDays  | int64 | 	0  |  [0 - 314572800] | 日志文件最多保存天数|
| logMaxBackups  | int64 | 	0  |  [0 - 314572800] | 旧日志文件最多保留数目maximum numbers of old log files to retain|
| lengthOfQueryPrinted  | int64 | 	50  |  [-1 - 10000] | 打印到控制台的查询的长度。“-1”：完整的字符串。“0”：空字符串。“>0"：字符串头部的字符长度。|
| printLogInterVal  | int64 | 	10  |  [1 - 1000] | 打印日志的时间间隔 |

### 数据存储设置

| 参数  | 数据类型  |  默认值   | 取值范围  | 作用 |
|  ----  | ----  |  --------  |  --- | --- |
| storePath  | string | ./store  | file path | 数据存储的根目录 |

### 内存设置

| 参数  | 数据类型  |  默认值   | 取值范围  | 作用 |
|  ----  | ----  |  --------  |  --- | --- |
| hostMmuLimitation  | int64 | 1099511627776  | [0 - 1099511627776] | 主机的mmu限制，默认值: 1 << 40 = 1099511627776  |
| guestMmuLimitation  | int64 | 1099511627776  | [0 - 1099511627776] | 虚拟机的mmu限制默认值: 1 << 40 = 1099511627776  |
| mempoolMaxSize  | int64 | 1099511627776  | [0 - 1099511627776] | 内存最大容量 默认值: 1 << 40 = 1099511627776  |
| mempoolFactor  | int64 | 8  | [0 - TBD] | mempool factor，默认值: 8   |
| processLimitationSize  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.Size，默认值: 10 << 32 = 42949672960  |
| processLimitationBatchRows  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.BatchRows，默认值: 10 << 32 = 42949672960  |
| processLimitationPartitionRows  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.PartitionRows，默认值: 10 << 32 = 42949672960  |

### 数据指标设置

| 参数  | 数据类型  |  默认值   | 取值范围  | 作用 |
|  ----  | ----  |  --------  |  --- | --- |
| statusPort  | int64 | 7001  | All ports | statusPort 定义状态服务器监听的端口和客户端连接的端口 |
| metricToProm  | bool | true  | true false | 如果设置为 true，数据指标可以通过host:status/metrics endpoint 抓取 |
| enableMetric  | bool | true  | true false | 默认为 true，表示在启动时启用数据指标|

### 其他设置

| 参数  | 数据类型  |  默认值   | 取值范围  | 作用 |
|  ----  | ----  |  --------  |  --- | --- |
| batchSizeInLoadData  | int64 | 50  | 10 - 40000 | 在加载数据时，批处理的行数 |
| loadDataConcurrencyCount  | int64 | 4  | 1 - 16 | 加载数据的并发线程 |
| maxBytesInOutbufToFlush  | int64 | 1024  | 32 - 4096 KB | 输出内存缓冲区，当缓冲区超过此限制时将刷新该缓冲区 |
| exportDataDefaultFlushSize  | int64 | 1  | 1,2,4,8 MB| 导出数据到 *.csv* 文件默认 flush 大小   |
