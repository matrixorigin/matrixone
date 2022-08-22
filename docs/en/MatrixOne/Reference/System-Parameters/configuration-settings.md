# MatrixOne Server Setting

To launch a MatrixOne instance, a configuration file is required.
The configuration file will be generated in `make config`, it's located in `matrixone/system_vars_config.toml` file.

The settings are listed as below:

### General Settings

|  Parameter   | Type  |  Default Value   | Range  | Functionality |
|  ----  | ----  |  --------  |  --- | --- |
| rootpassword  | string | 	""  | string value | password for root user|
| dumpdatabase  | string | 	default  | string value |  dump database name for backup|
| port  | int64 | 	6001  | [0 - 65536] | port defines which port the mo-server listens on and clients connect to |
| host  | string | 	0.0.0.0  | [0.0.0.0 - 255.255.255.255]  | listening ip|

### Log Settings

|  Parameter   | Type  |  Default Value   | Range  | Functionality |
|  ----  | ----  |  --------  |  --- | --- |
| logLevel  | string | debug  | [debug, info, warn, error, fatal] | the log output level |
| logFormat  | string | 	json  | [json, console] |  output log style |
| logFilename  | string | 	""  | string value | output log filename |
| logMaxSize  | int64 | 	512  |  [0 - 314572800] | maximum log file size|
| logMaxDays  | int64 | 	0  |  [0 - 314572800] | maximum log file days kept|
| logMaxBackups  | int64 | 	0  |  [0 - 314572800] | maximum numbers of old log files to retain|
| lengthOfQueryPrinted  | int64 | 	50  |  [-1 - 10000] | the length of query printed into console. -1, complete string. 0, empty string. >0 , length of characters at the header of the string. |
| printLogInterVal  | int64 | 	10  |  [1 - 1000] | the interval for printing logs |

### Data Storage Settings

|  Parameter   | Type  |  Default Value   | Range  | Functionality |
|  ----  | ----  |  --------  |  --- | --- |
| storePath  | string | ./store  | file path | the root directory of data storage |

### Memory Settings

|  Parameter   | Type  |  Default Value   | Range  | Functionality |
|  ----  | ----  |  --------  |  --- | --- |
| hostMmuLimitation  | int64 | 1099511627776  | [0 - 1099511627776] | host mmu limitation. default: 1 << 40 = 1099511627776  |
| guestMmuLimitation  | int64 | 1099511627776  | [0 - 1099511627776] | guest mmu limitation. default: 1 << 40 = 1099511627776  |
| mempoolMaxSize  | int64 | 1099511627776  | [0 - 1099511627776] | mempool maxsize. default: 1 << 40 = 1099511627776  |
| mempoolFactor  | int64 | 8  | [0 - TBD] | mempool factor. Default: 8   |
| processLimitationSize  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.Size. default: 10 << 32 = 42949672960  |
| processLimitationBatchRows  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.BatchRows. default: 10 << 32 = 42949672960  |
| processLimitationPartitionRows  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.PartitionRows. default: 10 << 32 = 42949672960  |

### Metrics Settings

|  Parameter   | Type  |  Default Value   | Range  | Functionality |
|  ----  | ----  |  --------  |  --- | --- |
| statusPort  | int64 | 7001  | All ports | statusPort defines which port the mo status server (for metric etc.) listens on and clients connect to |
| metricToProm  | bool | true  | true false | if true, metrics can be scraped through host:status/metrics endpoint |
| enableMetric  | bool | true  | true false | default is true. if true, enable metric at booting |

### Other Settings

|  Parameter   | Type  |  Default Value   | Range  | Functionality |
|  ----  | ----  |  --------  |  --- | --- |
| batchSizeInLoadData  | int64 | 50  | 10 - 40000 | the number of rows for a batch in loading data |
| loadDataConcurrencyCount  | int64 | 4  | 1 - 16 | the concurrent threads to load data |
| maxBytesInOutbufToFlush  | int64 | 1024  | 32 - 4096 KB | the output memory buffer, the buffer will be flushed when it exceeds this limit   |
| exportDataDefaultFlushSize  | int64 | 1  | 1,2,4,8 MB| texport data to csv file default flush size   |
