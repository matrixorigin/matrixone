The configuration file is located in [matrixone/pkg/config/test/system_vars_config.toml](https://github.com/matrixorigin/matrixone/blob/main/pkg/config/test/system_vars_def.toml).

The 0.1.0 version is a standalone version, only the following parameters should be paied attention to. The other settings should remain default.

### General Settings

|  Parameter   | Type  |  Default Value   | Range  | Functionality | 
|  ----  | ----  |  --------  |  --- | --- |
| rootpassword  | string | 	""  | string value | password for root user|
| dumpdatabase  | string | 	default  | string value |  dump database name for backup|
| port  | int64 | 	6001  | [0 - 65536] | port defines which port the mo-server listens on and clients connect to |
| host  | string | 	0.0.0.0  | [0.0.0.0 - 255.255.255.255]  | listening ip|
| sendRow  | bool | false  | [true, false] | send data row while producing  |
| dumpEnv  | bool | false  | [true, false] | dump Environment with memEngine Null nodes for testing  |


### Debug Settings


|  Parameter   | Type  |  Default Value   | Range  | Functionality | 
|  ----  | ----  |  --------  |  --- | --- |
| level  | string | debug  | [debug, info, warn, error, fatal] | the log output level |
| format  | string | 	json  | [json, console] |  output log style |
| filename  | string | 	""  | string value | output log filename |
| max-size  | int64 | 	512  |  [0 - 314572800] | maximum log file size|
| max-days  | int64 | 	0  |  [0 - 314572800] | maximum log file days kept|
| max-backups  | int64 | 	0  |  [0 - 314572800] | maximum numbers of old log files to retain|


### Memory Settings

|  Parameter   | Type  |  Default Value   | Range  | Functionality | 
|  ----  | ----  |  --------  |  --- | --- |
| hostMmuLimitation  | int64 | 1099511627776  | [0 - 1099511627776] | host mmu limitation. default: 1 << 40 = 1099511627776  |
| guestMmuLimitation  | int64 | 1099511627776  | [0 - 1099511627776] | guest mmu limitation. default: 1 << 40 = 1099511627776  |
| mempoolMaxSize  | int64 | 1099511627776  | [0 - 1099511627776] | mempool maxsize. default: 1 << 40 = 1099511627776  |
| mempoolFactor  | int64 | 8  | [0 - TBD] | mempool factor. Default: 8   |
| processLimitationSize  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.Size. default: 10 << 32 = 42949672960  |
| processLimitationBatchRows  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.BatchRows. default: 10 << 32 = 42949672960  |
| processLimitationBatchRows  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.BatchRows. default: 10 << 32 = 42949672960  |
| processLimitationPartitionRows  | int64 | 42949672960  | [0 - 42949672960] | process.Limitation.PartitionRows. default: 10 << 32 = 42949672960  |


