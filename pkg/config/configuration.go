// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type ConfigurationKeyType int

const (
	ParameterUnitKey ConfigurationKeyType = 1
)

var (
	//root name
	defaultRootName = "root"

	//root password
	defaultRootPassword = ""

	//dump user name
	defaultDumpUser = "dump"

	//dump user password
	defaultDumpPassword = "111"

	//port defines which port the mo-server listens on and clients connect to
	defaultPort = 6001

	//listening ip
	defaultHost = "0.0.0.0"

	//listening unix domain socket
	defaultUnixAddr = "/tmp/mysql.sock"

	//guest mmu limitation.  1 << 40 = 1099511627776
	defaultGuestMmuLimitation = 1099511627776

	//mempool maxsize.  1 << 40 = 1099511627776
	defaultMempoolMaxSize = 1099511627776

	//mempool factor.
	defaultMempoolFactor = 8

	//process.Limitation.Size.  10 << 32 = 42949672960
	defaultProcessLimitationSize = 42949672960

	//process.Limitation.BatchRows.  10 << 32 = 42949672960
	defaultProcessLimitationBatchRows = 42949672960

	//process.Limitation.PartitionRows.  10 << 32 = 42949672960
	defaultProcessLimitationPartitionRows = 42949672960

	//the root directory of the storage
	defaultStorePath = "./store"

	defaultServerVersionPrefix = "8.0.30-MatrixOne-v"

	//the length of query printed into console. -1, complete string. 0, empty string. >0 , length of characters at the header of the string.
	defaultLengthOfQueryPrinted = 200000

	//the count of rows in vector of batch in load data
	defaultBatchSizeInLoadData = 40000

	//initial value is 4. The count of go routine writing batch into the storage.
	defaultLoadDataConcurrencyCount = 4

	//KB. When the number of bytes in the outbuffer exceeds the it,the outbuffer will be flushed.
	defaultMaxBytesInOutbufToFlush = 1024

	//printLog Interval is 10s.
	defaultPrintLogInterVal = 10

	//export data to csv file default flush size
	defaultExportDataDefaultFlushSize = 1

	//port defines which port the rpc server listens on
	defaultPortOfRpcServerInComputationEngine = 20000

	//statusPort defines which port the mo status server (for metric etc.) listens on and clients connect to
	defaultStatusPort = 7001

	// defaultBatchProcessor default is FileService. if InternalExecutor, use internal sql executor, FileService will implement soon.
	defaultBatchProcessor = "FileService"

	// defaultTraceExportInterval default: 15 sec.
	defaultTraceExportInterval = 15

	// defaultMetricExportInterval default: 15 sec.
	defaultMetricExportInterval = 15

	// defaultLogShardID default: 1
	defaultLogShardID = 1

	// defaultDNReplicaID default: 1
	defaultDNReplicaID = 1
	// defaultMetricGatherInterval default: 15 sec.
	defaultMetricGatherInterval = 15

	// defaultMetricUpdateStorageUsageInterval default: 15 min.
	defaultMetricUpdateStorageUsageInterval = 15 * time.Minute

	// defaultMergeCycle default: 4 hours
	defaultMergeCycle = 4 * time.Hour

	// defaultMaxFileSize default: 128 MB
	defaultMaxFileSize = 128

	// defaultPathBuilder, val in [DBTable, AccountDate]
	defaultPathBuilder = "AccountDate"

	// defaultSessionTimeout default: 24 hour
	defaultSessionTimeout = 24 * time.Hour

	// defaultLogsExtension default: tae. Support val in [csv, tae]
	defaultLogsExtension = "tae"

	// defaultMergedExtension default: tae. Support val in [csv, tae]
	defaultMergedExtension = "tae"

	// defaultOBShowStatsInterval default: 1min
	defaultOBShowStatsInterval = time.Minute

	// defaultOBMaxBufferCnt
	defaultOBBufferCnt int32 = -1

	//defaultOBBufferSize, 10 << 20 = 10485760
	defaultOBBufferSize int64 = 10485760

	// defaultPrintDebugInterval default: 30 minutes
	defaultPrintDebugInterval = 30

	// defaultKillRountinesInterval default: 1 minutes
	defaultKillRountinesInterval = 1

	//defaultCleanKillQueueInterval default: 60 minutes
	defaultCleanKillQueueInterval = 60
)

// FrontendParameters of the frontend
type FrontendParameters struct {
	MoVersion string

	//root name
	RootName string `toml:"rootname"`

	//root password
	RootPassword string `toml:"rootpassword"`

	DumpUser string `toml:"dumpuser"`

	DumpPassword string `toml:"dumppassword"`

	//dump database
	DumpDatabase string `toml:"dumpdatabase"`

	//port defines which port the mo-server listens on and clients connect to
	Port int64 `toml:"port"`

	//listening ip
	Host string `toml:"host"`

	// UnixSocketAddress listening unix domain socket
	UnixSocketAddress string `toml:"unix-socket"`

	//guest mmu limitation. default: 1 << 40 = 1099511627776
	GuestMmuLimitation int64 `toml:"guestMmuLimitation"`

	//mempool maxsize. default: 1 << 40 = 1099511627776
	MempoolMaxSize int64 `toml:"mempoolMaxSize"`

	//mempool factor. default: 8
	MempoolFactor int64 `toml:"mempoolFactor"`

	//process.Limitation.Size. default: 10 << 32 = 42949672960
	ProcessLimitationSize int64 `toml:"processLimitationSize"`

	//process.Limitation.BatchRows. default: 10 << 32 = 42949672960
	ProcessLimitationBatchRows int64 `toml:"processLimitationBatchRows"`

	//process.Limitation.BatchSize. default: 0
	ProcessLimitationBatchSize int64 `toml:"processLimitationBatchSize"`

	//process.Limitation.PartitionRows. default: 10 << 32 = 42949672960
	ProcessLimitationPartitionRows int64 `toml:"processLimitationPartitionRows"`

	//the root directory of the storage and matrixcube's data. The actual dir is cubeDirPrefix + nodeID
	StorePath string `toml:"storePath"`

	//the root directory of the storage and matrixcube's data. The actual dir is cubeDirPrefix + nodeID
	ServerVersionPrefix string `toml:"serverVersionPrefix"`

	//the length of query printed into console. -1, complete string. 0, empty string. >0 , length of characters at the header of the string.
	LengthOfQueryPrinted int64 `toml:"lengthOfQueryPrinted"`

	//the count of rows in vector of batch in load data
	BatchSizeInLoadData int64 `toml:"batchSizeInLoadData"`

	//default is 4. The count of go routine writing batch into the storage.
	LoadDataConcurrencyCount int64 `toml:"loadDataConcurrencyCount"`

	//default is false. Skip writing batch into the storage
	LoadDataSkipWritingBatch bool `toml:"loadDataSkipWritingBatch"`

	//KB. When the number of bytes in the outbuffer exceeds it,the outbuffer will be flushed.
	MaxBytesInOutbufToFlush int64 `toml:"maxBytesInOutbufToFlush"`

	//default printLog Interval is 10s.
	PrintLogInterVal int64 `toml:"printLogInterVal"`

	//export data to csv file default flush size
	ExportDataDefaultFlushSize int64 `toml:"exportDataDefaultFlushSize"`

	//port defines which port the rpc server listens on
	PortOfRpcServerInComputationEngine int64 `toml:"portOfRpcServerInComputationEngine"`

	//default is false. false : one txn for an independent batch true : only one txn during loading data
	DisableOneTxnPerBatchDuringLoad bool `toml:"DisableOneTxnPerBatchDuringLoad"`

	//default is 'debug'. the level of log.
	LogLevel string `toml:"logLevel"`

	//default is 'json'. the format of log.
	LogFormat string `toml:"logFormat"`

	//default is ''. the file
	LogFilename string `toml:"logFilename"`

	//default is 512MB. the maximum of log file size
	LogMaxSize int64 `toml:"logMaxSize"`

	//default is 0. the maximum days of log file to be kept
	LogMaxDays int64 `toml:"logMaxDays"`

	//default is 0. the maximum numbers of log file to be retained
	LogMaxBackups int64 `toml:"logMaxBackups"`

	//default is false. With true. Server will support tls
	EnableTls bool `toml:"enableTls"`

	//default is ''. Path of file that contains list of trusted SSL CAs for client
	TlsCaFile string `toml:"tlsCaFile"`

	//default is ''. Path of file that contains X509 certificate in PEM format for client
	TlsCertFile string `toml:"tlsCertFile"`

	//default is ''. Path of file that contains X509 key in PEM format for client
	TlsKeyFile string `toml:"tlsKeyFile"`

	//default is 1
	LogShardID uint64 `toml:"logshardid"`

	//default is 1
	DNReplicaID uint64 `toml:"dnreplicalid"`

	EnableDoComQueryInProgress bool `toml:"comQueryInProgress"`

	//timeout of the session. the default is 10minutes
	SessionTimeout toml.Duration `toml:"sessionTimeout"`

	// MaxMessageSize max size for read messages from dn. Default is 10M
	MaxMessageSize uint64 `toml:"max-message-size"`

	// default off
	SaveQueryResult string `toml:"saveQueryResult"`

	// default 24 (h)
	QueryResultTimeout uint64 `toml:"queryResultTimeout"`

	// default 100 (MB)
	QueryResultMaxsize uint64 `toml:"queryResultMaxsize"`

	AutoIncrCacheSize uint64 `toml:"autoIncrCacheSize"`

	LowerCaseTableNames string `toml:"lowerCaseTableNames"`

	PrintDebug bool `toml:"printDebug"`

	PrintDebugInterval int `toml:"printDebugInterval"`

	KillRountinesInterval int `toml:"killRountinesInterval"`

	CleanKillQueueInterval int `toml:"cleanKillQueueInterval"`

	// ProxyEnabled indicates that proxy module is enabled and something extra
	// is needed, such as update the salt.
	ProxyEnabled bool `toml:"proxy-enabled"`

	// SkipCheckPrivilege denotes the privilege check should be passed.
	SkipCheckPrivilege bool `toml:"skipCheckPrivilege"`
}

func (fp *FrontendParameters) SetDefaultValues() {
	if fp.RootName == "" {
		fp.RootName = defaultRootName
	}

	if fp.RootPassword == "" {
		fp.RootPassword = defaultRootPassword
	}

	if fp.DumpUser == "" {
		fp.DumpUser = defaultDumpUser
	}

	if fp.DumpPassword == "" {
		fp.DumpPassword = defaultDumpPassword
	}

	if fp.Port == 0 {
		fp.Port = int64(defaultPort)
	}

	if fp.Host == "" {
		fp.Host = defaultHost
	}

	if fp.UnixSocketAddress == "" {
		fp.UnixSocketAddress = defaultUnixAddr
	}

	if fp.GuestMmuLimitation == 0 {
		fp.GuestMmuLimitation = int64(toml.ByteSize(defaultGuestMmuLimitation))
	}

	if fp.MempoolMaxSize == 0 {
		fp.MempoolMaxSize = int64(toml.ByteSize(defaultMempoolMaxSize))
	}

	if fp.MempoolFactor == 0 {
		fp.MempoolFactor = int64(defaultMempoolFactor)
	}

	if fp.ProcessLimitationSize == 0 {
		fp.ProcessLimitationSize = int64(toml.ByteSize(defaultProcessLimitationSize))
	}

	if fp.ProcessLimitationBatchRows == 0 {
		fp.ProcessLimitationBatchRows = int64(toml.ByteSize(defaultProcessLimitationBatchRows))
	}

	if fp.ProcessLimitationPartitionRows == 0 {
		fp.ProcessLimitationPartitionRows = int64(toml.ByteSize(defaultProcessLimitationPartitionRows))
	}

	if fp.StorePath == "" {
		fp.StorePath = defaultStorePath
	}

	if fp.ServerVersionPrefix == "" {
		fp.ServerVersionPrefix = defaultServerVersionPrefix
	}

	if fp.LengthOfQueryPrinted == 0 {
		fp.LengthOfQueryPrinted = int64(defaultLengthOfQueryPrinted)
	}

	if fp.BatchSizeInLoadData == 0 {
		fp.BatchSizeInLoadData = int64(defaultBatchSizeInLoadData)
	}

	if fp.LoadDataConcurrencyCount == 0 {
		fp.LoadDataConcurrencyCount = int64(defaultLoadDataConcurrencyCount)
	}

	if fp.MaxBytesInOutbufToFlush == 0 {
		fp.MaxBytesInOutbufToFlush = int64(defaultMaxBytesInOutbufToFlush)
	}

	if fp.PrintLogInterVal == 0 {
		fp.PrintLogInterVal = int64(defaultPrintLogInterVal)
	}

	if fp.ExportDataDefaultFlushSize == 0 {
		fp.ExportDataDefaultFlushSize = int64(defaultExportDataDefaultFlushSize)
	}

	if fp.PortOfRpcServerInComputationEngine == 0 {
		fp.PortOfRpcServerInComputationEngine = int64(defaultPortOfRpcServerInComputationEngine)
	}

	if fp.DNReplicaID == 0 {
		fp.DNReplicaID = uint64(defaultDNReplicaID)
	}

	if fp.LogShardID == 0 {
		fp.LogShardID = uint64(defaultLogShardID)
	}

	if fp.SessionTimeout.Duration == 0 {
		fp.SessionTimeout.Duration = defaultSessionTimeout
	}

	if fp.SaveQueryResult == "" {
		fp.SaveQueryResult = "off"
	}

	if fp.QueryResultTimeout == 0 {
		fp.QueryResultTimeout = 24
	}

	if fp.QueryResultMaxsize == 0 {
		fp.QueryResultMaxsize = 100
	}

	if fp.AutoIncrCacheSize == 0 {
		fp.AutoIncrCacheSize = 3000
	}

	if fp.LowerCaseTableNames == "" {
		fp.LowerCaseTableNames = "1"
	}

	if fp.PrintDebugInterval == 0 {
		fp.PrintDebugInterval = defaultPrintDebugInterval
	}

	if fp.KillRountinesInterval == 0 {
		fp.KillRountinesInterval = defaultKillRountinesInterval
	}

	if fp.CleanKillQueueInterval == 0 {
		fp.CleanKillQueueInterval = defaultCleanKillQueueInterval
	}
}

func (fp *FrontendParameters) SetMaxMessageSize(size uint64) {
	fp.MaxMessageSize = size
}

func (fp *FrontendParameters) SetLogAndVersion(log *logutil.LogConfig, version string) {
	fp.LogLevel = log.Level
	fp.LogFormat = log.Format
	fp.LogFilename = log.Filename
	fp.LogMaxSize = int64(log.MaxSize)
	fp.LogMaxDays = int64(log.MaxDays)
	fp.LogMaxBackups = int64(log.MaxBackups)
	fp.MoVersion = version
}

func (fp *FrontendParameters) GetUnixSocketAddress() string {
	if fp.UnixSocketAddress == "" {
		return ""
	}
	ok, err := isFileExist(fp.UnixSocketAddress)
	if err != nil || ok {
		return ""
	}

	canCreate := func() string {
		f, err := os.Create(fp.UnixSocketAddress)
		if err != nil {
			return ""
		}
		if err := f.Close(); err != nil {
			panic(err)
		}
		if err := os.Remove(fp.UnixSocketAddress); err != nil {
			panic(err)
		}
		return fp.UnixSocketAddress
	}

	rootPath := filepath.Dir(fp.UnixSocketAddress)
	f, err := os.Open(rootPath)
	if os.IsNotExist(err) {
		err := os.MkdirAll(rootPath, 0755)
		if err != nil {
			return ""
		}
		return canCreate()
	} else if err != nil {
		return ""
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	return canCreate()
}

func isFileExist(file string) (bool, error) {
	f, err := os.Open(file)
	if err == nil {
		return true, f.Close()
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// ObservabilityParameters hold metric/trace switch
type ObservabilityParameters struct {
	// MoVersion, see SetDefaultValues
	MoVersion string

	// Host listening ip. normally same as FrontendParameters.Host
	Host string `toml:"host"`

	// StatusPort defines which port the mo status server (for metric etc.) listens on and clients connect to
	// Start listen with EnableMetricToProm is true.
	StatusPort int64 `toml:"statusPort"`

	// EnableMetricToProm default is false. if true, metrics can be scraped through host:status/metrics endpoint
	EnableMetricToProm bool `toml:"enableMetricToProm"`

	// DisableMetric default is false. if false, enable metric at booting
	DisableMetric bool `toml:"disableMetric"`

	// DisableTrace default is false. if false, enable trace at booting
	DisableTrace bool `toml:"disableTrace"`

	// EnableTraceDebug default is FileService. if InternalExecutor, use internal sql executor, FileService will implement soon.
	BatchProcessor string `toml:"batchProcessor"`

	// EnableTraceDebug default is false. With true, system will check all the children span is ended, which belong to the closing span.
	EnableTraceDebug bool `toml:"enableTraceDebug"`

	// TraceExportInterval default is 15s.
	TraceExportInterval int `toml:"traceExportInterval"`

	// LongQueryTime default is 0.0 sec. if 0.0f, record every query. Record with exec time longer than LongQueryTime.
	LongQueryTime float64 `toml:"longQueryTime"`

	// MetricMultiTable default is false. With true, save all metric data in one table.
	MetricMultiTable bool `toml:"metricMultiTable"`

	// MetricExportInterval default is 15 sec.
	MetricExportInterval int `toml:"metricExportInterval"`

	// MetricGatherInterval default is 15 sec.
	MetricGatherInterval int `toml:"metricGatherInterval"`

	// MetricUpdateStorageUsageInterval, default: 30 min
	MetricUpdateStorageUsageInterval toml.Duration `toml:"metricUpdateStorageUsageInterval"`

	// MergeCycle default: 14400 sec (4 hours).
	// PS: only used while MO init.
	MergeCycle toml.Duration `toml:"mergeCycle"`

	// MergeMaxFileSize default: 128 (MB)
	MergeMaxFileSize int `toml:"mergeMaxFileSize"`

	// PathBuilder default: DBTable. Support val in [DBTable, AccountDate]
	PathBuilder string `toml:"pathBuilder"`

	// LogsExtension default: tae. Support val in [csv, tae]
	LogsExtension string `toml:"logsExtension"`

	// MergedExtension default: tae. Support val in [csv, tae]
	MergedExtension string `toml:"mergedExtension"`

	OBCollectorConfig
}

func (op *ObservabilityParameters) SetDefaultValues(version string) {
	op.OBCollectorConfig.SetDefaultValues()

	op.MoVersion = version

	if op.Host == "" {
		op.Host = defaultHost
	}

	if op.StatusPort == 0 {
		op.StatusPort = int64(defaultStatusPort)
	}

	if op.BatchProcessor == "" {
		op.BatchProcessor = defaultBatchProcessor
	}

	if op.TraceExportInterval <= 0 {
		op.TraceExportInterval = defaultTraceExportInterval
	}

	if op.MetricExportInterval <= 0 {
		op.MetricExportInterval = defaultMetricExportInterval
	}

	if op.MetricGatherInterval <= 0 {
		op.MetricGatherInterval = defaultMetricGatherInterval
	}

	if op.MetricUpdateStorageUsageInterval.Duration <= 0 {
		op.MetricUpdateStorageUsageInterval.Duration = defaultMetricUpdateStorageUsageInterval
	}

	if op.MergeCycle.Duration <= 0 {
		op.MergeCycle.Duration = defaultMergeCycle
	}

	if op.PathBuilder == "" {
		op.PathBuilder = defaultPathBuilder
	}

	if op.MergeMaxFileSize <= 0 {
		op.MergeMaxFileSize = defaultMaxFileSize
	}

	if op.LogsExtension == "" {
		op.LogsExtension = defaultLogsExtension
	}

	if op.MergedExtension == "" {
		op.MergedExtension = defaultMergedExtension
	}
}

type OBCollectorConfig struct {
	ShowStatsInterval toml.Duration `toml:"showStatsInterval"`
	// BufferCnt
	BufferCnt  int32 `toml:"bufferCnt"`
	BufferSize int64 `toml:"bufferSize"`
}

func (c *OBCollectorConfig) SetDefaultValues() {
	if c.ShowStatsInterval.Duration == 0 {
		c.ShowStatsInterval.Duration = defaultOBShowStatsInterval
	}
	if c.BufferCnt == 0 {
		c.BufferCnt = defaultOBBufferCnt
	}
	if c.BufferSize == 0 {
		c.BufferSize = defaultOBBufferSize
	}
}

type ParameterUnit struct {
	SV *FrontendParameters

	//Storage Engine
	StorageEngine engine.Engine

	//TxnClient
	TxnClient client.TxnClient

	//Cluster Nodes
	ClusterNodes engine.Nodes

	// FileService
	FileService fileservice.FileService

	// LockService instance
	LockService lockservice.LockService

	// HAKeeper client, which is used to get connection ID
	// from HAKeeper currently.
	HAKeeperClient logservice.CNHAKeeperClient
}

func NewParameterUnit(
	sv *FrontendParameters,
	storageEngine engine.Engine,
	txnClient client.TxnClient,
	clusterNodes engine.Nodes,
) *ParameterUnit {
	return &ParameterUnit{
		SV:            sv,
		StorageEngine: storageEngine,
		TxnClient:     txnClient,
		ClusterNodes:  clusterNodes,
	}
}

// GetParameterUnit gets the configuration from the context.
func GetParameterUnit(ctx context.Context) *ParameterUnit {
	pu := ctx.Value(ParameterUnitKey).(*ParameterUnit)
	if pu == nil {
		panic("parameter unit is invalid")
	}
	return pu
}
