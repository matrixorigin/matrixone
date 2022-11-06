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
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
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
	defaultUnixAddr = "/var/lib/mysql/mysql.sock"

	//host mmu limitation. 1 << 40 = 1099511627776
	defaultHostMmuLimitation = 1099511627776

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

	// defaultMergeCycle default: 4 hours
	defaultMergeCycle = 4 * time.Hour

	// defaultPathBuilder, val in [DBTable, AccountDate]
	defaultPathBuilder = "AccountDate"

	// defaultSessionTimeout default: 10 minutes
	defaultSessionTimeout = 10 * time.Minute
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

	//listening unix domain socket
	UAddr string `toml:"UAddr"`

	//host mmu limitation. default: 1 << 40 = 1099511627776
	HostMmuLimitation int64 `toml:"hostMmuLimitation"`

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

	//record the time elapsed of executing sql request
	DisableRecordTimeElapsedOfSqlRequest bool `toml:"DisableRecordTimeElapsedOfSqlRequest"`

	//the root directory of the storage and matrixcube's data. The actual dir is cubeDirPrefix + nodeID
	StorePath string `toml:"storePath"`

	//the length of query printed into console. -1, complete string. 0, empty string. >0 , length of characters at the header of the string.
	LengthOfQueryPrinted int64 `toml:"lengthOfQueryPrinted"`

	//the count of rows in vector of batch in load data
	BatchSizeInLoadData int64 `toml:"batchSizeInLoadData"`

	//default is 4. The count of go routine writing batch into the storage.
	LoadDataConcurrencyCount int64 `toml:"loadDataConcurrencyCount"`

	//default is false. Skip writing batch into the storage
	LoadDataSkipWritingBatch bool `toml:"loadDataSkipWritingBatch"`

	//default is false. true for profiling the getDataFromPipeline
	EnableProfileGetDataFromPipeline bool `toml:"enableProfileGetDataFromPipeline"`

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

	//timeout of the session. the default is 10minutes
	SessionTimeout toml.Duration `toml:"sessionTimeout"`

	// MaxMessageSize max size for read messages from dn. Default is 10M
	MaxMessageSize uint64 `toml:"max-message-size"`
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

	if fp.UAddr == "" {
		fp.UAddr = defaultUnixAddr
	}

	if fp.HostMmuLimitation == 0 {
		fp.HostMmuLimitation = int64(defaultHostMmuLimitation)
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

	// MergeCycle default: 14400 sec (4 hours).
	// PS: only used while MO init.
	MergeCycle toml.Duration `toml:"mergeCycle"`

	// PathBuilder default: DBTable. Support val in [DBTable, AccountDate]
	PathBuilder string `toml:"PathBuilder"`
}

func (op *ObservabilityParameters) SetDefaultValues(version string) {
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

	if op.MergeCycle.Duration <= 0 {
		op.MergeCycle.Duration = defaultMergeCycle
	}

	if op.PathBuilder == "" {
		op.PathBuilder = defaultPathBuilder
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

	// GetClusterDetails
	GetClusterDetails engine.GetClusterDetailsFunc
}

func NewParameterUnit(
	sv *FrontendParameters,
	storageEngine engine.Engine,
	txnClient client.TxnClient,
	clusterNodes engine.Nodes,
	getClusterDetails engine.GetClusterDetailsFunc,
) *ParameterUnit {
	return &ParameterUnit{
		SV:                sv,
		StorageEngine:     storageEngine,
		TxnClient:         txnClient,
		ClusterNodes:      clusterNodes,
		GetClusterDetails: getClusterDetails,
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
