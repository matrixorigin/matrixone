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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type ConfigurationKeyType int

const (
	ParameterUnitKey ConfigurationKeyType = 1
)

var (

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

	defaultServerVersionPrefix = "8.0.30-MatrixOne-v"

	defaultVersionComment = "MatrixOne"

	//the length of query printed into console. -1, complete string. 0, empty string. >0 , length of characters at the header of the string.
	defaultLengthOfQueryPrinted = 1024

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

	// defaultTraceExportInterval default: 15 sec.
	defaultTraceExportInterval = 15

	// defaultMetricExportInterval default: 15 sec.
	defaultMetricExportInterval = 15

	// defaultLogShardID default: 1
	defaultLogShardID = 1

	// defaultTNReplicaID default: 1
	defaultTNReplicaID = 1
	// defaultMetricGatherInterval default: 15 sec.
	defaultMetricGatherInterval = 15

	// defaultMetricInternalGatherInterval default: 1 min.
	defaultMetricInternalGatherInterval = time.Minute

	// defaultMetricUpdateStorageUsageInterval default: 15 min.
	defaultMetricUpdateStorageUsageInterval = 15 * time.Minute

	// defaultMetricStorageUsageCheckNewInterval default: 1 min
	defaultMetricStorageUsageCheckNewInterval = time.Minute

	// defaultMergeCycle default: 5 minute
	defaultMergeCycle = 5 * time.Minute

	// defaultSessionTimeout default: 24 hour
	defaultSessionTimeout = 24 * time.Hour

	// defaultWaitTimeoutMin default: 1 second
	defaultWaitTimeoutMin int64 = 1
	// defaultWaitTimeoutMax default: 24 hours (in seconds)
	defaultWaitTimeoutMax int64 = 24 * 60 * 60

	// defaultInteractiveTimeoutMin default: 1 second
	defaultInteractiveTimeoutMin int64 = 1
	// defaultInteractiveTimeoutMax default: 24 hours (in seconds)
	defaultInteractiveTimeoutMax int64 = 24 * 60 * 60

	// defaultNetReadTimeout default: 0 (no timeout for normal operations)
	defaultNetReadTimeout = time.Duration(0)

	// defaultNetWriteTimeout default: 0 (no timeout for normal operations)
	defaultNetWriteTimeout = time.Duration(0)

	// defaultLoadLocalReadTimeout default: 60 seconds
	// Timeout for reading data from client during LOAD DATA LOCAL operations
	// Used to detect F5/LoadBalancer idle timeout disconnections
	defaultLoadLocalReadTimeout = 60 * time.Second

	// defaultLoadLocalWriteTimeout default: 60 seconds
	// Timeout for writing data to client during LOAD DATA LOCAL operations
	defaultLoadLocalWriteTimeout = 60 * time.Second

	// defaultOBShowStatsInterval default: 1min
	defaultOBShowStatsInterval = time.Minute

	// defaultOBMaxBufferCnt
	defaultOBBufferCnt int32 = -1

	//defaultOBBufferSize, 10 << 20 = 10485760
	defaultOBBufferSize int64 = 10485760

	// defaultOBCollectorCntPercent
	defaultOBCollectorCntPercent = 1000
	// defaultOBGeneratorCntPercent
	defaultOBGeneratorCntPercent = 1000
	// defaultOBExporterCntPercent
	defaultOBExporterCntPercent = 1000

	// defaultPrintDebugInterval default: 30 minutes
	defaultPrintDebugInterval = 30

	// defaultKillRountinesInterval default: 10 seconds
	defaultKillRountinesInterval = 10

	//defaultCleanKillQueueInterval default: 60 minutes
	defaultCleanKillQueueInterval = 60

	// defaultLongSpanTime default: 10 s
	defaultLongSpanTime = 10 * time.Second

	defaultAggregationWindow = 5 * time.Second

	defaultSelectThreshold = 200 * time.Millisecond

	// defaultLongQueryTime
	defaultLongQueryTime = 1.0
	// defaultSkipRunningStmt
	defaultSkipRunningStmt = true
	// defaultLoggerLabelKey and defaultLoggerLabelVal
	defaultLoggerLabelKey = "role"
	defaultLoggerLabelVal = "logging_cn"
	defaultLoggerMap      = map[string]string{defaultLoggerLabelKey: defaultLoggerLabelVal}

	defaultMaxLogMessageSize = 16 << 10

	// largestEntryLimit is the max size for reading file to csv buf
	LargestEntryLimit = 10 * 1024 * 1024

	CNPrimaryCheck atomic.Bool

	defaultCreateTxnOpTimeout = 2 * time.Minute

	defaultConnectTimeout = time.Minute
)

// FrontendParameters of the frontend
type FrontendParameters struct {
	MoVersion string

	//port defines which port the mo-server listens on and clients connect to
	Port int64 `toml:"port" user_setting:"basic"`

	//listening ip
	Host string `toml:"host" user_setting:"basic"`

	// UnixSocketAddress listening unix domain socket
	UnixSocketAddress string `toml:"unix-socket" user_setting:"advanced"`

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
	ServerVersionPrefix string `toml:"serverVersionPrefix"`

	VersionComment string `toml:"versionComment"`

	//the length of query printed into console. -1, complete string. 0, empty string. >0 , length of characters at the header of the string.
	LengthOfQueryPrinted int64 `toml:"lengthOfQueryPrinted" user_setting:"advanced"`

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

	//default is false. With true. Server will support tls
	EnableTls bool `toml:"enableTls" user_setting:"advanced"`

	//default is ''. Path of file that contains list of trusted SSL CAs for client
	TlsCaFile string `toml:"tlsCaFile" user_setting:"advanced"`

	//default is ''. Path of file that contains X509 certificate in PEM format for client
	TlsCertFile string `toml:"tlsCertFile" user_setting:"advanced"`

	//default is ''. Path of file that contains X509 key in PEM format for client
	TlsKeyFile string `toml:"tlsKeyFile" user_setting:"advanced"`

	//default is 1
	LogShardID uint64 `toml:"logshardid"`

	//default is 1
	TNReplicaID uint64 `toml:"tnreplicalid"`

	EnableDoComQueryInProgress bool `toml:"comQueryInProgress"`

	//timeout of the session. the default is 10minutes
	SessionTimeout toml.Duration `toml:"sessionTimeout"`

	// NetReadTimeout is the timeout for reading from the network connection. Default is 0 (no timeout).
	NetReadTimeout toml.Duration `toml:"netReadTimeout"`

	// NetWriteTimeout is the timeout for writing to the network connection. Default is 60 seconds.
	NetWriteTimeout toml.Duration `toml:"netWriteTimeout"`

	// WaitTimeoutMin is the hard minimum for wait_timeout (seconds).
	WaitTimeoutMin int64 `toml:"waitTimeoutMin"`
	// WaitTimeoutMax is the hard maximum for wait_timeout (seconds).
	WaitTimeoutMax int64 `toml:"waitTimeoutMax"`
	// InteractiveTimeoutMin is the hard minimum for interactive_timeout (seconds).
	InteractiveTimeoutMin int64 `toml:"interactiveTimeoutMin"`
	// InteractiveTimeoutMax is the hard maximum for interactive_timeout (seconds).
	InteractiveTimeoutMax int64 `toml:"interactiveTimeoutMax"`

	// LoadLocalReadTimeout is the timeout for reading data from client during LOAD DATA LOCAL operations.
	// Used to detect F5/LoadBalancer idle timeout disconnections. Default is 60 seconds.
	LoadLocalReadTimeout toml.Duration `toml:"loadLocalReadTimeout"`

	// LoadLocalWriteTimeout is the timeout for writing data to client during LOAD DATA LOCAL operations.
	// Default is 60 seconds.
	LoadLocalWriteTimeout toml.Duration `toml:"loadLocalWriteTimeout"`

	// MaxMessageSize max size for read messages from dn. Default is 10M
	MaxMessageSize uint64 `toml:"max-message-size"`

	// default off
	SaveQueryResult string `toml:"saveQueryResult" user_setting:"advanced"`

	// default 24 (h)
	QueryResultTimeout uint64 `toml:"queryResultTimeout" user_setting:"advanced"`

	// default 100 (MB)
	QueryResultMaxsize uint64 `toml:"queryResultMaxsize" user_setting:"advanced"`

	AutoIncrCacheSize uint64 `toml:"autoIncrCacheSize"`

	PrintDebug bool `toml:"printDebug"`

	PrintDebugInterval int `toml:"printDebugInterval"`

	// Interval in seconds
	KillRountinesInterval int `toml:"killRountinesInterval"`

	CleanKillQueueInterval int `toml:"cleanKillQueueInterval"`

	// ProxyEnabled indicates that proxy module is enabled and something extra
	// is needed, such as update the salt.
	ProxyEnabled bool `toml:"proxy-enabled"`

	// SkipCheckPrivilege denotes the privilege check should be passed.
	SkipCheckPrivilege bool `toml:"skipCheckPrivilege"`

	// skip checking the password of the user
	SkipCheckUser bool `toml:"skipCheckUser"`

	// disable select into
	DisableSelectInto bool `toml:"disable-select-into"`

	// PubAllAccounts shows the accounts which can publish data to all accounts
	// consists of account names are separated by comma
	PubAllAccounts string `toml:"pub-all-accounts"`

	// KeyEncryptionKey is the key for encrypt key
	KeyEncryptionKey string `toml:"key-encryption-key"`

	// timeout of create txn.
	// txnclient.New
	// txnclient.RestartTxn
	// engine.New
	CreateTxnOpTimeout toml.Duration `toml:"createTxnOpTimeout" user_setting:"advanced"`

	// timeout of authenticating user. different from session timeout
	// including mysql protocol handshake, checking user, loading session variables
	ConnectTimeout toml.Duration `toml:"connectTimeout" user_setting:"advanced"`
}

func (fp *FrontendParameters) SetDefaultValues() {

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

	if fp.ServerVersionPrefix == "" {
		fp.ServerVersionPrefix = defaultServerVersionPrefix
	}

	if fp.VersionComment == "" {
		fp.VersionComment = defaultVersionComment
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

	if fp.TNReplicaID == 0 {
		fp.TNReplicaID = uint64(defaultTNReplicaID)
	}

	if fp.LogShardID == 0 {
		fp.LogShardID = uint64(defaultLogShardID)
	}

	if fp.SessionTimeout.Duration == 0 {
		fp.SessionTimeout.Duration = defaultSessionTimeout
	}

	if fp.NetReadTimeout.Duration == 0 {
		fp.NetReadTimeout.Duration = defaultNetReadTimeout
	}

	if fp.NetWriteTimeout.Duration == 0 {
		fp.NetWriteTimeout.Duration = defaultNetWriteTimeout
	}

	if fp.WaitTimeoutMin == 0 {
		fp.WaitTimeoutMin = defaultWaitTimeoutMin
	}
	if fp.WaitTimeoutMax == 0 {
		fp.WaitTimeoutMax = defaultWaitTimeoutMax
	}
	if fp.InteractiveTimeoutMin == 0 {
		fp.InteractiveTimeoutMin = defaultInteractiveTimeoutMin
	}
	if fp.InteractiveTimeoutMax == 0 {
		fp.InteractiveTimeoutMax = defaultInteractiveTimeoutMax
	}

	if fp.LoadLocalReadTimeout.Duration == 0 {
		fp.LoadLocalReadTimeout.Duration = defaultLoadLocalReadTimeout
	}

	if fp.LoadLocalWriteTimeout.Duration == 0 {
		fp.LoadLocalWriteTimeout.Duration = defaultLoadLocalWriteTimeout
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
		fp.AutoIncrCacheSize = 3000000
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

	if len(fp.KeyEncryptionKey) == 0 {
		fp.KeyEncryptionKey = "JlxRbXjFGnCsvbsFQSJFvhMhDLaAXq5y"
	}

	if fp.CreateTxnOpTimeout.Duration == 0 {
		fp.CreateTxnOpTimeout.Duration = defaultCreateTxnOpTimeout
	}

	if fp.ConnectTimeout.Duration == 0 {
		fp.ConnectTimeout.Duration = defaultConnectTimeout
	}
}

func (fp *FrontendParameters) SetMaxMessageSize(size uint64) {
	fp.MaxMessageSize = size
}

func (fp *FrontendParameters) SetLogAndVersion(log *logutil.LogConfig, version string) {
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
		var f *os.File
		f, err = os.Create(fp.UnixSocketAddress)
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
		err = os.MkdirAll(rootPath, 0755)
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
	Host string `toml:"host" user_setting:"advanced"`

	// StatusPort defines which port the mo status server (for metric etc.) listens on and clients connect to
	// Start listen with EnableMetricToProm is true.
	StatusPort int `toml:"status-port" user_setting:"advanced"`

	// EnableMetricToProm default is false. if true, metrics can be scraped through host:status/metrics endpoint
	EnableMetricToProm bool `toml:"enable-metric-to-prom" user_setting:"advanced"`

	// DisableMetric default is false. if false, enable metric at booting
	DisableMetric bool `toml:"disable-metric" user_setting:"advanced"`

	// DisableTrace default is false. if false, enable trace at booting
	DisableTrace bool `toml:"disable-trace" user_setting:"advanced"`

	// EnableTraceDebug default is false. With true, system will check all the children span is ended, which belong to the closing span.
	EnableTraceDebug bool `toml:"enable-trace-debug"`

	// TraceExportInterval default is 15s.
	TraceExportInterval int `toml:"trace-export-interval"`

	// LongQueryTime default is 0.0 sec. if 0.0f, record every query. Record with exec time longer than LongQueryTime.
	LongQueryTime float64 `toml:"long-query-time" user_setting:"advanced"`

	// MetricExportInterval default is 15 sec.
	MetricExportInterval int `toml:"metric-export-interval"`

	// MetricGatherInterval default is 15 sec.
	MetricGatherInterval int `toml:"metric-gather-interval"`

	// MetricInternalGatherInterval default is 1 min, handle metric.SubSystemMO metric
	MetricInternalGatherInterval toml.Duration `toml:"metric-internal-gather-interval"`

	// MetricStorageUsageUpdateInterval, default: 15 min
	// old version ObservabilityOldParameters.MetricUpdateStorageUsageIntervalV12
	// tips: diff name
	MetricStorageUsageUpdateInterval toml.Duration `toml:"metric-storage-usage-update-interval"`

	// MetricStorageUsageCheckNewInterval, default: 1 min
	MetricStorageUsageCheckNewInterval toml.Duration `toml:"metric-storage-usage-check-new-interval"`

	// MergeCycle default: 300 sec (5 minutes).
	// PS: only used while MO init.
	MergeCycle toml.Duration `toml:"merge-cycle"`

	// DisableSpan default: false. Disable span collection
	DisableSpan bool `toml:"disable-span"`

	// EnableSpanProfile default: false. Do NO profile by default.
	EnableSpanProfile bool `toml:"enable-span-profile"`

	// DisableError default: false. Disable error collection
	DisableError bool `toml:"disable-error"`

	// LongSpanTime default: 500 ms. Only record span, which duration >= LongSpanTime
	LongSpanTime toml.Duration `toml:"long-span-time"`

	// SkipRunningStmt default: false. Skip status:Running entry while collect statement_info
	SkipRunningStmt bool `toml:"skip-running-stmt"`

	// If disabled, the logs will be written to files stored in s3
	DisableSqlWriter bool `toml:"disable-sql-writer"`

	// DisableStmtAggregation ctrl statement aggregation. If disabled, the statements will not be aggregated.
	// If false, LongQueryTime is NO less than SelectAggThreshold
	DisableStmtAggregation bool `toml:"disable-stmt-aggregation"`

	// Seconds to aggregate the statements
	AggregationWindow toml.Duration `toml:"aggregation-window"`

	// SelectAggThreshold Duration to filter statements for aggregation
	SelectAggThreshold toml.Duration `toml:"select-agg-threshold"`

	// Disable merge statements
	EnableStmtMerge bool `toml:"enable-stmt-merge"`

	// LabelSelector
	LabelSelector map[string]string `toml:"label-selector"`

	// TaskLabel
	TaskLabel      map[string]string `toml:"task-label"`
	ResetTaskLabel bool              `toml:"reset-task-label"`

	// estimate tcp network packet cost
	TCPPacket bool `toml:"tcp-packet"`

	// MaxLogMessageSize truncate the reset. default: 16 KiB
	MaxLogMessageSize toml.ByteSize `toml:"max-log-message-size"`

	// for cu calculation
	CU   OBCUConfig `toml:"cu"`
	CUv1 OBCUConfig `toml:"cu_v1"`

	OBCollectorConfig

	ObservabilityOldParameters
}

// ObservabilityOldParameters will remove after 1.3.0
// all item default false, 0, nil
type ObservabilityOldParameters struct {
	StatusPortV12         int  `toml:"statusPort" user_setting:"advanced"`
	EnableMetricToPromV12 bool `toml:"enableMetricToProm"`

	// part metric
	MetricUpdateStorageUsageIntervalV12 toml.Duration `toml:"metricUpdateStorageUsageInterval"` /* tips: rename */

	// part Trace
	DisableMetricV12 bool `toml:"disableMetric" user_setting:"advanced"`
	DisableTraceV12  bool `toml:"disableTrace"`
	DisableErrorV12  bool `toml:"disableError"`
	DisableSpanV12   bool `toml:"disableSpan"`

	// part statement_info
	EnableStmtMergeV12        bool          `toml:"enableStmtMerge"`
	DisableStmtAggregationV12 bool          `toml:"disableStmtAggregation"`
	AggregationWindowV12      toml.Duration `toml:"aggregationWindow"`
	SelectAggThresholdV12     toml.Duration `toml:"selectAggrThreshold"`
	LongQueryTimeV12          float64       `toml:"longQueryTime" user_setting:"advanced"`
	SkipRunningStmtV12        bool          `toml:"skipRunningStmt"`

	// part labelSelector
	LabelSelectorV12 map[string]string `toml:"labelSelector"`
}

func NewObservabilityParameters() *ObservabilityParameters {
	op := &ObservabilityParameters{
		MoVersion:                          "",
		Host:                               defaultHost,
		StatusPort:                         defaultStatusPort,
		EnableMetricToProm:                 false,
		DisableMetric:                      false,
		DisableTrace:                       false,
		EnableTraceDebug:                   false,
		TraceExportInterval:                defaultTraceExportInterval,
		LongQueryTime:                      defaultLongQueryTime,
		MetricExportInterval:               defaultMetricExportInterval,
		MetricGatherInterval:               defaultMetricGatherInterval,
		MetricInternalGatherInterval:       toml.Duration{},
		MetricStorageUsageUpdateInterval:   toml.Duration{},
		MetricStorageUsageCheckNewInterval: toml.Duration{},
		MergeCycle:                         toml.Duration{},
		DisableSpan:                        false,
		EnableSpanProfile:                  false,
		DisableError:                       false,
		LongSpanTime:                       toml.Duration{},
		SkipRunningStmt:                    defaultSkipRunningStmt,
		DisableSqlWriter:                   false,
		DisableStmtAggregation:             false,
		AggregationWindow:                  toml.Duration{},
		SelectAggThreshold:                 toml.Duration{},
		EnableStmtMerge:                    false,
		LabelSelector:                      map[string]string{}, /*default: role=logging_cn*/
		TaskLabel:                          map[string]string{},
		ResetTaskLabel:                     false,
		TCPPacket:                          true,
		MaxLogMessageSize:                  toml.ByteSize(defaultMaxLogMessageSize),
		CU:                                 *NewOBCUConfig(),
		CUv1:                               *NewOBCUConfig(),
		OBCollectorConfig:                  *NewOBCollectorConfig(),
		//ObservabilityOldParameters // default as false/0/nil
	}
	op.MetricInternalGatherInterval.Duration = defaultMetricInternalGatherInterval
	op.MetricStorageUsageUpdateInterval.Duration = defaultMetricUpdateStorageUsageInterval
	op.MetricStorageUsageCheckNewInterval.Duration = defaultMetricStorageUsageCheckNewInterval
	op.MergeCycle.Duration = defaultMergeCycle
	op.LongSpanTime.Duration = defaultLongSpanTime
	op.AggregationWindow.Duration = defaultAggregationWindow
	op.SelectAggThreshold.Duration = defaultSelectThreshold
	return op
}

func (op *ObservabilityParameters) SetDefaultValues(version string) {
	op.OBCollectorConfig.SetDefaultValues()
	op.CU.SetDefaultValues()
	op.CUv1.SetDefaultValues()

	op.MoVersion = version

	if op.Host == "" {
		op.Host = defaultHost
	}

	if op.StatusPort == 0 {
		op.StatusPort = defaultStatusPort
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

	if op.MetricStorageUsageUpdateInterval.Duration <= 0 {
		op.MetricStorageUsageUpdateInterval.Duration = defaultMetricUpdateStorageUsageInterval
	}

	if op.MetricStorageUsageCheckNewInterval.Duration <= 0 {
		op.MetricStorageUsageCheckNewInterval.Duration = defaultMetricStorageUsageCheckNewInterval
	}

	if op.MergeCycle.Duration <= 0 {
		op.MergeCycle.Duration = defaultMergeCycle
	}

	if op.LongSpanTime.Duration <= 0 {
		op.LongSpanTime.Duration = defaultLongSpanTime
	}

	if op.AggregationWindow.Duration <= 0 {
		op.AggregationWindow.Duration = defaultAggregationWindow
	}

	if op.SelectAggThreshold.Duration <= 0 {
		op.SelectAggThreshold.Duration = defaultSelectThreshold
	}

	if len(op.LabelSelector) == 0 {
		op.LabelSelector = make(map[string]string)
		for k, v := range defaultLoggerMap {
			op.LabelSelector[k] = v
		}
	}

	// reset by old config
	// should before calculated logic
	op.resetConfigByOld()

	// ===========================
	// calculated logic
	// ===========================

	// this loop must after SelectAggThreshold and DisableStmtAggregation
	if !op.DisableStmtAggregation {
		val := float64(op.SelectAggThreshold.Duration) / float64(time.Second)
		if op.LongQueryTime <= val {
			op.LongQueryTime = val
		}
	}

}

// resetConfigByOld reset the ObservabilityParameters by ObservabilityOldParameters, which all default false, or nil, or 0.
func (op *ObservabilityParameters) resetConfigByOld() {
	resetIntConfig := func(target *int, defaultVal int, setVal int) {
		if *target == defaultVal && setVal > 0 {
			*target = setVal
		}
	}
	resetBoolConfig := func(target *bool, defaultVal bool, setVal bool) {
		if *target == defaultVal && setVal {
			*target = setVal
		}
	}
	resetDurationConfig := func(target *time.Duration, defaultVal time.Duration, setVal time.Duration) {
		if *target == defaultVal && setVal > 0 {
			*target = setVal
		}
	}
	resetFloat64Config := func(target *float64, defaultVal float64, setVal float64) {
		if *target == defaultVal && setVal > 0 {
			*target = setVal
		}
	}
	resetMapConfig := func(target map[string]string, defaultVal map[string]string, setVal map[string]string) {
		eq := len(target) == len(defaultVal)
		// check eq
		if eq {
			for k, v := range defaultVal {
				if target[k] != v {
					eq = false
					break
				}
			}
		}
		if eq {
			for k := range target {
				delete(target, k)
			}
			for k, v := range setVal {
				target[k] = v
			}

		}
	}
	// port prom-export
	resetIntConfig(&op.StatusPort, defaultStatusPort, op.StatusPortV12)
	resetBoolConfig(&op.EnableMetricToProm, false, op.EnableMetricToPromV12)
	resetBoolConfig(&op.DisableMetric, false, op.DisableMetricV12)
	resetBoolConfig(&op.DisableTrace, false, op.DisableTraceV12)
	resetBoolConfig(&op.DisableError, false, op.DisableErrorV12)
	resetBoolConfig(&op.DisableSpan, false, op.DisableSpanV12)
	// part metric
	resetDurationConfig(&op.MetricStorageUsageUpdateInterval.Duration,
		defaultMetricUpdateStorageUsageInterval,
		op.MetricUpdateStorageUsageIntervalV12.Duration)
	// part statement_info
	resetBoolConfig(&op.EnableStmtMerge, false, op.EnableStmtMergeV12)
	resetBoolConfig(&op.DisableStmtAggregation, false, op.DisableStmtAggregationV12)
	resetDurationConfig(&op.AggregationWindow.Duration, defaultAggregationWindow, op.AggregationWindowV12.Duration)
	resetDurationConfig(&op.SelectAggThreshold.Duration, defaultSelectThreshold, op.SelectAggThresholdV12.Duration)
	resetFloat64Config(&op.LongQueryTime, defaultLongQueryTime, op.LongQueryTimeV12)
	resetBoolConfig(&op.SkipRunningStmt, defaultSkipRunningStmt, op.SkipRunningStmtV12)
	// part labelSelector
	resetMapConfig(op.LabelSelector, defaultLoggerMap, op.LabelSelectorV12)
}

type OBCollectorConfig struct {
	ShowStatsInterval toml.Duration `toml:"showStatsInterval"`
	// BufferCnt
	BufferCnt  int32 `toml:"bufferCnt"`
	BufferSize int64 `toml:"bufferSize"`
	// Collector Worker

	CollectorCntPercent int `toml:"collector_cnt_percent"`
	GeneratorCntPercent int `toml:"generator_cnt_percent"`
	ExporterCntPercent  int `toml:"exporter_cnt_percent"`
}

func NewOBCollectorConfig() *OBCollectorConfig {
	cfg := &OBCollectorConfig{
		ShowStatsInterval:   toml.Duration{},
		BufferCnt:           defaultOBBufferCnt,
		BufferSize:          defaultOBBufferSize,
		CollectorCntPercent: defaultOBCollectorCntPercent,
		GeneratorCntPercent: defaultOBGeneratorCntPercent,
		ExporterCntPercent:  defaultOBExporterCntPercent,
	}
	cfg.ShowStatsInterval.Duration = defaultOBShowStatsInterval
	return cfg
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
	if c.CollectorCntPercent <= 0 {
		c.CollectorCntPercent = defaultOBCollectorCntPercent
	}
	if c.GeneratorCntPercent <= 0 {
		c.GeneratorCntPercent = defaultOBGeneratorCntPercent
	}
	if c.ExporterCntPercent <= 0 {
		c.ExporterCntPercent = defaultOBExporterCntPercent
	}
}

type OBCUConfig struct {
	// cu unit
	CUUnit float64 `toml:"cu_unit"`
	// price
	CpuPrice   float64 `toml:"cpu_price"`
	MemPrice   float64 `toml:"mem_price"`
	IoInPrice  float64 `toml:"io_in_price"`
	IoOutPrice float64 `toml:"io_out_price"`
	// IoListPrice default value: IoInPrice
	// NOT allow 0 value.
	IoListPrice float64 `toml:"io_list_price"`
	// IoDeletePrice default value: IoInPrice, cc SetDefaultValues
	// The only one ALLOW 0 value.
	IoDeletePrice float64 `toml:"io_delete_price"`
	TrafficPrice0 float64 `toml:"traffic_price_0"`
	TrafficPrice1 float64 `toml:"traffic_price_1"`
	TrafficPrice2 float64 `toml:"traffic_price_2"`
}

const CUUnitDefault = 1.002678e-06
const CUCpuPriceDefault = 3.45e-14
const CUMemPriceDefault = 4.56e-24
const CUIOInPriceDefault = 5.67e-06
const CUIOOutPriceDefault = 6.78e-06
const CUTrafficPrice0Default = 7.89e-10
const CUTrafficPrice1Default = 7.89e-10
const CUTrafficPrice2Default = 7.89e-10

func NewOBCUConfig() *OBCUConfig {
	cfg := &OBCUConfig{
		CUUnit:        CUUnitDefault,
		CpuPrice:      CUCpuPriceDefault,
		MemPrice:      CUMemPriceDefault,
		IoInPrice:     CUIOInPriceDefault,
		IoOutPrice:    CUIOOutPriceDefault,
		IoListPrice:   -1, // default as OBCUConfig.IoInPrice
		IoDeletePrice: -1, // default as OBCUConfig.IoInPrice
		TrafficPrice0: CUTrafficPrice0Default,
		TrafficPrice1: CUTrafficPrice1Default,
		TrafficPrice2: CUTrafficPrice2Default,
	}
	return cfg
}

func (c *OBCUConfig) SetDefaultValues() {
	if c.CUUnit <= 0 {
		c.CUUnit = CUUnitDefault
	}
	if c.CpuPrice < 0 {
		c.CpuPrice = CUCpuPriceDefault
	}
	if c.MemPrice < 0 {
		c.MemPrice = CUMemPriceDefault
	}
	if c.IoInPrice < 0 {
		c.IoInPrice = CUIOInPriceDefault
	}
	if c.IoOutPrice < 0 {
		c.IoOutPrice = CUIOOutPriceDefault
	}
	// default as c.IoInPrice
	if c.IoListPrice < 0 {
		c.IoListPrice = c.IoInPrice
	}
	// default as c.IoInPrice, allow value: 0
	if c.IoDeletePrice < 0 {
		c.IoDeletePrice = c.IoInPrice
	}
	if c.TrafficPrice0 < 0 {
		c.TrafficPrice0 = CUTrafficPrice0Default
	}
	if c.TrafficPrice1 < 0 {
		c.TrafficPrice1 = CUTrafficPrice1Default
	}
	if c.TrafficPrice2 < 0 {
		c.TrafficPrice2 = CUTrafficPrice2Default
	}
}

type ParameterUnit struct {
	sync.RWMutex

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

	// QueryClient instance
	QueryClient qclient.QueryClient

	UdfService udf.Service

	// HAKeeper client, which is used to get connection ID
	// from HAKeeper currently.
	HAKeeperClient logservice.CNHAKeeperClient

	TaskService taskservice.TaskService

	CNMemoryThrottler rscthrottler.RSCThrottler
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

func (p *ParameterUnit) SetTaskService(taskService taskservice.TaskService) {
	p.Lock()
	defer p.Unlock()
	p.TaskService = taskService
}

func (p *ParameterUnit) GetTaskService() taskservice.TaskService {
	p.RLock()
	defer p.RUnlock()
	return p.TaskService
}
