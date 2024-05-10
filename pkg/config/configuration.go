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

	// default lower_case_table_names
	defaultLowerCaseTableNames = "1"

	CNPrimaryCheck = false
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

	// MaxMessageSize max size for read messages from dn. Default is 10M
	MaxMessageSize uint64 `toml:"max-message-size"`

	// default off
	SaveQueryResult string `toml:"saveQueryResult" user_setting:"advanced"`

	// default 24 (h)
	QueryResultTimeout uint64 `toml:"queryResultTimeout" user_setting:"advanced"`

	// default 100 (MB)
	QueryResultMaxsize uint64 `toml:"queryResultMaxsize" user_setting:"advanced"`

	AutoIncrCacheSize uint64 `toml:"autoIncrCacheSize"`

	LowerCaseTableNames string `toml:"lowerCaseTableNames" user_setting:"advanced"`

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

	if fp.LowerCaseTableNames == "" {
		fp.LowerCaseTableNames = defaultLowerCaseTableNames
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
	Host string `toml:"host" user_setting:"advanced"`

	// StatusPort defines which port the mo status server (for metric etc.) listens on and clients connect to
	// Start listen with EnableMetricToProm is true.
	StatusPort int `toml:"statusPort" user_setting:"advanced"`

	// EnableMetricToProm default is false. if true, metrics can be scraped through host:status/metrics endpoint
	EnableMetricToProm bool `toml:"enableMetricToProm" user_setting:"advanced"`

	// DisableMetric default is false. if false, enable metric at booting
	DisableMetric bool `toml:"disableMetric" user_setting:"advanced"`

	// DisableTrace default is false. if false, enable trace at booting
	DisableTrace bool `toml:"disableTrace" user_setting:"advanced"`

	// EnableTraceDebug default is false. With true, system will check all the children span is ended, which belong to the closing span.
	EnableTraceDebug bool `toml:"enableTraceDebug"`

	// TraceExportInterval default is 15s.
	TraceExportInterval int `toml:"traceExportInterval"`

	// LongQueryTime default is 0.0 sec. if 0.0f, record every query. Record with exec time longer than LongQueryTime.
	LongQueryTime float64 `toml:"longQueryTime" user_setting:"advanced"`

	// MetricExportInterval default is 15 sec.
	MetricExportInterval int `toml:"metric-export-interval"`

	// MetricGatherInterval default is 15 sec.
	MetricGatherInterval int `toml:"metric-gather-interval"`

	// MetricInternalGatherInterval default is 1 min, handle metric.SubSystemMO metric
	MetricInternalGatherInterval toml.Duration `toml:"metric-internal-gather-interval"`

	// MetricStorageUsageUpdateInterval, default: 15 min
	MetricStorageUsageUpdateInterval toml.Duration `toml:"metricStorageUsageUpdateInterval"`

	// MetricStorageUsageCheckNewInterval, default: 1 min
	MetricStorageUsageCheckNewInterval toml.Duration `toml:"metricStorageUsageCheckNewInterval"`

	// MergeCycle default: 300 sec (5 minutes).
	// PS: only used while MO init.
	MergeCycle toml.Duration `toml:"mergeCycle"`

	// DisableSpan default: false. Disable span collection
	DisableSpan bool `toml:"disableSpan"`

	// DisableError default: false. Disable error collection
	DisableError bool `toml:"disableError"`

	// LongSpanTime default: 500 ms. Only record span, which duration >= LongSpanTime
	LongSpanTime toml.Duration `toml:"longSpanTime"`

	// SkipRunningStmt default: false. Skip status:Running entry while collect statement_info
	SkipRunningStmt bool `toml:"skipRunningStmt"`

	// If disabled, the logs will be written to files stored in s3
	DisableSqlWriter bool `toml:"disableSqlWriter"`

	// DisableStmtAggregation ctrl statement aggregation. If disabled, the statements will not be aggregated.
	// If false, LongQueryTime is NO less than SelectAggrThreshold
	DisableStmtAggregation bool `toml:"disableStmtAggregation"`

	// Seconds to aggregate the statements
	AggregationWindow toml.Duration `toml:"aggregationWindow"`

	// SelectAggrThreshold Duration to filter statements for aggregation
	SelectAggrThreshold toml.Duration `toml:"selectAggrThreshold"`

	// Disable merge statements
	EnableStmtMerge bool `toml:"enableStmtMerge"`

	// LabelSelector
	LabelSelector map[string]string `toml:"labelSelector"`

	// estimate tcp network packet cost
	TCPPacket bool `toml:"tcpPacket"`

	// for cu calculation
	CU   OBCUConfig `toml:"cu"`
	CUv1 OBCUConfig `toml:"cu_v1"`

	OBCollectorConfig
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
		DisableError:                       false,
		LongSpanTime:                       toml.Duration{},
		SkipRunningStmt:                    defaultSkipRunningStmt,
		DisableSqlWriter:                   false,
		DisableStmtAggregation:             false,
		AggregationWindow:                  toml.Duration{},
		SelectAggrThreshold:                toml.Duration{},
		EnableStmtMerge:                    false,
		LabelSelector:                      map[string]string{defaultLoggerLabelKey: defaultLoggerLabelVal}, /*role=logging_cn*/
		TCPPacket:                          false,
		CU:                                 *NewOBCUConfig(),
		CUv1:                               *NewOBCUConfig(),
		OBCollectorConfig:                  *NewOBCollectorConfig(),
	}
	op.MetricInternalGatherInterval.Duration = defaultMetricInternalGatherInterval
	op.MetricStorageUsageUpdateInterval.Duration = defaultMetricUpdateStorageUsageInterval
	op.MetricStorageUsageCheckNewInterval.Duration = defaultMetricStorageUsageCheckNewInterval
	op.MergeCycle.Duration = defaultMergeCycle
	op.LongSpanTime.Duration = defaultLongSpanTime
	op.AggregationWindow.Duration = defaultAggregationWindow
	op.SelectAggrThreshold.Duration = defaultSelectThreshold
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

	if op.SelectAggrThreshold.Duration <= 0 {
		op.SelectAggrThreshold.Duration = defaultSelectThreshold
	}

	// this loop must after SelectAggrThreshold and DisableStmtAggregation
	if !op.DisableStmtAggregation {
		val := float64(op.SelectAggrThreshold.Duration) / float64(time.Second)
		if op.LongQueryTime <= val {
			op.LongQueryTime = val
		}
	}
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
	CpuPrice      float64 `toml:"cpu_price"`
	MemPrice      float64 `toml:"mem_price"`
	IoInPrice     float64 `toml:"io_in_price"`
	IoOutPrice    float64 `toml:"io_out_price"`
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
	if c.CpuPrice <= 0 {
		c.CpuPrice = CUCpuPriceDefault
	}
	if c.MemPrice <= 0 {
		c.MemPrice = CUMemPriceDefault
	}
	if c.IoInPrice <= 0 {
		c.IoInPrice = CUIOInPriceDefault
	}
	if c.IoOutPrice <= 0 {
		c.IoOutPrice = CUIOOutPriceDefault
	}
	if c.TrafficPrice0 <= 0 {
		c.TrafficPrice0 = CUTrafficPrice0Default
	}
	if c.TrafficPrice1 <= 0 {
		c.TrafficPrice1 = CUTrafficPrice1Default
	}
	if c.TrafficPrice2 <= 0 {
		c.TrafficPrice2 = CUTrafficPrice2Default
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

	// QueryClient instance
	QueryClient qclient.QueryClient

	UdfService udf.Service

	// HAKeeper client, which is used to get connection ID
	// from HAKeeper currently.
	HAKeeperClient logservice.CNHAKeeperClient

	TaskService taskservice.TaskService
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
