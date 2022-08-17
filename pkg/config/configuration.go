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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
)

type ConfigurationKeyType int

const (
	ParameterUnitKey ConfigurationKeyType = 1
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
	RecordTimeElapsedOfSqlRequest bool `toml:"recordTimeElapsedOfSqlRequest"`

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

	//default is false. true : one txn for an independent batch false : only one txn during loading data
	OneTxnPerBatchDuringLoad bool `toml:"oneTxnPerBatchDuringLoad"`

	//statusPort defines which port the mo status server (for metric etc.) listens on and clients connect to
	StatusPort int64 `toml:"statusPort"`

	//default is true. if true, metrics can be scraped through host:status/metrics endpoint
	MetricToProm bool `toml:"metricToProm"`

	//default is true. if true, enable metric at booting
	EnableMetric bool `toml:"enableMetric"`

	//default is true. if true, enable trace at booting
	EnableTrace bool `toml:"enableTrace"`

	//default is InternalExecutor. if InternalExecutor, use internal sql executor, FileService will implement soon.
	TraceBatchProcessor string `toml:"traceBatchProcessor"`

	//default is false. With true, system will check all the children span is ended, which belong to the closing span.
	EnableTraceDebug bool `toml:"enableTraceDebug"`

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
}

type ParameterUnit struct {
	SV *FrontendParameters

	//host memory
	HostMmu *host.Mmu

	//mempool
	Mempool *mempool.Mempool

	//Storage Engine
	StorageEngine engine.Engine

	//Cluster Nodes
	ClusterNodes engine.Nodes
}

func NewParameterUnit(sv *FrontendParameters, hostMmu *host.Mmu, mempool *mempool.Mempool, storageEngine engine.Engine, clusterNodes engine.Nodes) *ParameterUnit {
	return &ParameterUnit{
		SV:            sv,
		HostMmu:       hostMmu,
		Mempool:       mempool,
		StorageEngine: storageEngine,
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
