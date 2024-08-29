// Copyright 2023 Matrix Origin
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

package v2

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var (
	registry = prometheus.NewRegistry()
)

func GetPrometheusRegistry() prometheus.Registerer {
	return registry
}

func GetPrometheusGatherer() prometheus.Gatherer {
	return registry
}

func init() {
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registry.MustRegister(collectors.NewGoCollector(
		collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsAll),
	))

	initFileServiceMetrics()
	initLogtailMetrics()
	initTxnMetrics()
	initTaskMetrics()
	initRPCMetrics()
	initMemMetrics()
	initTraceMetrics()
	initProxyMetrics()
	initFrontendMetrics()
	initPipelineMetrics()
	initLogServiceMetrics()

	registry.MustRegister(HeartbeatHistogram)
	registry.MustRegister(HeartbeatFailureCounter)
	registry.MustRegister(HeartbeatRecvHistogram)
	registry.MustRegister(HeartbeatRecvFailureCounter)
	registry.MustRegister(statsTriggerCounter)
	registry.MustRegister(StatsUpdateBlockCounter)
}

func initMemMetrics() {
	registry.MustRegister(memMPoolAllocatedSizeGauge)
	registry.MustRegister(MemTotalCrossPoolFreeCounter)
	registry.MustRegister(memMPoolHighWaterMarkGauge)
	registry.MustRegister(mallocCounter)
	registry.MustRegister(mallocGauge)
}

func initTaskMetrics() {
	registry.MustRegister(taskShortDurationHistogram)
	registry.MustRegister(taskLongDurationHistogram)
	registry.MustRegister(taskBytesHistogram)
	registry.MustRegister(taskCountHistogram)

	registry.MustRegister(taskScheduledByCounter)
	registry.MustRegister(taskGeneratedStuffCounter)
	registry.MustRegister(taskSelectivityCounter)

	registry.MustRegister(transferPageHitHistogram)
	registry.MustRegister(TransferPageRowHistogram)
	registry.MustRegister(TaskMergeTransferPageLengthGauge)
	registry.MustRegister(transferDurationHistogram)
	registry.MustRegister(transferShortDurationHistogram)

	registry.MustRegister(TaskStorageUsageCacheMemUsedGauge)
}

func initFileServiceMetrics() {
	registry.MustRegister(fsReadCounter)
	registry.MustRegister(S3ConnectCounter)
	registry.MustRegister(S3DNSResolveCounter)

	registry.MustRegister(s3IOBytesHistogram)
	registry.MustRegister(s3ConnDurationHistogram)
	registry.MustRegister(localIOBytesHistogram)

	registry.MustRegister(ioMergerCounter)
	registry.MustRegister(ioMergerDuration)
	registry.MustRegister(fsReadWriteDuration)
	registry.MustRegister(FSObjectStorageOperations)
}

func initLogtailMetrics() {
	registry.MustRegister(LogtailLoadCheckpointCounter)
	registry.MustRegister(logtailReceivedCounter)

	registry.MustRegister(logTailQueueSizeGauge)

	registry.MustRegister(LogTailBytesHistogram)
	registry.MustRegister(logTailApplyDurationHistogram)
	registry.MustRegister(logtailUpdatePartitionDurationHistogram)
	registry.MustRegister(LogTailAppendDurationHistogram)
	registry.MustRegister(logTailSendDurationHistogram)
	registry.MustRegister(LogTailLoadCheckpointDurationHistogram)

	registry.MustRegister(LogTailPushCollectionDurationHistogram)
	registry.MustRegister(LogTailPullCollectionPhase1DurationHistogram)
	registry.MustRegister(LogTailPullCollectionPhase2DurationHistogram)
	registry.MustRegister(LogTailSubscriptionCounter)
	registry.MustRegister(txnTNSideDurationHistogram)
}

func initTxnMetrics() {
	registry.MustRegister(txnCounter)
	registry.MustRegister(txnStatementCounter)
	registry.MustRegister(txnCommitCounter)
	registry.MustRegister(TxnRollbackCounter)
	registry.MustRegister(txnLockCounter)

	registry.MustRegister(txnQueueSizeGauge)

	registry.MustRegister(txnCommitDurationHistogram)
	registry.MustRegister(TxnLifeCycleDurationHistogram)
	registry.MustRegister(TxnLifeCycleStatementsTotalHistogram)
	registry.MustRegister(txnCreateDurationHistogram)
	registry.MustRegister(txnStatementDurationHistogram)
	registry.MustRegister(txnLockDurationHistogram)
	registry.MustRegister(txnUnlockDurationHistogram)
	registry.MustRegister(TxnTableRangeDurationHistogram)
	registry.MustRegister(TxnCheckPKDupDurationHistogram)
	registry.MustRegister(TxnLockWaitersTotalHistogram)
	registry.MustRegister(txnTableRangeTotalHistogram)
	registry.MustRegister(txnMpoolDurationHistogram)
	registry.MustRegister(TxnUnlockTableTotalHistogram)
	registry.MustRegister(txnReaderDurationHistogram)

	registry.MustRegister(txnCNCommittedLocationQuantityGauge)

	registry.MustRegister(txnRangesSelectivityHistogram)
	registry.MustRegister(txnTNDeduplicateDurationHistogram)

	registry.MustRegister(TxnReaderScannedTotalTombstoneHistogram)
	registry.MustRegister(TxnReaderEachBLKLoadedTombstoneHistogram)
	registry.MustRegister(txnReaderTombstoneSelectivityHistogram)
	registry.MustRegister(txnTransferDurationHistogram)
	registry.MustRegister(TransferTombstonesCountHistogram)
}

func initRPCMetrics() {
	registry.MustRegister(RPCClientCreateCounter)
	registry.MustRegister(rpcBackendCreateCounter)
	registry.MustRegister(rpcBackendClosedCounter)
	registry.MustRegister(rpcBackendConnectCounter)
	registry.MustRegister(rpcMessageCounter)
	registry.MustRegister(rpcNetworkBytesCounter)

	registry.MustRegister(rpcBackendPoolSizeGauge)
	registry.MustRegister(rpcSendingQueueSizeGauge)
	registry.MustRegister(rpcSendingBatchSizeGauge)
	registry.MustRegister(rpcServerSessionSizeGauge)

	registry.MustRegister(rpcBackendConnectDurationHistogram)
	registry.MustRegister(rpcWriteDurationHistogram)
	registry.MustRegister(rpcWriteLatencyDurationHistogram)
	registry.MustRegister(rpcBackendDoneDurationHistogram)

}

func initProxyMetrics() {
	registry.MustRegister(proxyConnectCounter)
	registry.MustRegister(proxyDisconnectCounter)
	registry.MustRegister(proxyTransferCounter)
	registry.MustRegister(ProxyTransferDurationHistogram)
	registry.MustRegister(ProxyDrainCounter)
	registry.MustRegister(ProxyAvailableBackendServerNumGauge)
	registry.MustRegister(ProxyTransferQueueSizeGauge)
	registry.MustRegister(ProxyConnectionsNeedToTransferGauge)
}

func initFrontendMetrics() {
	registry.MustRegister(acceptConnDurationHistogram)
	registry.MustRegister(routineCounter)
	registry.MustRegister(requestCounter)
	registry.MustRegister(resolveDurationHistogram)
	registry.MustRegister(createAccountDurationHistogram)
	registry.MustRegister(pubSubDurationHistogram)
	registry.MustRegister(sqlLengthHistogram)
}

func initPipelineMetrics() {
	registry.MustRegister(PipelineServerDurationHistogram)
	registry.MustRegister(pipelineStreamCounter)
}

func initLogServiceMetrics() {
	registry.MustRegister(LogServiceAppendDurationHistogram)
	registry.MustRegister(LogServiceAppendCounter)
	registry.MustRegister(LogServiceAppendBytesHistogram)
}

func getDurationBuckets() []float64 {
	return append(prometheus.ExponentialBuckets(0.00001, 2, 30), math.MaxFloat64)
}

func getShortDurationBuckets() []float64 {
	return append(prometheus.ExponentialBuckets(0.0000001, 2, 30), math.MaxFloat64)
}
