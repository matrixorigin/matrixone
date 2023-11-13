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
	"github.com/prometheus/client_golang/prometheus"
)

var (
	registry = prometheus.DefaultRegisterer
)

func init() {
	initFileServiceMetrics()
	initLogtailMetrics()
	initTxnMetrics()
	initTaskMetrics()
	initRPCMetrics()
	initMemMetrics()

	registry.MustRegister(HeartbeatHistogram)
	registry.MustRegister(HeartbeatFailureCounter)
	registry.MustRegister(HeartbeatRecvHistogram)
	registry.MustRegister(HeartbeatRecvFailureCounter)
}

func initMemMetrics() {
	registry.MustRegister(memMPoolAllocatedSizeGauge)
	registry.MustRegister(MemTotalCrossPoolFreeCounter)
}

func initTaskMetrics() {
	registry.MustRegister(taskShortDurationHistogram)
	registry.MustRegister(taskLongDurationHistogram)

	registry.MustRegister(taskScheduledByCounter)
	registry.MustRegister(taskGeneratedStuffCounter)
	registry.MustRegister(taskSelectivityCounter)

	registry.MustRegister(TaskMergeTransferPageLengthGauge)
}

func initFileServiceMetrics() {
	registry.MustRegister(fsReadCounter)
	registry.MustRegister(S3ConnectCounter)
	registry.MustRegister(S3DNSResolveCounter)

	registry.MustRegister(s3IOBytesHistogram)
	registry.MustRegister(s3IODurationHistogram)
	registry.MustRegister(s3ConnDurationHistogram)
	registry.MustRegister(localIOBytesHistogram)
	registry.MustRegister(localIODurationHistogram)
}

func initLogtailMetrics() {
	registry.MustRegister(LogtailLoadCheckpointCounter)

	registry.MustRegister(logTailQueueSizeGauge)

	registry.MustRegister(LogTailBytesHistogram)
	registry.MustRegister(logTailApplyDurationHistogram)
	registry.MustRegister(LogTailAppendDurationHistogram)
	registry.MustRegister(logTailSendDurationHistogram)
	registry.MustRegister(LogTailLoadCheckpointDurationHistogram)

	registry.MustRegister(LogTailCollectDurationHistogram)
	registry.MustRegister(LogTailSubscriptionCounter)
	registry.MustRegister(txnTNSideDurationHistogram)

	registry.MustRegister(TxnShowAccountsDurationHistogram)
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
	registry.MustRegister(TxnLockWaitersTotalHistogram)
	registry.MustRegister(TxnTableRangeSizeHistogram)
	registry.MustRegister(txnMpoolDurationHistogram)

	registry.MustRegister(TxnRangesLoadedObjectMetaTotalCounter)
	registry.MustRegister(txnCNCommittedLocationQuantityGauge)
}

func initRPCMetrics() {
	registry.MustRegister(RPCClientCreateCounter)
	registry.MustRegister(rpcBackendCreateCounter)
	registry.MustRegister(rpcBackendClosedCounter)
	registry.MustRegister(rpcBackendConnectCounter)
	registry.MustRegister(rpcMessageCounter)

	registry.MustRegister(rpcBackendPoolSizeGauge)
	registry.MustRegister(rpcSendingQueueSizeGauge)
	registry.MustRegister(rpcSendingBatchSizeGauge)
	registry.MustRegister(rpcServerSessionSizeGauge)

	registry.MustRegister(rpcBackendConnectDurationHistogram)
	registry.MustRegister(rpcWriteDurationHistogram)
	registry.MustRegister(rpcWriteLatencyDurationHistogram)
	registry.MustRegister(rpcBackendDoneDurationHistogram)

}
