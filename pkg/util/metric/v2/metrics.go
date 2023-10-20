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

// MustRegister Delegate the prometheus MustRegister
func MustRegister(cs ...prometheus.Collector) {
	registry.MustRegister(cs...)
}

func init() {
	registry.MustRegister(S3FSCounter)
	registry.MustRegister(LocalFSCounter)
	registry.MustRegister(MemFSCounter)
	registry.MustRegister(TxnCounter)
	registry.MustRegister(TxnStatementCounter)
	registry.MustRegister(TxnStatementRetryCounter)
	registry.MustRegister(TxnHandleCommitCounter)

	registry.MustRegister(S3IOSizeGauge)
	registry.MustRegister(LocalIOSizeGauge)
	registry.MustRegister(LogTailSizeGauge)
	registry.MustRegister(TxnCommitSizeGauge)
	registry.MustRegister(TxnHandleQueueSizeGauge)

	registry.MustRegister(S3IODurationHistogram)
	registry.MustRegister(LocalIODurationHistogram)
	registry.MustRegister(S3GetConnDurationHistogram)
	registry.MustRegister(S3DNSDurationHistogram)
	registry.MustRegister(S3ConnectDurationHistogram)
	registry.MustRegister(S3TLSHandshakeDurationHistogram)
	registry.MustRegister(LogTailApplyDurationHistogram)
	registry.MustRegister(LogTailWaitDurationHistogram)
	registry.MustRegister(LogTailAppendDurationHistogram)
	registry.MustRegister(SQLBuildPlanDurationHistogram)
	registry.MustRegister(TxnDetermineSnapshotDurationHistogram)
	registry.MustRegister(TxnWaitActiveDurationHistogram)
	registry.MustRegister(TxnLockDurationHistogram)
	registry.MustRegister(TxnUnlockDurationHistogram)
	registry.MustRegister(TxnCommitDurationHistogram)
	registry.MustRegister(TxnTableRangeDurationHistogram)
	registry.MustRegister(TxnSendRequestDurationHistogram)
	registry.MustRegister(TxnHandleQueueInDurationHistogram)
	registry.MustRegister(TxnHandleCommitDurationHistogram)
}
