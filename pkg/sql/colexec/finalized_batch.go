// Copyright 2026 Matrix Origin
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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// FinalizedBatchConsumer consumes a borrowed final-result batch
// synchronously. It must retain only copies after ConsumeFinalizedBatch
// returns; the source remains the sole owner of the batch and its vectors.
type FinalizedBatchConsumer interface {
	ConsumeFinalizedBatch(*process.Process, *batch.Batch) error
}

// FinalizedBatchConsumerToken identifies one Prepare generation. A source
// accepts at most one consumer in a generation and ignores stale detaches.
type FinalizedBatchConsumerToken uint64

// FinalizedBatchSource is an optional, local-only operator handshake. It lets
// a blocking finalizer stream bounded result chunks directly into a retaining
// consumer without changing the physical plan or the distributed wire format.
type FinalizedBatchSource interface {
	TryAttachFinalizedBatchConsumer(
		FinalizedBatchConsumer,
	) (FinalizedBatchConsumerToken, bool)
	DetachFinalizedBatchConsumer(FinalizedBatchConsumerToken)
}
