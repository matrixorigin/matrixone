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

package compile

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
)

// SiriusPrepareRequest transfers admitted read ownership to Prepare. The engine
// must either return an execution owner or finish/retain cleanup on failure. It
// must join its consumer before relinquishing query ownership. The deadline includes
// neither a new transaction nor permission to replay an externally visible query.
type SiriusPrepareRequest struct {
	AccountID          uint64
	QueryID            []byte
	Plan               []byte
	OutputTypes        []planpb.Type
	Headings           []string
	Deadline           time.Time
	Snapshot           [12]byte
	Reads              []SiriusReadDescriptor
	NativeCapabilities uint64
	NumericProfile     string
}

// SiriusInput reserves native credit before a producer materializes its outgoing
// payload. Existing MO reader batches remain subject to their pipeline bounds.
type SiriusInput interface {
	Acquire(context.Context, uint64) (SiriusInputLease, error)
}

// SiriusInputLease owns reserved credit. Register Release immediately after
// acquisition. Publish synchronously copies Go buffers: success transfers native
// ownership to Sirius; failure leaves cleanup with the producer. Release is
// idempotent and transfers failed native cleanup to the query owner.
type SiriusInputLease interface {
	Capacity() uint64
	Publish(context.Context, uint32, []SiriusInputVector) error
	Release() error
}

type SiriusInputVector struct {
	Class             uint32
	Data, Area, Nulls []byte
}

type SiriusReadColumn struct {
	Type       planpb.Type
	Name       string
	PhysicalID uint64
	Sequence   uint32
}

type SiriusReadDescriptor struct {
	BindingID               uint64
	Database, Table, Schema string
	Columns                 []SiriusReadColumn
	Producer                func(context.Context, SiriusInput) error
}

// SiriusExecution owns one prepared execution. Run is single-use and must not
// retain a batch after fill returns. Cleanup interrupts active execution and
// joins its consumer before releasing read ownership; CleanupAfterRun handles
// graceful completion. Both cleanup paths are idempotent and retain retryable
// ownership when their context expires before cleanup is complete.
type SiriusExecution interface {
	Run(context.Context, *mpool.MPool, *perfcounter.CounterSet, func(*batch.Batch, *perfcounter.CounterSet) error) error
	Cleanup(context.Context) error
	CleanupAfterRun(context.Context, error) error
}

// SiriusEngine is the active native execution and ownership boundary.
// Close seals admission and joins all work before releasing engine resources.
type SiriusEngine interface {
	Capabilities() uint64
	Accepting() bool
	Prepare(context.Context, SiriusPrepareRequest) (SiriusExecution, error)
	Close(context.Context) error
}
