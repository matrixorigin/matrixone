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
	"github.com/matrixorigin/matrixone/pkg/sql/compile/sidecarflight"
)

// SiriusPrepareRequest transfers admitted read ownership to Prepare. The backend
// must either return an execution owner or finish/retain cleanup on failure. It
// may call Release only after its consumer has quiesced. The deadline includes
// neither a new transaction nor permission to replay an externally visible query.
type SiriusPrepareRequest struct {
	AccountID   uint64
	QueryID     []byte
	Plan        []byte
	OutputTypes []planpb.Type
	Headings    []string
	Deadline    time.Time
	Release     func(context.Context) error
	Snapshot    [12]byte
	Reads       []SiriusReadDescriptor
}

// SiriusInput accepts MO-native vector buffers synchronously. It never retains
// Go memory after Push returns. Producers are lazy and owned by the execution.
type SiriusInput interface {
	Push(context.Context, uint32, []SiriusInputVector) error
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
	TAEManifest             []byte
	DataRoot                string
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

// SiriusBackend is the service-owned execution boundary. It has no registry or
// global mutable selector: a CN publishes exactly its configured backend.
// Reconcile retains cleanup ownership by statement identity until the previous
// consumer is quiescent. Close stops admission and drains that ownership.
type SiriusBackend interface {
	Prepare(context.Context, SiriusPrepareRequest) (SiriusExecution, error)
	Reconcile(uint64, []byte, func(context.Context) error) error
	Close(context.Context) error
	CanFallbackBeforeVisibility(error) bool
}

type siriusFlightBackend struct {
	*sidecarflight.Runtime
}

// NewSiriusFlightBackend preserves Flight's existing execution, recovery and
// fallback contracts while keeping its concrete types out of compiler owners.
// A nil runtime returns a nil interface, not a non-nil typed-nil backend.
func NewSiriusFlightBackend(runtime *sidecarflight.Runtime) SiriusBackend {
	if runtime == nil {
		return nil
	}
	return &siriusFlightBackend{Runtime: runtime}
}

func (b *siriusFlightBackend) Prepare(ctx context.Context, request SiriusPrepareRequest) (SiriusExecution, error) {
	execution, err := b.Runtime.Prepare(ctx, request.AccountID, request.QueryID,
		request.Plan, request.OutputTypes, request.Headings, request.Deadline, request.Release)
	if err != nil {
		return nil, err
	}
	return execution, nil
}

func (*siriusFlightBackend) CanFallbackBeforeVisibility(err error) bool {
	return sidecarflight.IsPreVisibilityFallback(err)
}

var _ SiriusBackend = (*siriusFlightBackend)(nil)
var _ SiriusExecution = (*sidecarflight.Execution)(nil)
