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

package mongoscan

import (
	"context"
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const mongoCleanupTimeout = 10 * time.Second

var _ vm.Operator = new(MongoScan)

type container struct {
	cursor        mongodb.Cursor
	lease         *mongodb.ClientLease
	converter     *mongodb.Converter
	userQuery     *mongodb.UserQuery
	buf           *batch.Batch
	done          bool
	rows          int64
	rawBytes      int64
	generation    uint64
	pendingRaw    []byte
	pendingBudget *process.ExecutionResourceGeneration
	pendingCharge uint64
	releaseLimit  func()
	queryCtx      context.Context
	cancelQuery   context.CancelFunc
	closeErr      error
}

// MongoScan performs source I/O and BSON-to-vector conversion. Ordinary SQL
// filtering, grouping, time-window aggregation, and writes remain MO operators;
// an explicit, validated __mo_query may opt into MongoDB filter or aggregation
// semantics before conversion.
type MongoScan struct {
	Scan             *plan.MongoScan
	OutputAllocation *vector.AllocationAccountSelection

	// Dependencies is set only by unit tests and embedded deployments. Normal
	// CN execution obtains the same object from the service runtime.
	Dependencies *mongodb.RuntimeDependencies

	ctr container
	vm.OperatorBase
	colexec.Projection
}

func init() {
	reuse.CreatePool[MongoScan](
		func() *MongoScan { return &MongoScan{} },
		func(scan *MongoScan) { *scan = MongoScan{} },
		reuse.DefaultOptions[MongoScan]().WithEnableChecker(),
	)
}

func NewArgument() *MongoScan { return reuse.Alloc[MongoScan](nil) }

func (scan *MongoScan) WithScan(spec *plan.MongoScan) *MongoScan {
	scan.Scan = spec
	return scan
}

func (scan *MongoScan) GetOperatorBase() *vm.OperatorBase { return &scan.OperatorBase }
func (scan MongoScan) TypeName() string                   { return "mongoscan" }

func (scan *MongoScan) Release() {
	if scan != nil {
		reuse.Free[MongoScan](scan, nil)
	}
}

func (scan *MongoScan) Reset(proc *process.Process, _ bool, _ error) {
	scan.closeResources(proc.Ctx)
	scan.ctr.closeErr = nil
	if scan.ctr.buf != nil {
		scan.ctr.buf.Clean(proc.Mp())
		scan.ctr.buf = nil
	}
	scan.ctr.converter = nil
	scan.ctr.userQuery = nil
	scan.ctr.done = false
	scan.ctr.rows = 0
	scan.ctr.rawBytes = 0
	scan.ctr.pendingRaw = nil
	if scan.ProjectList != nil {
		if scan.OpAnalyzer != nil {
			scan.OpAnalyzer.Alloc(scan.ProjectAllocSize)
		}
		scan.ResetProjection(proc)
	}
}

func (scan *MongoScan) Free(proc *process.Process, _ bool, _ error) {
	scan.closeResources(proc.Ctx)
	if scan.ctr.buf != nil {
		scan.ctr.buf.Clean(proc.Mp())
		scan.ctr.buf = nil
	}
	scan.ctr.converter = nil
	scan.ctr.userQuery = nil
	scan.ctr.pendingRaw = nil
	scan.FreeProjection(proc)
}

func (scan *MongoScan) closeResources(_ context.Context) {
	scan.clearPendingRaw()
	// Reset/Free and cursor-error cleanup frequently run after the statement
	// context has been canceled. Give the driver a separate bounded window to
	// send killCursors and retire a stale client generation; using the canceled
	// statement context would turn cleanup into a no-op, while an unbounded
	// background context could stall pipeline teardown indefinitely.
	cleanupCtx, cancel := context.WithTimeout(context.Background(), mongoCleanupTimeout)
	defer cancel()
	if scan.ctr.cancelQuery != nil {
		scan.ctr.cancelQuery()
		scan.ctr.cancelQuery = nil
	}
	scan.ctr.queryCtx = nil
	if scan.ctr.cursor != nil {
		scan.ctr.closeErr = errors.Join(scan.ctr.closeErr, scan.ctr.cursor.Close(cleanupCtx))
		metric.MongoDBCursorEventCounter.WithLabelValues("close").Inc()
		scan.ctr.cursor = nil
	}
	if scan.ctr.lease != nil {
		scan.ctr.closeErr = errors.Join(scan.ctr.closeErr, scan.ctr.lease.Release(cleanupCtx))
		scan.ctr.lease = nil
	}
	if scan.ctr.releaseLimit != nil {
		scan.ctr.releaseLimit()
		scan.ctr.releaseLimit = nil
	}
}

func (scan *MongoScan) clearPendingRaw() {
	scan.ctr.pendingRaw = nil
	if scan.ctr.pendingBudget != nil {
		scan.ctr.pendingBudget.ReleaseAllocationCapacity(scan.ctr.pendingCharge)
	}
	scan.ctr.pendingBudget, scan.ctr.pendingCharge = nil, 0
}

func (scan *MongoScan) retainPendingRaw(proc *process.Process, raw []byte) error {
	// A previously retained document is already immutable and accounted.
	if len(scan.ctr.pendingRaw) != 0 {
		return nil
	}
	if scan.OutputAllocation != nil {
		budget, err := proc.GetExecutionResourceBudget()
		if err != nil {
			return err
		}
		if err = budget.AcquireAllocationCapacity(uint64(len(raw))); err != nil {
			return err
		}
		scan.ctr.pendingBudget, scan.ctr.pendingCharge = budget, uint64(len(raw))
	}
	scan.ctr.pendingRaw = make([]byte, len(raw))
	copy(scan.ctr.pendingRaw, raw)
	return nil
}

// CloseReader lets a dependent source retire I/O before publishing its scalar
// result. EOF may have already closed the cursor, so retain that terminal error
// until Reset starts the next generation.
func (scan *MongoScan) CloseReader() error {
	scan.closeResources(context.Background())
	return scan.ctr.closeErr
}

func (scan *MongoScan) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	if scan.ProjectList == nil {
		return input, nil
	}
	return scan.EvalProjection(input, proc)
}
