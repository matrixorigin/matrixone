// Copyright 2021-2023 Matrix Origin
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

package table_scan

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(TableScan)

type container struct {
	// scanMP is the scan's own pool for its output vectors when no accounted
	// OutputAllocation is bound. Output rows copied out of the buffer pool
	// live in it, so no other operator (they use proc.Mp() or their own
	// pools) can modify them: the strict mpool check rejects any other pool.
	// It lives as long as buf.
	scanMP                    *mpool.MPool
	maxAllocSize              int
	buf                       *batch.Batch
	msgReceiver               *message.MessageReceiver
	filterExecutors           []colexec.ExpressionExecutor
	runtimeFilterExecutors    []colexec.ExpressionExecutor
	allFilterExecutors        []colexec.ExpressionExecutor // runtime + static; elements owned by above slices
	earlyColumns              []int
	lateColumns               []int
	filterReadMetrics         bool
	filterLateMaterialized    bool
	membershipPrepared        bool
	membershipApplied         bool
	filterActiveDuration      time.Duration
	readerFilter              *engine.ReaderFilterRequest
	columnUses                []engine.ReaderColumnUse
	columnUsesReady           bool
	metricView                *batch.Batch
	padCharToFullLength       bool
	subjectReuseAdmissionDone bool
	domainSources             []localDomainSourceState
	domainProbes              []localDomainProbeState
	domainRows                []int64
	domainsReady              bool
	domainEmpty               bool
	// selection is the scan's selected-output machinery (selection.go).
	selection selectionState
}

type TableScan struct {
	// OutputAllocation belongs to the caller execution and is installed before reading.
	// It is an accounted view of the process pool (nil = unaccounted).
	OutputAllocation  *mpool.MPool
	allocationAccount *mpool.AllocationAccount
	ctr               container
	TopValueMsgTag    int32
	Reader            engine.Reader
	// letter case: origin
	Attrs              []string
	Types              []plan.Type
	TableID            uint64
	FilterExprs        []*plan.Expr // static inline filters (from plan.Node.FilterList, set at compile time)
	RuntimeFilterExprs []*plan.Expr // runtime inline filters (set by handleRuntimeFilters before Prepare)
	LocalDomainBuilds  []runtimefilter.LocalDomainBinding
	LocalDomainProbes  []runtimefilter.LocalDomainBinding
	ReadPolicy         fileservice.Policy // added to the read context's fileservice policy

	vm.OperatorBase
	colexec.Projection
}

func (tableScan *TableScan) ActivatesAllocationAccountLifecycle() bool {
	return len(tableScan.FilterExprs) > 0 || len(tableScan.RuntimeFilterExprs) > 0
}

func (tableScan *TableScan) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if tableScan.allocationAccount != nil {
		if tableScan.allocationAccount == account {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	if !tableScan.ActivatesAllocationAccountLifecycle() && tableScan.OutputAllocation == nil {
		return nil
	}
	if tableScan.ctr.buf != nil || len(tableScan.ctr.filterExecutors) != 0 || tableScan.HasPreparedProjection() {
		return mpool.ErrAllocationAccountInvariant
	}
	if tableScan.OutputAllocation != nil && tableScan.OutputAllocation.ViewAccount() != account {
		return mpool.ErrAllocationAccountMismatch
	}
	// Without a caller-installed view, Prepare builds one on the process pool.
	tableScan.allocationAccount = account
	return nil
}

// bindOutputAllocation builds the accounted view of proc's pool for a bound
// account when the caller did not install one. Binding happens before the
// operator knows its process pool, so the view is built on first use.
func (tableScan *TableScan) bindOutputAllocation(proc *process.Process) error {
	if tableScan.allocationAccount == nil || tableScan.OutputAllocation != nil {
		return nil
	}
	view, err := proc.Mp().WithAccount(
		tableScan.allocationAccount, mpool.AllocationOwnerSelection, 1, mpool.AllocationCapacityClassDefault)
	if err != nil {
		return err
	}
	tableScan.OutputAllocation = view
	return nil
}

// BoundAllocationAccount returns the statement account bound by
// SetAllocationAccount, or nil.
func (tableScan *TableScan) BoundAllocationAccount() *mpool.AllocationAccount {
	return tableScan.allocationAccount
}

// mp returns the pool scan output allocates from: the OutputAllocation view
// when one is bound, otherwise the scan's own pool (proc's pool before
// Prepare creates it).
func (tableScan *TableScan) mp(proc *process.Process) *mpool.MPool {
	if tableScan.OutputAllocation != nil {
		return tableScan.OutputAllocation
	}
	if tableScan.ctr.scanMP != nil {
		return tableScan.ctr.scanMP
	}
	return proc.Mp()
}

// prepareScanMP creates the scan's own output pool. It is called only while
// the output batch does not exist yet.
func (tableScan *TableScan) prepareScanMP(proc *process.Process) error {
	if tableScan.OutputAllocation != nil || tableScan.ctr.scanMP != nil {
		return nil
	}
	mp, err := colexec.NewOperatorMPool(proc, "table_scan")
	if err != nil {
		return err
	}
	tableScan.ctr.scanMP = mp
	return nil
}

func (tableScan *TableScan) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	if tableScan.allocationAccount == nil {
		return nil
	}
	if tableScan.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if tableScan.ctr.buf != nil || len(tableScan.ctr.filterExecutors) != 0 ||
		len(tableScan.ctr.runtimeFilterExecutors) != 0 || tableScan.HasPreparedProjection() {
		return mpool.ErrAllocationAccountLive
	}
	tableScan.allocationAccount, tableScan.OutputAllocation = nil, nil
	return nil
}

func (tableScan *TableScan) GetOperatorBase() *vm.OperatorBase {
	return &tableScan.OperatorBase
}

func init() {
	reuse.CreatePool[TableScan](
		func() *TableScan {
			return &TableScan{}
		},
		func(a *TableScan) {
			*a = TableScan{}
		},
		reuse.DefaultOptions[TableScan]().
			WithEnableChecker(),
	)
}

func (tableScan TableScan) TypeName() string {
	return opName
}

func NewArgument() *TableScan {
	return reuse.Alloc[TableScan](nil)
}

func (tableScan *TableScan) WithTypes(types []plan.Type) *TableScan {
	tableScan.Types = types
	return tableScan
}

func (tableScan *TableScan) Release() {
	if tableScan != nil {
		reuse.Free[TableScan](tableScan, nil)
	}
}

func (tableScan *TableScan) Reset(proc *process.Process, pipelineFailed bool, err error) {
	tableScan.releaseLocalDomains()
	allocSize := int64(0)
	allocSize += int64(tableScan.ctr.maxAllocSize)
	if tableScan.ProjectList != nil {
		allocSize += tableScan.ProjectAllocSize
		tableScan.ResetProjection(proc)
	}
	for i := range tableScan.ctr.filterExecutors {
		if tableScan.ctr.filterExecutors[i] != nil {
			tableScan.ctr.filterExecutors[i].ResetForNextQuery()
		}
	}
	// runtime filter executors are recreated each query; free and clear them
	for i := range tableScan.ctr.runtimeFilterExecutors {
		if tableScan.ctr.runtimeFilterExecutors[i] != nil {
			tableScan.ctr.runtimeFilterExecutors[i].Free()
		}
	}
	tableScan.ctr.runtimeFilterExecutors = nil
	tableScan.ctr.allFilterExecutors = nil
	tableScan.ctr.earlyColumns = tableScan.ctr.earlyColumns[:0]
	tableScan.ctr.lateColumns = tableScan.ctr.lateColumns[:0]
	tableScan.ctr.filterReadMetrics = false
	tableScan.ctr.filterLateMaterialized = false
	tableScan.ctr.filterActiveDuration = 0
	if tableScan.ctr.readerFilter != nil && tableScan.ctr.readerFilter.Exact != nil {
		tableScan.ctr.readerFilter.Exact.Close()
	}
	tableScan.ctr.readerFilter.FreeMembership(proc.Mp())
	tableScan.ctr.readerFilter = nil
	tableScan.ctr.membershipPrepared = false
	tableScan.ctr.membershipApplied = false
	tableScan.RuntimeFilterExprs = nil
	tableScan.ctr.maxAllocSize = 0
	if tableScan.OpAnalyzer != nil {
		tableScan.OpAnalyzer.Alloc(allocSize)
	}
	tableScan.closeReader()
	// The output batch is the lender of the cache pins its borrowed columns
	// alias; nothing reads it after Reset, so do not keep the pins while the
	// operator waits to be reused.
	if tableScan.ctr.buf != nil {
		tableScan.ctr.buf.CleanOnlyData()
	}
	tableScan.ctr.selection.reset()
	if tableScan.allocationAccount != nil {
		tableScan.Free(proc, pipelineFailed, err)
	}
}

func (tableScan *TableScan) Free(proc *process.Process, pipelineFailed bool, err error) {
	tableScan.ctr.columnUses = nil
	tableScan.ctr.columnUsesReady = false

	tableScan.releaseLocalDomains()
	tableScan.ctr.selection.free(tableScan.mp(proc))
	if tableScan.ctr.buf != nil {
		// The output vectors were created with the scan's output pool.
		tableScan.ctr.buf.Clean(tableScan.mp(proc))
		tableScan.ctr.buf = nil
	}
	if tableScan.ctr.scanMP != nil {
		// The reader fills buf through scanMP; close it before the pool goes.
		tableScan.closeReader()
		colexec.FreeOperatorMPool(&tableScan.ctr.scanMP)
	}

	if tableScan.ProjectList != nil {
		tableScan.FreeProjection(proc)
	}

	for i := range tableScan.ctr.filterExecutors {
		if tableScan.ctr.filterExecutors[i] != nil {
			tableScan.ctr.filterExecutors[i].Free()
		}
	}
	tableScan.ctr.filterExecutors = nil
	tableScan.ctr.subjectReuseAdmissionDone = false

	for i := range tableScan.ctr.runtimeFilterExecutors {
		if tableScan.ctr.runtimeFilterExecutors[i] != nil {
			tableScan.ctr.runtimeFilterExecutors[i].Free()
		}
	}
	tableScan.ctr.runtimeFilterExecutors = nil
	tableScan.ctr.allFilterExecutors = nil
	tableScan.ctr.earlyColumns = nil
	tableScan.ctr.lateColumns = nil
	tableScan.ctr.filterReadMetrics = false
	tableScan.ctr.filterLateMaterialized = false
	tableScan.ctr.filterActiveDuration = 0
	if tableScan.ctr.readerFilter != nil && tableScan.ctr.readerFilter.Exact != nil {
		tableScan.ctr.readerFilter.Exact.Close()
	}
	tableScan.ctr.readerFilter.FreeMembership(proc.Mp())
	tableScan.ctr.readerFilter = nil
	tableScan.ctr.membershipPrepared = false
	tableScan.ctr.membershipApplied = false
	if tableScan.ctr.metricView != nil {
		tableScan.ctr.metricView.Vecs = nil
		tableScan.ctr.metricView = nil
	}
}

func (tableScan *TableScan) closeReader() {
	if tableScan.Reader != nil {
		e := tableScan.Reader.Close()
		if e != nil {
			logutil.Errorf("close reader for table id=%d, err=%v", tableScan.TableID, e)
		}
		tableScan.Reader = nil
	}
}

func (tableScan *TableScan) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	if input.HasSelection() {
		return tableScan.execSelectedProjection(proc, input)
	}
	var err error
	batch := input
	if tableScan.ProjectList != nil {
		batch, err = tableScan.EvalProjection(input, proc)
	}

	if err != nil {
		return nil, err
	}
	return batch, nil
}
