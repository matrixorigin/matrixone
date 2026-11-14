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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	dictionaryInputs          colexec.DictionaryInputFrame
	dictionaryBoundary        vm.DictionaryOutputState
	maxAllocSize              int
	buf                       *batch.Batch
	msgReceiver               *message.MessageReceiver
	filterExecutors           []colexec.ExpressionExecutor
	runtimeFilterExecutors    []colexec.ExpressionExecutor
	allFilterExecutors        []colexec.ExpressionExecutor // runtime + static; elements owned by above slices
	filterBs                  vector.FunctionParameterWrapper[bool]
	earlyColumns              []int
	lateColumns               []int
	filterRows                []int64
	filterReadMetrics         bool
	filterLateMaterialized    bool
	filterActiveDuration      time.Duration
	readerFilter              *engine.ReaderFilterRequest
	readerWorkspace           []*vector.Vector
	readerHeaders             []*vector.Vector
	readerSnapshots           []*vector.DictionaryVectorSlot
	readerSelection           *vector.DictionaryVectorSlot
	columnUses                []engine.ReaderColumnUse
	columnUsesReady           bool
	columnUsesOwned           bool
	metricView                *batch.Batch
	padCharToFullLength       bool
	subjectReuseAdmissionDone bool
	domainSources             []localDomainSourceState
	domainProbes              []localDomainProbeState
	domainRows                []int64
	domainsReady              bool
	domainEmpty               bool
}

type TableScan struct {
	// OutputAllocation belongs to the caller execution and is installed before reading.
	OutputAllocation  *vector.AllocationAccountSelection
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
	if tableScan.OutputAllocation != nil {
		if !tableScan.OutputAllocation.UsesAccount(account) {
			return mpool.ErrAllocationAccountMismatch
		}
	} else {
		selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
		if err != nil {
			return err
		}
		tableScan.OutputAllocation = selection
	}
	tableScan.allocationAccount = account
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

func (tableScan *TableScan) DictionaryOutput() (bool, *vector.AllocationAccountSelection) {
	return tableScan.OutputAllocation != nil && tableScan.ActivatesAllocationAccountLifecycle() &&
		!tableScan.ctr.padCharToFullLength && colexec.ExpressionsAcceptDictionary(tableScan.ctr.allFilterExecutors) &&
		colexec.ExpressionsAcceptDictionary(tableScan.ProjectExecutors), tableScan.OutputAllocation
}
func (tableScan *TableScan) DictionaryOutputState() *vm.DictionaryOutputState {
	return &tableScan.ctr.dictionaryBoundary
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
	tableScan.ctr.filterRows = tableScan.ctr.filterRows[:0]
	tableScan.ctr.filterReadMetrics = false
	tableScan.ctr.filterLateMaterialized = false
	tableScan.ctr.filterActiveDuration = 0
	if tableScan.ctr.readerFilter != nil && tableScan.ctr.readerFilter.Exact != nil {
		tableScan.ctr.readerFilter.Exact.Close()
	}
	tableScan.ctr.readerFilter = nil
	tableScan.RuntimeFilterExprs = nil
	tableScan.ctr.maxAllocSize = 0
	if tableScan.OpAnalyzer != nil {
		tableScan.OpAnalyzer.Alloc(allocSize)
	}
	tableScan.closeReader()
	if tableScan.allocationAccount != nil {
		tableScan.Free(proc, pipelineFailed, err)
	}
}

func (tableScan *TableScan) Free(proc *process.Process, pipelineFailed bool, err error) {
	for i, vec := range tableScan.ctr.readerWorkspace {
		if vec != nil {
			vec.Free(proc.Mp())
			tableScan.ctr.readerWorkspace[i] = nil
		}
	}
	tableScan.ctr.readerWorkspace = nil
	for _, v := range tableScan.ctr.readerHeaders {
		if v != nil {
			v.Free(proc.Mp())
		}
	}
	tableScan.ctr.readerHeaders = nil
	for _, slot := range tableScan.ctr.readerSnapshots {
		slot.Free(proc.Mp())
	}
	tableScan.ctr.readerSnapshots = nil
	tableScan.ctr.readerSelection.Free(proc.Mp())
	tableScan.ctr.readerSelection = nil
	tableScan.ctr.columnUses = nil
	tableScan.ctr.columnUsesReady = false
	tableScan.ctr.columnUsesOwned = false

	tableScan.ctr.dictionaryInputs.Free(proc.Mp())
	tableScan.releaseLocalDomains()
	if tableScan.ctr.buf != nil {
		tableScan.ctr.buf.Clean(proc.Mp())
		tableScan.ctr.buf = nil
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
	tableScan.ctr.filterRows = nil
	tableScan.ctr.filterReadMetrics = false
	tableScan.ctr.filterLateMaterialized = false
	tableScan.ctr.filterActiveDuration = 0
	if tableScan.ctr.readerFilter != nil && tableScan.ctr.readerFilter.Exact != nil {
		tableScan.ctr.readerFilter.Exact.Close()
	}
	tableScan.ctr.readerFilter = nil
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
	var err error
	batch := input
	if tableScan.ProjectList != nil {
		batch, err = tableScan.EvalProjection(input, proc)
		tableScan.ObserveDictionaryInputs(tableScan.OpAnalyzer)
	}

	if err != nil {
		return nil, err
	}
	return tableScan.ctr.dictionaryBoundary.Normalize(batch, proc.Mp())
}
