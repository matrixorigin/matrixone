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

package lockop

import (
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(LockOp)

// FetchLockRowsFunc fetch lock rows from vector.
type FetchLockRowsFunc func(
	// primary data vector
	vec *vector.Vector,
	// holder to encode primary key to lock row
	parker *types.Packer,
	// primary key type
	tp types.Type,
	// global config: max lock rows bytes per lock
	max int,
	// is lock table lock
	lockTable bool,
	// used to filter rows
	filter RowsFilter,
	// used by filter rows func
	filterCols []int32) (bool, [][]byte, lock.Granularity)

// LockOptions lock operation options
type LockOptions struct {
	maxCountPerLock          int
	mode                     lock.LockMode
	sharding                 lock.Sharding
	group                    uint32
	lockTable                bool
	changeDef                bool
	refreshTableSnapshot     bool
	admissionOnly            bool
	reuseOwnedRange          bool
	admissionResult          *lock.Result
	noWait                   bool
	skipLocked               bool
	waitPolicy               *lock.WaitPolicy
	parker                   *types.Packer
	fetchFunc                FetchLockRowsFunc
	filter                   RowsFilter
	filterCols               []int32
	hasNewVersionInRangeFunc hasNewVersionInRangeFunc
}

// LockOp lock op argument.
type LockOp struct {
	vm.OperatorBase

	logger  *log.MOLogger
	ctr     state
	engine  engine.Engine
	targets []lockTarget
}

func (lockOp *LockOp) GetOperatorBase() *vm.OperatorBase {
	return &lockOp.OperatorBase
}

func init() {
	reuse.CreatePool[LockOp](
		func() *LockOp {
			return &LockOp{}
		},
		func(a *LockOp) {
			*a = LockOp{}
		},
		reuse.DefaultOptions[LockOp]().
			WithEnableChecker(),
	)
}

func (lockOp LockOp) TypeName() string {
	return opName
}

func NewArgument() *LockOp {
	return reuse.Alloc[LockOp](nil)
}

func (lockOp *LockOp) Release() {
	if lockOp != nil {
		reuse.Free[LockOp](lockOp, nil)
	}
}

type lockTarget struct {
	tableID                      uint64
	objRef                       *plan.ObjectRef
	primaryColumnIndexInBatch    int32
	refreshTimestampIndexInBatch int32
	primaryColumnType            types.Type
	partitionColumnIndexInBatch  int32
	filter                       RowsFilter
	filterColIndexInBatch        int32
	lockTable                    bool
	changeDef                    bool
	mode                         lock.LockMode
	noWait                       bool
	skipLocked                   bool
	lockRows                     *plan.Expr
	lockTableAtTheEnd            bool
}

// RowsFilter used to filter row from primary vector. The row will not lock if filter return false.
type RowsFilter func(row int, filterCols []int32) bool

type hasNewVersionInRangeFunc func(
	proc *process.Process,
	rel engine.Relation,
	analyzer process.Analyzer,
	tableID uint64,
	eng engine.Engine,
	bat *batch.Batch,
	idx int32,
	partitionIdx int32,
	from, to timestamp.Timestamp) (bool, error)

type state struct {
	parker *types.Packer
	// skipLockedOutput is the operator-owned copy returned to downstream
	// consumers. It remains live until the next Call or operator cleanup so a
	// consumer may finish using the borrowed batch before it is reclaimed.
	skipLockedOutput *batch.Batch
	// refresh holds the operator's own refresh-timestamp vectors (one per
	// batch column, created once and refilled for every batch) and the view
	// of the child's batch that publishes them; the child's vectors are
	// never appended to.
	refresh              refreshOutput
	retryError           error
	defChanged           bool
	fetchers             []FetchLockRowsFunc
	relations            []engine.Relation
	hasNewVersionInRange hasNewVersionInRangeFunc
	lockCount            int64
}

// refreshOutput is the refresh-timestamp output of one Call.
type refreshOutput struct {
	mp       *mpool.MPool
	vecs     map[int32]*vector.Vector
	replaced map[int32]*vector.Vector
	view     *batch.Batch
}

// vector returns the operator's empty refresh-timestamp vector for column idx.
func (r *refreshOutput) vector(idx int32, typ types.Type, mp *mpool.MPool) *vector.Vector {
	if r.mp != mp {
		r.free()
		r.mp = mp
	}
	if r.vecs == nil {
		r.vecs = make(map[int32]*vector.Vector)
	}
	vec := r.vecs[idx]
	if vec != nil && vec.GetType().Eq(typ) {
		vec.CleanOnlyData()
		return vec
	}
	if vec != nil {
		vec.Free(r.mp)
	}
	vec = vector.NewVec(typ, r.mp)
	r.vecs[idx] = vec
	return vec
}

// publish returns bat, or the operator's view of bat with the refreshed
// columns installed when performLock refreshed any, and forgets them.
func (r *refreshOutput) publish(bat *batch.Batch) *batch.Batch {
	if len(r.replaced) == 0 {
		return bat
	}
	if r.view == nil {
		r.view = batch.NewWithSize(0)
	}
	bat.ViewInto(r.view)
	for idx, vec := range r.replaced {
		r.view.Vecs[idx] = vec
	}
	clear(r.replaced)
	return r.view
}

func (r *refreshOutput) free() {
	for _, vec := range r.vecs {
		vec.Free(r.mp)
	}
	r.vecs, r.replaced, r.view, r.mp = nil, nil, nil, nil
}
