// Copyright 2022 Matrix Origin
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

package preinsert

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(PreInsert)

type proc = process.Process

type container struct {
	buf           *batch.Batch
	canFreeVecIdx map[int]bool //auto incr & expand constant vecotr.need free
	// ownedVecSlots records which buf slots currently hold storage this
	// operator allocated; Free releases exactly the recorded slots and
	// detaches every other entry as a child-owned borrow. canFreeVecIdx
	// carries a slot's ownership intent one step early: constructColBuf
	// marks aliased slots before converting them, so a marked slot whose
	// conversion never ran (an earlier marked slot's UnionBatch failed)
	// still references the child's vector - such an entry must be replaced
	// with independent storage when its conversion runs, and it must never
	// be cleaned or freed in place, on either the fill or the teardown path.
	ownedVecSlots     map[int]bool
	clusterByExecutor colexec.ExpressionExecutor
	compPkExecutor    colexec.ExpressionExecutor
	// autoIncrementGenerated is statement-row provenance captured before the
	// allocator fills the AUTO_INCREMENT column. It is emitted as a transient
	// bool vector only for the ordered INSERT IGNORE path.
	autoIncrementGenerated []bool
	odkuOrdinals           []uint64
	// tblId is a local copy of TableDef.TblId, refreshed by
	// refreshAutoIncrementTableID.  Storing it here avoids mutating the
	// shared *plan.TableDef that other operators may read concurrently.
	tblId uint64
}
type PreInsert struct {
	ctr container

	HasAutoCol  bool
	IsOldUpdate bool
	IsNewUpdate bool
	SchemaName  string
	TableDef    *plan.TableDef
	// letter case: origin
	Attrs []string

	EstimatedRowCount            int64
	CompPkeyExpr                 *plan.Expr
	ClusterByExpr                *plan.Expr
	ColOffset                    int32
	RejectZeroTemporal           bool
	TrackAutoIncrementGenerated  bool
	AutoIncrementGeneratedColumn int32
	TrackODKUResult              bool
	ODKUOrdinalColumn            int32
	// ODKUAutoIncrementColumn is the table-column index of the user-visible
	// AUTO_INCREMENT source. It excludes MatrixOne's hidden fake primary key.
	ODKUAutoIncrementColumn int32
	HasTargetSelector       bool
	TargetRowNumberCol      int32
	TargetActiveCol         int32
	TargetRowIDCol          int32

	vm.OperatorBase
}

func (preInsert *PreInsert) GetOperatorBase() *vm.OperatorBase {
	return &preInsert.OperatorBase
}

func (preInsert *PreInsert) SetRejectZeroTemporal(reject bool) {
	preInsert.RejectZeroTemporal = reject
}

func init() {
	reuse.CreatePool[PreInsert](
		func() *PreInsert {
			return &PreInsert{}
		},
		func(a *PreInsert) {
			*a = PreInsert{}
		},
		reuse.DefaultOptions[PreInsert]().
			WithEnableChecker(),
	)
}

func (preInsert PreInsert) TypeName() string {
	return opName
}

func NewArgument() *PreInsert {
	return reuse.Alloc[PreInsert](nil)
}

func (preInsert *PreInsert) Release() {
	if preInsert != nil {
		reuse.Free[PreInsert](preInsert, nil)
	}
}

func (preInsert *PreInsert) Reset(proc *process.Process, pipelineFailed bool, err error) {
	preInsert.ctr.autoIncrementGenerated = nil
	preInsert.ctr.odkuOrdinals = nil
	if preInsert.ctr.compPkExecutor != nil {
		preInsert.ctr.compPkExecutor.ResetForNextQuery()
	}
	if preInsert.ctr.clusterByExecutor != nil {
		preInsert.ctr.clusterByExecutor.ResetForNextQuery()
	}
}

func (preInsert *PreInsert) Free(proc *process.Process, pipelineFailed bool, err error) {
	preInsert.ctr.autoIncrementGenerated = nil
	preInsert.ctr.odkuOrdinals = nil
	if preInsert.ctr.compPkExecutor != nil {
		preInsert.ctr.compPkExecutor.Free()
		preInsert.ctr.compPkExecutor = nil
	}
	if preInsert.ctr.clusterByExecutor != nil {
		preInsert.ctr.clusterByExecutor.Free()
		preInsert.ctr.clusterByExecutor = nil
	}
	if preInsert.ctr.buf != nil {
		// Teardown must agree with conversion on actual ownership. Key the
		// detach on ownedVecSlots, not on the canFreeVecIdx intent: a marked
		// slot whose conversion was interrupted (the failing UnionBatch of an
		// earlier marked slot returned the error first) still holds the
		// child vector an earlier batch borrowed, and freeing that entry
		// would release child-owned storage and zero the child vector's
		// length. Genuinely owned entries - converted/expanded columns and
		// the hidden composite-key, cluster-by, rowid and provenance
		// vectors this operator installed - are recorded in ownedVecSlots at
		// their install sites and are left for buf.Clean to release.
		for idx := range preInsert.ctr.buf.Vecs {
			if !preInsert.ctr.ownedVecSlots[idx] {
				preInsert.ctr.buf.SetVector(int32(idx), nil)
			}
		}
		preInsert.ctr.buf.Clean(proc.Mp())
		preInsert.ctr.buf = nil
	}
}
