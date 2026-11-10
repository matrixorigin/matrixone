// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loopjoin

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// A direct column lookup cannot introduce an earlier expression error. Merely
// proving determinism or non-nullability would not justify eager RHS evaluation.
func (loopJoin *LoopJoin) prepareMembership() {
	ctr := &loopJoin.ctr
	ctr.markProbeCol, ctr.markBuildCol = -1, -1
	ctr.markWork = 0
	if loopJoin.JoinType != plan.Node_MARK {
		return
	}
	f := loopJoin.NonEqCond.GetF()
	if f == nil || f.Func == nil || len(f.Args) != 2 {
		return
	}
	id, _ := function.DecodeOverloadID(f.Func.Obj)
	if id != function.EQUAL {
		return
	}
	a, b := f.Args[0], f.Args[1]
	left, right := a.GetCol(), b.GetCol()
	if left == nil || right == nil {
		return
	}
	if left.RelPos == 1 && right.RelPos == 0 {
		left, right = right, left
	}
	if left.RelPos != 0 || right.RelPos != 1 || left.ColPos < 0 || right.ColPos < 0 ||
		a.Typ.Id != b.Typ.Id || a.Typ.Width != b.Typ.Width || a.Typ.Scale != b.Typ.Scale {
		return
	}
	typ := types.T(a.Typ.Id).ToType()
	typ.Width, typ.Scale = a.Typ.Width, a.Typ.Scale
	if !message.SupportsMembershipType(typ) {
		return
	}
	ctr.markProbeCol, ctr.markBuildCol = int(left.ColPos), int(right.ColPos)
	ctr.markType = typ
}

// Credit only completed, nonconstant fixed-width evaluation. A real NULL
// predicate result can be FLAT; inspect the input key, not IsConst alone.
func (ctr *container) creditMembershipWork(probe *vector.Vector, row int, rhs *vector.Vector, rows int, result *vector.Vector) {
	if probe.IsConst() {
		row = 0
	}
	if probe.IsConstNull() || probe.GetNulls().Contains(uint64(row)) || rhs.IsConst() || result.IsConst() {
		return
	}
	credit := uint64(min(rows, result.Length()))
	// Normally at most one probe's RHS work is pending. Also saturate locally
	// so even malformed published counts cannot wrap a work increment.
	ctr.markWork += min(credit, ^uint64(0)-ctr.markWork)
}

func (ctr *container) probeMembership(ap *LoopJoin, index *message.MembershipIndex, proc *process.Process, result *vm.CallResult, prefix int) error {
	in := ctr.inBat
	key := in.Vecs[ctr.markProbeCol]
	start := ctr.probeIdx
	end := min(in.RowCount(), start+colexec.DefaultBatchSize-prefix)
	for row := start; row < end; row++ {
		matched, unknown, err := index.Probe(proc.Ctx, key, row)
		if err != nil {
			return err
		}
		for k, rp := range ap.ResultCols {
			if rp.Rel == 0 {
				err = ctr.resBat.Vecs[k].UnionOne(in.Vecs[rp.Pos], int64(row), proc.Mp())
			} else {
				err = vector.AppendFixed(ctr.resBat.Vecs[k], matched, unknown, proc.Mp())
			}
			if err != nil {
				return err
			}
		}
	}
	ctr.resBat.SetRowCount(prefix + end - start)
	ctr.probeIdx = end
	if end == in.RowCount() {
		ctr.inBat = nil
	}
	result.Batch = ctr.resBat
	return nil
}
