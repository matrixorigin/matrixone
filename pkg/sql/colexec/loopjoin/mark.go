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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// markHash indexes a single, side-local equality predicate. Other predicates
// retain the general MARK evaluator, including correlated and row comparisons.
// HashBuild owns the shared immutable index; each probe owns only its iterator.
type markHash struct {
	keys     [2]colexec.ExpressionExecutor
	iter     hashmap.Iterator
	probeVec *vector.Vector
	hasRows  bool
	hasNull  bool
	hasZero  bool
	keyType  types.T
}

// MarkHashKeys returns probe/build keys when a scalar MARK equality can use
// HashBuild. The compiler and receiver must use the same eligibility rule.
func MarkHashKeys(cond *plan.Expr) []*plan.Expr {
	f := cond.GetF()
	if f == nil || len(f.Args) != 2 {
		return nil
	}
	fid, _ := function.DecodeOverloadID(f.Func.Obj)
	if fid != function.EQUAL {
		return nil
	}
	a, b := f.Args[0], f.Args[1]
	if a.Typ.Id != b.Typ.Id || a.Typ.Scale != b.Typ.Scale || a.Typ.Width != b.Typ.Width {
		return nil
	}
	// These domains use the existing key encoding. Floating-point zero and
	// NaN need the explicit equality handling below. Scaled float32 equality
	// rounds its operands, so it must retain the general evaluator.
	if types.T(a.Typ.Id) == types.T_float32 && a.Typ.Scale > 0 {
		return nil
	}
	t := types.Type{Oid: types.T(a.Typ.Id)}
	if !t.IsIntOrUint() && !t.IsDecimal() && !t.IsTemporal() {
		switch t.Oid {
		case types.T_bool, types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
			types.T_text, types.T_blob, types.T_json, types.T_datalink, types.T_uuid,
			types.T_enum, types.T_Rowid, types.T_float32, types.T_float64:
		default:
			return nil
		}
	}
	left, right := markKeySide(a), markKeySide(b)
	if left >= 0 && right >= 0 && left&^1 == 0 && right&^2 == 0 && left|right != 0 {
		return []*plan.Expr{a, b}
	}
	if left >= 0 && right >= 0 && left&^2 == 0 && right&^1 == 0 && left|right != 0 {
		return []*plan.Expr{b, a}
	}
	return nil
}

func newMarkHash(proc *process.Process, cond *plan.Expr) (*markHash, error) {
	keys := MarkHashKeys(cond)
	if keys == nil {
		return nil, nil
	}
	m := &markHash{keyType: types.T(keys[0].Typ.Id)}
	var err error
	for i, key := range keys {
		m.keys[i], err = colexec.NewExpressionExecutor(proc, key)
		if err != nil {
			m.free()
			return nil, err
		}
	}
	return m, nil
}

// Return the referenced input mask, or -1 for an unsafe expression. Moving a
// volatile call out of the pairwise evaluator would change its evaluation count.
func markKeySide(expr *plan.Expr) int {
	if c := expr.GetCol(); c != nil {
		if c.RelPos < 0 || c.RelPos > 1 {
			return -1
		}
		return 1 << c.RelPos
	}
	if f := expr.GetF(); f != nil {
		ov, ok := function.GetFunctionByIdWithoutError(f.Func.Obj)
		if !ok || ov.CannotFold() || ov.IsRealTimeRelated() {
			return -1
		}
		mask := 0
		for _, arg := range f.Args {
			side := markKeySide(arg)
			if side < 0 {
				return -1
			}
			mask |= side
		}
		return mask
	}
	switch expr.Expr.(type) {
	case *plan.Expr_Lit, *plan.Expr_T:
		return 0
	default:
		return -1
	}
}

func (m *markHash) reset() {
	m.iter = nil
	m.probeVec = nil
	m.hasRows, m.hasNull, m.hasZero = false, false, false
	for _, key := range m.keys {
		if key != nil {
			key.ResetForNextQuery()
		}
	}
}

func (m *markHash) free() {
	m.reset()
	for i, key := range m.keys {
		if key != nil {
			key.Free()
			m.keys[i] = nil
		}
	}
}

func (m *markHash) build(proc *process.Process, jm *message.JoinMap) error {
	m.iter = jm.NewIterator()
	if m.iter == nil {
		return nil // General loop build, not an indexed MARK build.
	}
	for _, bat := range jm.GetBatches() {
		if err := proc.Ctx.Err(); err != nil {
			return err
		}
		if bat.RowCount() == 0 {
			continue
		}
		m.hasRows = true
		// Constants/type expressions take their row count from input zero.
		// Both slots must name this side-local batch, even for build-side keys.
		vec, err := m.keys[1].Eval(proc, []*batch.Batch{bat, bat}, nil)
		if err != nil {
			return err
		}
		m.hasNull = m.hasNull || vec.IsConstNull() || vec.GetNulls().Any()
	}
	if err := proc.Ctx.Err(); err != nil {
		return err
	}
	return m.findFloatZero(proc)
}

// The shared index can contain either sign of zero. Combine their presence
// once without rewriting shared keys or allocating a second membership index.
func (m *markHash) findFloatZero(proc *process.Process) error {
	if m.keyType != types.T_float32 && m.keyType != types.T_float64 {
		return nil
	}
	zeros := vector.NewVec(m.keyType.ToType())
	defer zeros.Free(proc.Mp())
	var err error
	if m.keyType == types.T_float32 {
		err = vector.AppendFixedList(zeros, []float32{0, float32(math.Copysign(0, -1))}, nil, proc.Mp())
	} else {
		err = vector.AppendFixedList(zeros, []float64{0, math.Copysign(0, -1)}, nil, proc.Mp())
	}
	if err != nil {
		return err
	}
	values, _ := m.iter.Find(0, 2, []*vector.Vector{zeros})
	m.hasZero = values[0] != 0 || values[1] != 0
	return nil
}

func (m *markHash) floatMatch(row int, matched bool) bool {
	if m.keyType != types.T_float32 && m.keyType != types.T_float64 {
		return matched
	}
	if m.probeVec.IsConst() {
		row = 0
	}
	var v float64
	if m.keyType == types.T_float32 {
		v = float64(vector.MustFixedColNoTypeCheck[float32](m.probeVec)[row])
	} else {
		v = vector.MustFixedColNoTypeCheck[float64](m.probeVec)[row]
	}
	if math.IsNaN(v) {
		return false
	}
	if v == 0 {
		return m.hasZero
	}
	return matched
}

func (ctr *container) probeMarkHash(ap *LoopJoin, proc *process.Process, result *vm.CallResult) error {
	m := ctr.markHash
	if ctr.probeIdx == 0 {
		var err error
		m.probeVec, err = m.keys[0].Eval(proc, []*batch.Batch{ctr.inBat, ctr.inBat}, nil)
		if err != nil {
			return err
		}
	}
	end := min(ctr.inBat.RowCount(), ctr.probeIdx+colexec.DefaultBatchSize)
	for start := ctr.probeIdx; start < end; start += hashmap.UnitLimit {
		if err := proc.Ctx.Err(); err != nil {
			return err
		}
		count := min(hashmap.UnitLimit, end-start)
		values, nonnull := m.iter.Find(start, count, []*vector.Vector{m.probeVec})
		for i := 0; i < count; i++ {
			matched := nonnull[i] != 0 && m.floatMatch(start+i, values[i] != 0)
			unknown := !matched && m.hasRows && (nonnull[i] == 0 || m.hasNull)
			for k, rp := range ap.ResultCols {
				var err error
				if rp.Rel == 0 {
					err = ctr.resBat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(start+i), proc.Mp())
				} else {
					err = vector.AppendFixed(ctr.resBat.Vecs[k], matched, unknown, proc.Mp())
				}
				if err != nil {
					return err
				}
			}
		}
	}
	ctr.resBat.SetRowCount(end - ctr.probeIdx)
	ctr.probeIdx = end
	if end == ctr.inBat.RowCount() {
		ctr.inBat = nil
		m.probeVec = nil
	}
	result.Batch = ctr.resBat
	return nil
}
