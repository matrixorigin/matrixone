// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package odkueval

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// CompileUpdateChanges uses the same typed SQL equality as the former planner
// no-op guard. Physical key identity and final storage equality are separate.
func CompileUpdateChanges(columns []int32, imageTypes []types.Type, proc *process.Process, account *mpool.AllocationAccount) ([]colexec.ExpressionExecutor, error) {
	if len(columns) == 0 {
		return nil, nil
	}
	exprs := make([]*plan.Expr, len(columns))
	for i, column := range columns {
		if column < 0 || int(column) >= len(imageTypes) {
			return nil, moerr.NewInternalError(proc.Ctx, "invalid ODKU change column")
		}
		t := imageTypes[column]
		f, err := function.GetFunctionByName(proc.Ctx, "<=>", []types.Type{t, t})
		if err != nil {
			return nil, err
		}
		typ := plan.Type{Id: int32(t.Oid), Width: t.Width, Scale: t.Scale}
		exprs[i] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: f.GetEncodedOverloadID(), ObjName: "<=>"},
			Args: []*plan.Expr{
				{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: column}}},
				{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: column}}},
			},
		}}}
	}
	return hashbuild.NewExpressionExecutors(proc, exprs, account)
}

// UpdateChanged compares the candidate with the immediately preceding image.
// Both inputs remain borrowed; no expression is re-evaluated here.
func UpdateChanged(execs []colexec.ExpressionExecutor, columns []int32, before, after *batch.Batch, proc *process.Process) (bool, error) {
	for i, exec := range execs {
		left, right := before.Vecs[columns[i]], after.Vecs[columns[i]]
		equal, known := false, false
		if left.Length() == 1 && right.Length() == 1 {
			equal, known = IdenticalUpdateValueAt(left, 0, right, 0)
		}
		if known {
			if !equal {
				return true, nil
			}
			continue
		}
		v, err := exec.Eval(proc, []*batch.Batch{before, after}, nil)
		if err != nil {
			return false, err
		}
		if v.IsNull(0) || !vector.GetFixedAtNoTypeCheck[bool](v, 0) {
			return true, nil
		}
	}
	return false, nil
}

// IdenticalUpdateValueAt is only a sufficient equality proof for the primitive/string
// operators in nullSafeEqualFn. Other types and unequal bytes retain the SQL
// executor, including its floating scale, JSON/array and error semantics.
func IdenticalUpdateValueAt(left *vector.Vector, leftRow int, right *vector.Vector, rightRow int) (equal, known bool) {
	if left == nil || right == nil || leftRow < 0 || rightRow < 0 || leftRow >= left.Length() || rightRow >= right.Length() ||
		left.GetType().Oid != right.GetType().Oid {
		return false, false
	}
	switch left.GetType().Oid {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_enum,
		types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_datalink:
		// These SQL operators ignore Width/Scale. Raw access still requires
		// both canonical physical sizes, including for integer metadata.
		size := left.GetType().Oid.FixedLength()
		if size < 0 {
			size = -size
		}
		if left.GetType().TypeSize() != size || right.GetType().TypeSize() != size {
			return false, false
		}
		ln, rn := left.IsNull(uint64(leftRow)), right.IsNull(uint64(rightRow))
		if ln || rn {
			return ln == rn, true
		}
		if bytes.Equal(left.GetRawBytesAt(leftRow), right.GetRawBytesAt(rightRow)) {
			return true, true
		}
	}
	return false, false
}
