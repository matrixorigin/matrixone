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

package window

import (
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ValidateIntervalFrameBounds checks execution-time arguments even when a
// physical shortcut (such as LIMIT 0) omits the window operator. Validation
// borrows the logical plan and discards normalized copies; Prepare still owns
// the operator's runtime frames. Detached optimizer nodes are not executable.
func ValidateIntervalFrameBounds(proc *process.Process, pn *plan.Plan) error {
	query := pn.GetQuery()
	if query == nil {
		query = pn.GetDdl().GetQuery()
	}
	if query == nil {
		return nil
	}
	visited := make([]bool, len(query.Nodes))
	pending := append([]int32(nil), query.Steps...)
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if id < 0 || int(id) >= len(query.Nodes) || visited[id] || query.Nodes[id] == nil {
			continue
		}
		visited[id] = true
		node := query.Nodes[id]
		for _, expr := range node.WinSpecList {
			frame := expr.GetW().GetFrame()
			if frame == nil || frame.Type != plan.FrameClause_RANGE {
				continue
			}
			for _, bound := range []*plan.FrameBound{frame.Start, frame.End} {
				if bound == nil || bound.UnBounded || bound.Type == plan.FrameBound_CURRENT_ROW ||
					bound.Val == nil || bound.Val.Typ.Id != int32(types.T_interval) {
					continue
				}
				if _, err := materializeWindowInterval(proc, bound.Val); err != nil {
					return err
				}
			}
		}
		pending = append(pending, node.Children...)
	}
	return nil
}

func materializeWindowInterval(proc *process.Process, expr *plan.Expr) (*plan.Expr, error) {
	list := expr.GetList()
	invalid := func() (*plan.Expr, error) {
		return nil, moerr.NewInvalidInputNoCtx("invalid window interval bound")
	}
	if expr.Typ.Id != int32(types.T_interval) || list == nil || len(list.List) != 2 || list.List[0] == nil || list.List[1] == nil || list.List[1].GetLit() == nil || list.List[1].GetLit().Isnull {
		return invalid()
	}
	source, unitExpr := list.List[0], list.List[1]
	if unit, ok := unitExpr.GetLit().Value.(*plan.Literal_I64Val); ok {
		// Normalized plans are immutable. The unit's type distinguishes them
		// from a raw interval whose parameter was replaced by a literal.
		if unitExpr.Typ.Id != int32(types.T_int64) || unit.I64Val <= int64(types.IntervalTypeInvalid) || unit.I64Val >= int64(types.IntervalTypeMax) || source.Typ.Id != int32(types.T_int64) || source.GetLit() == nil {
			return invalid()
		}
		if !source.GetLit().Isnull {
			value, ok := source.GetLit().Value.(*plan.Literal_I64Val)
			if !ok || value.I64Val < 0 {
				return invalid()
			}
		}
		return expr, nil
	}
	if !types.T(unitExpr.Typ.Id).IsMySQLString() || proc == nil {
		return invalid()
	}
	unit, err := types.IntervalTypeOf(unitExpr.GetLit().GetSval())
	if err != nil {
		return nil, err
	}
	executor, err := colexec.NewExpressionExecutor(proc, source)
	if err != nil {
		return nil, err
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return nil, err
	}
	if vec == nil || vec.Length() != 1 || vec.IsNull(0) {
		return nil, moerr.NewParseError(proc.Ctx, "Window '<unnamed window>': frame start or end is negative, NULL or of non-integral type")
	}
	text, numeric, err := windowIntervalScalarText(vec)
	if err != nil {
		return nil, err
	}
	value, unit, null, err := types.NormalizeWindowInterval(text, unit, numeric)
	if err != nil {
		return nil, err
	}
	if !null {
		if err := types.JudgeIntervalNumOverflow(value, unit); err != nil {
			return nil, err
		}
		if !temporalRangeIntervalConversionOK(value, unit) {
			return nil, moerr.NewOutOfRange(proc.Ctx, "int64", "window interval")
		}
	}
	requiresProtocol, err := plan.RequiresMORPCVersion89DecimalLiteralSemantics(source)
	if err != nil {
		return nil, err
	}
	magnitude := intervalInt64(value)
	magnitude.GetLit().Isnull = null
	magnitude.Typ.NotNullable = !null
	magnitude.GetLit().DecimalLiteralRequiresV82 = requiresProtocol || source.Typ.Id == int32(types.T_decimal256)
	return &plan.Expr{Typ: expr.Typ, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
		magnitude, intervalInt64(int64(unit)),
	}}}}, nil
}

func intervalInt64(value int64) *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: value}}}}
}

func hasNullIntervalBound(frame *plan.FrameClause) bool {
	for _, bound := range []*plan.FrameBound{frame.Start, frame.End} {
		if bound == nil || bound.UnBounded || bound.Type == plan.FrameBound_CURRENT_ROW || bound.Val == nil || bound.Val.Typ.Id != int32(types.T_interval) {
			continue
		}
		list := bound.Val.GetList()
		if list != nil && len(list.List) == 2 && list.List[0].GetLit() != nil && list.List[0].GetLit().Isnull {
			return true
		}
	}
	return false
}

func windowIntervalScalarText(vec *vector.Vector) (text string, numeric bool, err error) {
	numeric = true
	switch vec.GetType().Oid {
	case types.T_char, types.T_varchar, types.T_text, types.T_binary, types.T_varbinary, types.T_blob:
		return vec.GetStringAt(0), false, nil
	case types.T_bool:
		text = "0"
		if vector.MustFixedColNoTypeCheck[bool](vec)[0] {
			text = "1"
		}
	case types.T_int8:
		text = fmt.Sprint(vector.MustFixedColNoTypeCheck[int8](vec)[0])
	case types.T_int16:
		text = fmt.Sprint(vector.MustFixedColNoTypeCheck[int16](vec)[0])
	case types.T_int32:
		text = fmt.Sprint(vector.MustFixedColNoTypeCheck[int32](vec)[0])
	case types.T_int64:
		text = fmt.Sprint(vector.MustFixedColNoTypeCheck[int64](vec)[0])
	case types.T_uint8:
		text = fmt.Sprint(vector.MustFixedColNoTypeCheck[uint8](vec)[0])
	case types.T_uint16:
		text = fmt.Sprint(vector.MustFixedColNoTypeCheck[uint16](vec)[0])
	case types.T_uint32:
		text = fmt.Sprint(vector.MustFixedColNoTypeCheck[uint32](vec)[0])
	case types.T_uint64, types.T_bit:
		text = fmt.Sprint(vector.MustFixedColNoTypeCheck[uint64](vec)[0])
	case types.T_float32:
		text = strconv.FormatFloat(float64(vector.MustFixedColNoTypeCheck[float32](vec)[0]), 'f', -1, 32)
	case types.T_float64:
		text = strconv.FormatFloat(vector.MustFixedColNoTypeCheck[float64](vec)[0], 'f', -1, 64)
	case types.T_decimal64:
		text = vector.MustFixedColNoTypeCheck[types.Decimal64](vec)[0].Format(vec.GetType().Scale)
	case types.T_decimal128:
		text = vector.MustFixedColNoTypeCheck[types.Decimal128](vec)[0].Format(vec.GetType().Scale)
	case types.T_decimal256:
		text = vector.MustFixedColNoTypeCheck[types.Decimal256](vec)[0].Format(vec.GetType().Scale)
	default:
		err = moerr.NewInvalidInputNoCtxf("invalid window interval source type %s", vec.GetType())
	}
	return
}
