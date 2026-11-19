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

package plan

import (
	"context"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// PreparedDecimalFloatProof belongs to one cached execution plan. Both native
// and generic comparisons retain the expected uniqueness class, so either
// class can reuse the existing single cache entry without freezing a value.
type PreparedDecimalFloatProof struct {
	position       int32
	source         types.T
	column         types.Type
	expectedUnique bool
}

// MatchPreparedDecimalFloatProofs checks the current typed binding independently
// of conversion diagnostics. Missing, NULL, or changed source domains cannot
// authorize reuse of either uniqueness class.
func MatchPreparedDecimalFloatProofs(proofs []PreparedDecimalFloatProof, bindings []PreparedSourceBinding, values []any) bool {
	state := preparedSourceBindingState{bindings: bindings}
	for _, proof := range proofs {
		binding, ok := state.bindingForPosition(proof.position)
		if !ok || binding.Type.Oid != proof.source || proof.position < 0 || int(proof.position) >= len(values) {
			return false
		}
		value, ok := preparedIntegerFloatValue(values[proof.position], proof.source)
		if !ok || decimalFloatComparisonHasUniqueValue(value, proof.column) != proof.expectedUnique {
			return false
		}
	}
	return true
}

func preparedIntegerFloatValue(value any, source types.T) (float64, bool) {
	if param, ok := value.(ParamValue); ok {
		if param.Value == nil {
			return 0, false
		}
		value = param.Value
		if param.MaterializedValue != "" {
			value = param.MaterializedValue
		}
	}
	if !source.IsInteger() {
		return 0, false
	}
	switch value.(type) {
	case string, []byte, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
	default:
		return 0, false
	}
	// Reuse the exact transport spelling, but decode in the integer domain
	// before converting to DOUBLE, just like numericToNumeric. This validates
	// the source integer's signedness and range before entering the float domain.
	spelling := preparedNumericValueSpelling(value)
	bits := source.TypeLen() * 8
	if source.IsUnsignedInt() {
		integer, err := strconv.ParseUint(spelling, 10, bits)
		return float64(integer), err == nil
	}
	integer, err := strconv.ParseInt(spelling, 10, bits)
	return float64(integer), err == nil
}

func bindPreparedDecimalFloatArguments(ctx context.Context, name string, args []*Expr) ([]*Expr, bool, error) {
	state := preparedBindingState(ctx)
	if state == nil || !state.selectStatement || len(args) != 2 || !isDecimalComparisonOperator(name) {
		return args, false, nil
	}
	for side, column := range args {
		if column == nil || column.GetCol() == nil ||
			(column.Typ.Id != int32(types.T_decimal64) && column.Typ.Id != int32(types.T_decimal128)) {
			continue
		}
		peer := args[1-side]
		if peer == nil || peer.Typ.Id != int32(types.T_float64) || peer.Typ.Width > 0 && peer.Typ.Scale >= 0 {
			continue
		}
		fn := peer.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) != 2 ||
			fn.Args[1] == nil || fn.Args[1].GetT() == nil || !isSameColumnType(fn.Args[1].Typ, peer.Typ) {
			continue
		}
		id, overload := planfunction.DecodeOverloadID(fn.Func.Obj)
		if id != planfunction.CAST || overload > 1 {
			continue
		}
		source := preparedDecimalIntegerParam(fn.Args[0])
		if source == nil {
			continue
		}
		position := source.GetP().Pos
		binding, ok := state.bindingForPosition(position)
		if !ok || binding.Type.Oid != types.T(source.Typ.Id) {
			continue
		}
		if position < 0 || int(position) >= len(state.values) {
			state.valueDependent = true
			return args, true, nil
		}
		value, ok := preparedIntegerFloatValue(state.values[position], binding.Type.Oid)
		if !ok {
			state.valueDependent = true
			return args, true, nil
		}
		columnType := makeTypeByPlan2Expr(column)
		unique := decimalFloatComparisonHasUniqueValue(value, columnType)
		state.decimalFloatProofs = append(state.decimalFloatProofs, PreparedDecimalFloatProof{
			position: position, source: binding.Type.Oid, column: columnType, expectedUnique: unique,
		})
		if !unique {
			return args, true, nil
		}
		converted, err := makePlan2CastExpr(ctx, DeepCopyExpr(peer), column.Typ)
		if err != nil {
			return nil, false, err
		}
		result := append([]*Expr(nil), args...)
		result[1-side] = converted
		return result, true, nil
	}
	return args, false, nil
}
