// Copyright 2021 - 2026 Matrix Origin
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

package frontend

import (
	"context"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

const maxMigrateUserDefinedVarsSize = 16 << 20

const migrationNextTxnIsolationKey = transactionIsolationSystemVariable + ":next"

func isMigrationSnapshotSizeLimitError(err error) bool {
	return err != nil &&
		moerr.IsMoErrCode(err, moerr.ErrInternal) &&
		strings.Contains(err.Error(), "connection migration size limit")
}

func (ses *Session) hasUnreplayableMigrationUserVars() bool {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	for _, variable := range ses.userDefinedVars {
		if variable == nil || !variable.Replayable {
			return true
		}
	}
	return false
}

func hasMigrationRuntimeSideEffect(name string) bool {
	switch canonicalSystemVariableName(name) {
	case "optimizer_hints", "runtime_filter_limit_in", "runtime_filter_limit_bloom_filter":
		return true
	default:
		return false
	}
}

func (ses *Session) snapshotUserDefinedVars(ctx context.Context) ([]*query.MigrateUserDefinedVar, error) {
	ses.mu.Lock()
	defer ses.mu.Unlock()

	names := make([]string, 0, len(ses.userDefinedVars))
	for name := range ses.userDefinedVars {
		names = append(names, name)
	}
	sort.Strings(names)

	result := make([]*query.MigrateUserDefinedVar, 0, len(names))
	for _, name := range names {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		variable := ses.userDefinedVars[name]
		if variable == nil {
			return nil, moerr.NewInternalErrorf(ctx, "cannot migrate nil user variable %q", name)
		}
		value, err := encodeUserDefinedVarValue(ctx, variable.Value, variable.IsBin)
		if err != nil {
			return nil, err
		}
		item := &query.MigrateUserDefinedVar{
			Name:             name,
			Value:            value,
			Sql:              variable.Sql,
			IsBin:            variable.IsBin,
			PrepareParamKind: uint32(variable.PrepareParamKind),
		}
		result = append(result, item)
	}
	if migrateUserDefinedVarsProtoSize(result) > maxMigrateUserDefinedVarsSize {
		return nil, moerr.NewInternalError(ctx, "user variables exceed the connection migration size limit")
	}
	return result, nil
}

func (ses *Session) snapshotSessionSystemVars(ctx context.Context) ([]*query.MigrateSystemVariable, error) {
	names := make([]string, 0, len(gSysVarsDefs))
	seen := make(map[string]struct{}, len(gSysVarsDefs))
	for name, def := range gSysVarsDefs {
		if def.Scope == ScopeGlobal || !def.Dynamic {
			continue
		}
		canonicalName := canonicalSystemVariableName(name)
		if _, ok := seen[canonicalName]; ok {
			continue
		}
		seen[canonicalName] = struct{}{}
		names = append(names, canonicalName)
	}
	sort.Strings(names)
	var nextTxnIsolationValue string
	var hasNextTxnIsolation bool
	if isolation, ok := ses.GetTxnHandler().nextTxnIsolationSnapshot(); ok {
		var supported bool
		nextTxnIsolationValue, supported = txnIsolationToSystemValue(isolation)
		if !supported {
			return nil, moerr.NewInternalError(ctx, "cannot migrate unsupported next transaction isolation")
		}
		hasNextTxnIsolation = true
	}

	result := make([]*query.MigrateSystemVariable, 0, len(names))
	for _, name := range names {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		value, err := ses.GetSessionSysVar(name)
		if err != nil {
			return nil, err
		}
		encoded, err := encodeUserDefinedVarValue(ctx, value, false)
		if err != nil {
			return nil, moerr.NewInternalErrorf(ctx,
				"cannot encode session system variable %q for connection migration: %v", name, err)
		}
		result = append(result, &query.MigrateSystemVariable{
			Name:  name,
			Value: encoded,
		})
		if hasMigrationRuntimeSideEffect(name) {
			if runtimeValue, ok := moruntime.ServiceRuntime(ses.service).GetGlobalVariables(name); ok {
				runtimeEncoded, err := encodeUserDefinedVarValue(ctx, runtimeValue, false)
				if err != nil {
					return nil, moerr.NewInternalErrorf(ctx,
						"cannot encode runtime side effect %q for connection migration: %v", name, err)
				}
				result[len(result)-1].RuntimeValue = runtimeEncoded
			}
		}
		if name == transactionIsolationSystemVariable && hasNextTxnIsolation {
			nextEncoded, err := encodeUserDefinedVarValue(ctx, nextTxnIsolationValue, false)
			if err != nil {
				return nil, moerr.NewInternalErrorf(ctx,
					"cannot encode next transaction isolation for connection migration: %v", err)
			}
			result = append(result, &query.MigrateSystemVariable{
				Name:            name,
				Value:           nextEncoded,
				NextTransaction: true,
			})
		}
	}
	if (&query.MigrateConnToRequest{SystemVariables: result}).ProtoSize() > maxMigrateUserDefinedVarsSize {
		return nil, moerr.NewInternalError(ctx, "session system variables exceed the connection migration size limit")
	}
	return result, nil
}

func decodeUserDefinedVars(
	ctx context.Context,
	vars []*query.MigrateUserDefinedVar,
	replayable bool,
) (map[string]*UserDefinedVar, error) {
	if migrateUserDefinedVarsProtoSize(vars) > maxMigrateUserDefinedVarsSize {
		return nil, moerr.NewInternalError(ctx, "user variables exceed the connection migration size limit")
	}
	result := make(map[string]*UserDefinedVar, len(vars))
	for _, item := range vars {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		if item == nil || item.Name == "" {
			return nil, moerr.NewInternalError(ctx, "invalid user variable in connection migration")
		}
		name := strings.ToLower(item.Name)
		if _, exists := result[name]; exists {
			return nil, moerr.NewInternalErrorf(ctx, "duplicate user variable %q in connection migration", name)
		}
		if item.PrepareParamKind > uint32(vector.PrepareParamBoolean) {
			return nil, moerr.NewInternalErrorf(ctx, "invalid prepare parameter kind for user variable %q", name)
		}
		value, err := decodeUserDefinedVarValue(ctx, item.Value)
		if err != nil {
			return nil, err
		}
		result[name] = &UserDefinedVar{
			Value:            value,
			Sql:              item.Sql,
			IsBin:            item.IsBin,
			PrepareParamKind: vector.PrepareParamKind(item.PrepareParamKind),
			Replayable:       replayable,
		}
	}
	return result, nil
}

type migratedSystemVariable struct {
	name                string
	value               any
	runtimeValue        any
	runtimeValuePresent bool
	nextTransaction     bool
}

func decodeSessionSystemVars(ctx context.Context, vars []*query.MigrateSystemVariable) ([]migratedSystemVariable, error) {
	if (&query.MigrateConnToRequest{SystemVariables: vars}).ProtoSize() > maxMigrateUserDefinedVarsSize {
		return nil, moerr.NewInternalError(ctx, "session system variables exceed the connection migration size limit")
	}
	seen := make(map[string]struct{}, len(vars)*2)
	result := make([]migratedSystemVariable, 0, len(vars))
	for _, item := range vars {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		if item == nil || item.Name == "" {
			return nil, moerr.NewInternalError(ctx, "invalid session system variable in connection migration")
		}
		name := canonicalSystemVariableName(item.Name)
		if item.NextTransaction && name != transactionIsolationSystemVariable {
			return nil, moerr.NewInternalErrorf(ctx,
				"next transaction scope is invalid for session system variable %q", name)
		}
		seenKey := name
		if item.NextTransaction {
			seenKey += ":next"
		}
		if _, exists := seen[seenKey]; exists {
			return nil, moerr.NewInternalErrorf(ctx, "duplicate session system variable %q in connection migration", name)
		}
		seen[seenKey] = struct{}{}
		if _, ok := gSysVarsDefs[name]; !ok {
			return nil, moerr.NewInternalErrorf(ctx, "unknown session system variable %q in connection migration", name)
		}
		value, err := decodeUserDefinedVarValue(ctx, item.Value)
		if err != nil {
			return nil, err
		}
		var runtimeValue any
		runtimeValuePresent := item.RuntimeValue != nil
		if runtimeValuePresent {
			if !hasMigrationRuntimeSideEffect(name) {
				return nil, moerr.NewInternalErrorf(ctx,
					"runtime side effect is invalid for session system variable %q", name)
			}
			runtimeValue, err = decodeUserDefinedVarValue(ctx, item.RuntimeValue)
			if err != nil {
				return nil, err
			}
		}
		if item.NextTransaction {
			if _, err := txnIsolationFromSystemValue(ctx, value); err != nil {
				return nil, err
			}
		}
		result = append(result, migratedSystemVariable{
			name:                name,
			value:               value,
			runtimeValue:        runtimeValue,
			runtimeValuePresent: runtimeValuePresent,
			nextTransaction:     item.NextTransaction,
		})
	}
	return result, nil
}

func migrateUserDefinedVarsProtoSize(vars []*query.MigrateUserDefinedVar) int {
	return (&query.MigrateConnToRequest{UserDefinedVars: vars}).ProtoSize()
}

func (ses *Session) installUserDefinedVars(vars map[string]*UserDefinedVar) {
	ses.mu.Lock()
	defer ses.mu.Unlock()
	ses.userDefinedVars = vars
}

func encodeUserDefinedVarValue(ctx context.Context, value any, isBin bool) (*plan.Expr, error) {
	switch v := value.(type) {
	case nil:
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_any)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}, nil
	case bool:
		return plan2.MakePlan2BoolConstExprWithType(v), nil
	case int8:
		return plan2.MakePlan2Int8ConstExprWithType(v), nil
	case int16:
		return plan2.MakePlan2Int16ConstExprWithType(v), nil
	case int32:
		return plan2.MakePlan2Int32ConstExprWithType(v), nil
	case int:
		return plan2.MakePlan2Int64ConstExprWithType(int64(v)), nil
	case int64:
		return plan2.MakePlan2Int64ConstExprWithType(v), nil
	case uint8:
		return plan2.MakePlan2Uint8ConstExprWithType(v), nil
	case uint16:
		return plan2.MakePlan2Uint16ConstExprWithType(v), nil
	case uint32:
		return plan2.MakePlan2Uint32ConstExprWithType(v), nil
	case uint:
		return plan2.MakePlan2Uint64ConstExprWithType(uint64(v)), nil
	case uint64:
		return plan2.MakePlan2Uint64ConstExprWithType(v), nil
	case types.MoYear:
		return makeUserVarFixedVectorLiteral(types.T_year, types.EncodeFixed(v)), nil
	case float32:
		return plan2.MakePlan2Float32ConstExprWithType(v), nil
	case float64:
		return plan2.MakePlan2Float64ConstExprWithType(v), nil
	case string:
		return plan2.MakePlan2StringConstExprWithType(v, isBin), nil
	case types.Enum:
		return makeUserVarLiteral(types.T_enum, &plan.Literal{Value: &plan.Literal_EnumVal{EnumVal: uint32(v)}}, 0), nil
	case types.Decimal64:
		return makeUserVarFixedVectorLiteral(types.T_decimal64, types.EncodeFixed(v)), nil
	case types.Decimal128:
		return makeUserVarFixedVectorLiteral(types.T_decimal128, types.EncodeFixed(v)), nil
	case types.Decimal256:
		return makeUserVarFixedVectorLiteral(types.T_decimal256, types.EncodeFixed(v)), nil
	case []float32:
		return makeUserVarVectorLiteral(types.T_array_float32, types.ArrayToBytes(v), len(v)), nil
	case []float64:
		return makeUserVarVectorLiteral(types.T_array_float64, types.ArrayToBytes(v), len(v)), nil
	case []types.BF16:
		return makeUserVarVectorLiteral(types.T_array_bf16, types.ArrayToBytes(v), len(v)), nil
	case []types.Float16:
		return makeUserVarVectorLiteral(types.T_array_float16, types.ArrayToBytes(v), len(v)), nil
	case []int8:
		return makeUserVarVectorLiteral(types.T_array_int8, types.ArrayToBytes(v), len(v)), nil
	case []uint8:
		return makeUserVarVectorLiteral(types.T_array_uint8, types.ArrayToBytes(v), len(v)), nil
	default:
		return nil, moerr.NewInternalErrorf(ctx, "unsupported user variable type %T in connection migration", value)
	}
}

func makeUserVarLiteral(typ types.T, literal *plan.Literal, width int32) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(typ), Width: width, NotNullable: true},
		Expr: &plan.Expr_Lit{Lit: literal},
	}
}

func makeUserVarVectorLiteral(typ types.T, value []byte, width int) *plan.Expr {
	return makeUserVarLiteral(typ, &plan.Literal{Value: &plan.Literal_VecVal{VecVal: string(value)}}, int32(width))
}

func makeUserVarFixedVectorLiteral(typ types.T, value []byte) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(typ), NotNullable: true},
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 1, Data: append([]byte(nil), value...)}},
	}
}

func decodeUserDefinedVarValue(ctx context.Context, expr *plan.Expr) (any, error) {
	if expr == nil {
		return nil, moerr.NewInternalError(ctx, "invalid user variable value in connection migration")
	}
	if expr.GetVec() != nil {
		return decodeUserDefinedVarFixedValue(ctx, expr)
	}
	if expr.GetLit() == nil {
		return nil, moerr.NewInternalError(ctx, "invalid user variable value in connection migration")
	}
	lit := expr.GetLit()
	if lit.Isnull {
		return nil, nil
	}
	invalid := func() (any, error) {
		return nil, moerr.NewInternalErrorf(ctx, "invalid %s user variable value in connection migration", types.T(expr.Typ.Id).String())
	}
	switch types.T(expr.Typ.Id) {
	case types.T_bool:
		v, ok := lit.Value.(*plan.Literal_Bval)
		if !ok {
			return invalid()
		}
		return v.Bval, nil
	case types.T_int8:
		v, ok := lit.Value.(*plan.Literal_I8Val)
		if !ok {
			return invalid()
		}
		return int8(v.I8Val), nil
	case types.T_int16:
		v, ok := lit.Value.(*plan.Literal_I16Val)
		if !ok {
			return invalid()
		}
		return int16(v.I16Val), nil
	case types.T_int32:
		v, ok := lit.Value.(*plan.Literal_I32Val)
		if !ok {
			return invalid()
		}
		return v.I32Val, nil
	case types.T_int64:
		v, ok := lit.Value.(*plan.Literal_I64Val)
		if !ok {
			return invalid()
		}
		return v.I64Val, nil
	case types.T_uint8:
		v, ok := lit.Value.(*plan.Literal_U8Val)
		if !ok {
			return invalid()
		}
		return uint8(v.U8Val), nil
	case types.T_uint16:
		v, ok := lit.Value.(*plan.Literal_U16Val)
		if !ok {
			return invalid()
		}
		return uint16(v.U16Val), nil
	case types.T_uint32:
		v, ok := lit.Value.(*plan.Literal_U32Val)
		if !ok {
			return invalid()
		}
		return v.U32Val, nil
	case types.T_uint64:
		v, ok := lit.Value.(*plan.Literal_U64Val)
		if !ok {
			return invalid()
		}
		return v.U64Val, nil
	case types.T_float32:
		v, ok := lit.Value.(*plan.Literal_Fval)
		if !ok {
			return invalid()
		}
		return v.Fval, nil
	case types.T_float64:
		v, ok := lit.Value.(*plan.Literal_Dval)
		if !ok {
			return invalid()
		}
		return v.Dval, nil
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_text, types.T_blob, types.T_datalink:
		v, ok := lit.Value.(*plan.Literal_Sval)
		if !ok {
			return invalid()
		}
		return v.Sval, nil
	case types.T_enum:
		v, ok := lit.Value.(*plan.Literal_EnumVal)
		if !ok {
			return invalid()
		}
		return types.Enum(v.EnumVal), nil
	case types.T_array_float32:
		value, err := decodeUserVarVectorBytes(ctx, expr, lit, 4)
		if err != nil {
			return nil, err
		}
		return types.BytesToArray[float32](value), nil
	case types.T_array_float64:
		value, err := decodeUserVarVectorBytes(ctx, expr, lit, 8)
		if err != nil {
			return nil, err
		}
		return types.BytesToArray[float64](value), nil
	case types.T_array_bf16:
		value, err := decodeUserVarVectorBytes(ctx, expr, lit, 2)
		if err != nil {
			return nil, err
		}
		return types.BytesToArray[types.BF16](value), nil
	case types.T_array_float16:
		value, err := decodeUserVarVectorBytes(ctx, expr, lit, 2)
		if err != nil {
			return nil, err
		}
		return types.BytesToArray[types.Float16](value), nil
	case types.T_array_int8:
		value, err := decodeUserVarVectorBytes(ctx, expr, lit, 1)
		if err != nil {
			return nil, err
		}
		return types.BytesToArray[int8](value), nil
	case types.T_array_uint8:
		value, err := decodeUserVarVectorBytes(ctx, expr, lit, 1)
		if err != nil {
			return nil, err
		}
		return types.BytesToArray[uint8](value), nil
	default:
		return nil, moerr.NewInternalErrorf(ctx, "unsupported user variable type %s in connection migration", types.T(expr.Typ.Id).String())
	}
}

func decodeUserDefinedVarFixedValue(ctx context.Context, expr *plan.Expr) (any, error) {
	vec := expr.GetVec()
	if vec.Len != 1 {
		return nil, moerr.NewInternalError(ctx, "invalid fixed user variable cardinality in connection migration")
	}
	invalidSize := func(expected int) error {
		if len(vec.Data) != expected {
			return moerr.NewInternalError(ctx, "invalid fixed user variable length in connection migration")
		}
		return nil
	}
	switch types.T(expr.Typ.Id) {
	case types.T_year:
		if err := invalidSize(2); err != nil {
			return nil, err
		}
		return types.DecodeFixed[types.MoYear](vec.Data), nil
	case types.T_decimal64:
		if err := invalidSize(8); err != nil {
			return nil, err
		}
		return types.DecodeFixed[types.Decimal64](vec.Data), nil
	case types.T_decimal128:
		if err := invalidSize(16); err != nil {
			return nil, err
		}
		return types.DecodeFixed[types.Decimal128](vec.Data), nil
	case types.T_decimal256:
		if err := invalidSize(32); err != nil {
			return nil, err
		}
		return types.DecodeFixed[types.Decimal256](vec.Data), nil
	default:
		return nil, moerr.NewInternalErrorf(ctx, "unsupported fixed user variable type %s in connection migration", types.T(expr.Typ.Id).String())
	}
}

func decodeUserVarVectorBytes(ctx context.Context, expr *plan.Expr, lit *plan.Literal, elementSize int) ([]byte, error) {
	value, ok := lit.Value.(*plan.Literal_VecVal)
	if !ok {
		return nil, moerr.NewInternalError(ctx, "invalid vector user variable value in connection migration")
	}
	raw := []byte(value.VecVal)
	if len(raw)%elementSize != 0 || expr.Typ.Width != int32(len(raw)/elementSize) {
		return nil, moerr.NewInternalError(ctx, "invalid vector user variable length in connection migration")
	}
	return raw, nil
}
