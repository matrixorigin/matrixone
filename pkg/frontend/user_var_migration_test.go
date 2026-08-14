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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
)

func TestUserDefinedVarMigrationRoundTrip(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)

	values := map[string]any{
		"null_value":   nil,
		"bool_value":   true,
		"i8_value":     int8(-8),
		"i16_value":    int16(-16),
		"i32_value":    int32(-32),
		"i64_value":    int64(-64),
		"u8_value":     uint8(8),
		"u16_value":    uint16(16),
		"u32_value":    uint32(32),
		"u64_value":    uint64(64),
		"f32_value":    float32(1.25),
		"f64_value":    2.5,
		"text_value":   "timestamp-value",
		"enum_value":   types.Enum(3),
		"year_value":   types.MoYear(2026),
		"dec64_value":  types.Decimal64(12345),
		"dec128_value": types.Decimal128{B0_63: 123, B64_127: 456},
		"dec256_value": types.Decimal256{B0_63: 1, B64_127: 2, B128_191: 3, B192_255: 4},
		"vec_value":    []float32{1, 2, 3},
	}
	for name, value := range values {
		require.NoError(t, ses.setUserDefinedVarWithKind(name, value, "set @"+name+" = expression", name == "text_value", vector.PrepareParamDecimal))
	}

	snapshot, err := ses.snapshotUserDefinedVars(context.Background())
	require.NoError(t, err)
	require.Len(t, snapshot, len(values))
	for i := 1; i < len(snapshot); i++ {
		require.Less(t, snapshot[i-1].Name, snapshot[i].Name)
	}

	values["vec_value"].([]float32)[0] = 99
	restored, err := decodeUserDefinedVars(context.Background(), snapshot)
	require.NoError(t, err)
	for name, value := range values {
		if name == "vec_value" {
			require.Equal(t, []float32{1, 2, 3}, restored[name].Value)
			continue
		}
		require.Equal(t, value, restored[name].Value)
		require.Equal(t, "set @"+name+" = expression", restored[name].Sql)
		require.Equal(t, vector.PrepareParamDecimal, restored[name].PrepareParamKind)
	}
}

func TestDecodeUserDefinedVarsIsAtomic(t *testing.T) {
	_, err := encodeUserDefinedVarValue(context.Background(), struct{}{}, false)
	require.ErrorContains(t, err, "unsupported user variable type")

	valid := &query.MigrateUserDefinedVar{
		Name:  "kept",
		Value: &plan.Expr{Typ: plan.Type{Id: int32(types.T_any)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}},
	}
	invalid := &query.MigrateUserDefinedVar{Name: "broken", Value: &plan.Expr{}}
	result, err := decodeUserDefinedVars(context.Background(), []*query.MigrateUserDefinedVar{valid, invalid})
	require.Error(t, err)
	require.Nil(t, result)

	malformedVector := &query.MigrateUserDefinedVar{
		Name: "vector",
		Value: &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_array_float32), Width: 1},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "bad"}}},
		},
	}
	result, err = decodeUserDefinedVars(context.Background(), []*query.MigrateUserDefinedVar{malformedVector})
	require.ErrorContains(t, err, "vector user variable length")
	require.Nil(t, result)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = decodeUserDefinedVars(canceled, []*query.MigrateUserDefinedVar{valid})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
}

func TestDecodeUserDefinedVarsRejectsDuplicateAndOversize(t *testing.T) {
	value, err := encodeUserDefinedVarValue(context.Background(), "value", false)
	require.NoError(t, err)
	_, err = decodeUserDefinedVars(context.Background(), []*query.MigrateUserDefinedVar{
		{Name: "Var", Value: value},
		{Name: "var", Value: value},
	})
	require.ErrorContains(t, err, "duplicate user variable")

	largeValue, err := encodeUserDefinedVarValue(context.Background(), string(make([]byte, maxMigrateUserDefinedVarsSize)), false)
	require.NoError(t, err)
	_, err = decodeUserDefinedVars(context.Background(), []*query.MigrateUserDefinedVar{{Name: "large", Value: largeValue}})
	require.ErrorContains(t, err, "size limit")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	require.NoError(t, ses.SetUserDefinedVar("large", string(make([]byte, maxMigrateUserDefinedVarsSize)), ""))
	_, err = ses.snapshotUserDefinedVars(context.Background())
	require.ErrorContains(t, err, "size limit")

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = ses.snapshotUserDefinedVars(canceled)
	require.ErrorIs(t, err, context.Canceled)
}

func TestUserDefinedVarRepeatedMigrationDoesNotReevaluateExpressions(t *testing.T) {
	source := &Session{userDefinedVars: map[string]*UserDefinedVar{
		"random_value": {
			Value: "0.123456789",
			Sql:   "set @random_value = rand()",
		},
		"table_value": {
			Value: "2026-08-07 04:20:01.123456",
			Sql:   "set @table_value = (select updated_at from source_table limit 1)",
		},
	}}
	for i := 0; i < 100; i++ {
		snapshot, err := source.snapshotUserDefinedVars(context.Background())
		require.NoError(t, err)
		restored, err := decodeUserDefinedVars(context.Background(), snapshot)
		require.NoError(t, err)
		target := &Session{userDefinedVars: make(map[string]*UserDefinedVar)}
		target.installUserDefinedVars(restored)
		require.Equal(t, "0.123456789", target.userDefinedVars["random_value"].Value)
		require.Equal(t, "2026-08-07 04:20:01.123456", target.userDefinedVars["table_value"].Value)
		source = target
	}
}
