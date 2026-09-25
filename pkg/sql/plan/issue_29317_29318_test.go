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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestPreparedVariadicRuntimeSourceDomains(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sql    string
		fn     string
		values []ParamValue
		want   []types.T
	}{
		{
			name: "greatest decimal and string", sql: "prepare p from 'select greatest(?, ?)'", fn: "greatest",
			values: []ParamValue{
				{Value: "2.0", SourceType: types.New(types.T_decimal64, 2, 1), HasSourceType: true,
					PrepareParamKind: vector.PrepareParamDecimal, EnableNumericPrefix: true},
				{Value: "10", SourceType: types.T_varchar.ToType(), HasSourceType: true, EnableNumericPrefix: true},
			},
			want: []types.T{types.T_varchar, types.T_varchar},
		},
		{
			name: "field decimal and string", sql: "prepare p from 'select field(?, ?)'", fn: "field",
			values: []ParamValue{
				{Value: "2.0", SourceType: types.New(types.T_decimal64, 2, 1), HasSourceType: true},
				{Value: "2", SourceType: types.T_varchar.ToType(), HasSourceType: true},
			},
			want: []types.T{types.T_float64, types.T_float64},
		},
		{
			name: "field decimal with fixed exact candidate",
			sql:  "prepare p from 'select field(?, cast(9007199254740992 as decimal(20,0)))'", fn: "field",
			values: []ParamValue{{
				Value: "9007199254740993", SourceType: types.New(types.T_decimal128, 20, 0),
				HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal,
			}},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field fixed exact needle with decimal candidate",
			sql:  "prepare p from 'select field(cast(9007199254740993 as decimal(20,0)), ?)'", fn: "field",
			values: []ParamValue{{
				Value: "9007199254740992", SourceType: types.New(types.T_decimal128, 20, 0),
				HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal,
			}},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field nested abs decimal with fixed exact candidate",
			sql:  "prepare p from 'select field(abs(?), cast(9007199254740992 as decimal(20,0)))'", fn: "field",
			values: []ParamValue{{
				Value: "9007199254740993", SourceType: types.New(types.T_decimal128, 20, 0),
				HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal,
			}},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field fixed exact needle with nested abs decimal candidate",
			sql:  "prepare p from 'select field(cast(9007199254740993 as decimal(20,0)), abs(?))'", fn: "field",
			values: []ParamValue{{
				Value: "9007199254740992", SourceType: types.New(types.T_decimal128, 20, 0),
				HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal,
			}},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field nested coalesce decimal with fixed exact candidate",
			sql:  "prepare p from 'select field(coalesce(?, cast(0 as decimal(20,0))), cast(9007199254740992 as decimal(20,0)))'", fn: "field",
			values: []ParamValue{{
				Value: "9007199254740993", SourceType: types.New(types.T_decimal128, 20, 0),
				HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal,
			}},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field nested if decimal with fixed exact candidate",
			sql:  "prepare p from 'select field(if(true, ?, cast(0 as decimal(20,0))), cast(9007199254740992 as decimal(20,0)))'", fn: "field",
			values: []ParamValue{{
				Value: "9007199254740993", SourceType: types.New(types.T_decimal128, 20, 0),
				HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal,
			}},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field nested decimal with fixed string boundary",
			sql:  "prepare p from 'select field(abs(?), \"9007199254740992\")'", fn: "field",
			values: []ParamValue{{
				Value: "9007199254740993", SourceType: types.New(types.T_decimal128, 20, 0),
				HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal,
			}},
			want: []types.T{types.T_float64, types.T_float64},
		},
		{
			name: "field binary protocol numeric with fixed exact candidate",
			sql:  "prepare p from 'select field(?, cast(9007199254740992 as decimal(20,0)))'", fn: "field",
			values: []ParamValue{{
				Value: "9007199254740993", RuntimeType: types.T_uint64.ToType(),
				HasRuntimeType: true, IsBinaryProtocol: true,
			}},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field binary protocol nested abs with fixed exact candidate",
			sql:  "prepare p from 'select field(abs(?), cast(9007199254740992 as decimal(20,0)))'", fn: "field",
			values: []ParamValue{{
				Value: "9007199254740993", RuntimeType: types.T_uint64.ToType(),
				HasRuntimeType: true, IsBinaryProtocol: true,
			}},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field binary protocol text preserves approximate comparison",
			sql:  "prepare p from 'select field(?, cast(9007199254740992 as decimal(20,0)))'", fn: "field",
			values: []ParamValue{{
				Value: "9007199254740993", RuntimeType: types.T_varchar.ToType(),
				HasRuntimeType: true, IsBinaryProtocol: true,
			}},
			want: []types.T{types.T_float64, types.T_float64},
		},
		{
			name: "field decimal with fixed real boundary",
			sql:  "prepare p from 'select field(?, cast(9007199254740992 as double))'", fn: "field",
			values: []ParamValue{{
				Value: "9007199254740993", SourceType: types.New(types.T_decimal128, 20, 0),
				HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal,
			}},
			want: []types.T{types.T_float64, types.T_float64},
		},
		{
			name: "field binary", sql: "prepare p from 'select field(?, ?)'", fn: "field",
			values: []ParamValue{
				{Value: []byte{0, 'b'}, SourceType: types.T_varbinary.ToType(), HasSourceType: true, IsBinaryString: true},
				{Value: []byte{'b'}, SourceType: types.T_varbinary.ToType(), HasSourceType: true, IsBinaryString: true},
			},
			want: []types.T{types.T_varbinary, types.T_varbinary},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, tc.sql)
			require.NoError(t, err)
			plan := prepared.GetDcl().GetPrepare().Plan
			require.True(t, PreparedPlanNeedsRuntimeSpecialization(plan))
			params := make([]any, len(tc.values))
			for i, value := range tc.values {
				params[i] = value
			}
			filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), plan, params)
			require.NoError(t, err)
			require.True(t, specialized)
			fn := findPlanFunctionExpr(filled, tc.fn)
			require.NotNil(t, fn)
			for i, want := range tc.want {
				require.Equal(t, want, types.T(fn.GetF().Args[i].Typ.Id), fn.String())
			}
		})
	}
}
