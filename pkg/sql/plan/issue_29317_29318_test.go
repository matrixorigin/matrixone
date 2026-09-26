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
	decimal := func(value any) ParamValue {
		return ParamValue{Value: value, SourceType: types.New(types.T_decimal128, 20, 0),
			HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal}
	}
	type sourceDomainCase struct {
		name   string
		sql    string
		fn     string
		values []ParamValue
		want   []types.T
	}
	cases := []sourceDomainCase{
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
			values: []ParamValue{decimal("9007199254740993")},
			want:   []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field fixed exact needle with decimal candidate",
			sql:  "prepare p from 'select field(cast(9007199254740993 as decimal(20,0)), ?)'", fn: "field",
			values: []ParamValue{decimal("9007199254740992")},
			want:   []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field nested abs decimal with fixed exact candidate",
			sql:  "prepare p from 'select field(abs(?), cast(9007199254740992 as decimal(20,0)))'", fn: "field",
			values: []ParamValue{decimal("9007199254740993")},
			want:   []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field fixed abs decimal candidate",
			sql:  "prepare p from 'select field(?, abs(cast(9007199254740992 as decimal(20,0))))'", fn: "field",
			values: []ParamValue{decimal("9007199254740993")},
			want:   []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field fixed coalesce decimal candidate",
			sql:  "prepare p from 'select field(?, coalesce(cast(9007199254740992 as decimal(20,0)), cast(0 as decimal(20,0))))'", fn: "field",
			values: []ParamValue{decimal("9007199254740993")},
			want:   []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field nested greatest with exact peer",
			sql:  "prepare p from 'select field(greatest(?, cast(0 as decimal(20,0))), cast(9007199254740992 as decimal(20,0)))'", fn: "field",
			values: []ParamValue{decimal("9007199254740993")},
			want:   []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field nested least with exact peer",
			sql:  "prepare p from 'select field(least(?, cast(9007199254740994 as decimal(20,0))), cast(9007199254740992 as decimal(20,0)))'", fn: "field",
			values: []ParamValue{decimal("9007199254740993")},
			want:   []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field fixed exact needle with nested abs decimal candidate",
			sql:  "prepare p from 'select field(cast(9007199254740993 as decimal(20,0)), abs(?))'", fn: "field",
			values: []ParamValue{decimal("9007199254740992")},
			want:   []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field nested coalesce decimal with fixed exact candidate",
			sql:  "prepare p from 'select field(coalesce(?, cast(0 as decimal(20,0))), cast(9007199254740992 as decimal(20,0)))'", fn: "field",
			values: []ParamValue{decimal("9007199254740993")},
			want:   []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field nested if decimal with fixed exact candidate",
			sql:  "prepare p from 'select field(if(true, ?, cast(0 as decimal(20,0))), cast(9007199254740992 as decimal(20,0)))'", fn: "field",
			values: []ParamValue{decimal("9007199254740993")},
			want:   []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field nested decimal with fixed string boundary",
			sql:  "prepare p from 'select field(abs(?), \"9007199254740992\")'", fn: "field",
			values: []ParamValue{decimal("9007199254740993")},
			want:   []types.T{types.T_float64, types.T_float64},
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
			values: []ParamValue{decimal("9007199254740993")},
			want:   []types.T{types.T_float64, types.T_float64},
		},
		{
			name: "field explicit char peer stays a string",
			sql:  "prepare p from 'select field(?, cast(9007199254740993 as char))'", fn: "field",
			values: []ParamValue{decimal("9007199254740992")},
			want:   []types.T{types.T_float64, types.T_float64},
		},
		{
			name: "field folded explicit char peer stays a string",
			sql:  "prepare p from 'select field(?, cast(abs(cast(9007199254740993 as decimal(20,0))) as char))'", fn: "field",
			values: []ParamValue{decimal("9007199254740992")},
			want:   []types.T{types.T_float64, types.T_float64},
		},
		{
			name: "field reverse coalesce with text null keeps exact result",
			sql:  "prepare p from 'select field(coalesce(abs(?), ?), abs(cast(9007199254740992 as decimal(20,0))))'", fn: "field",
			values: []ParamValue{
				{Value: nil, SourceType: types.T_text.ToType(), HasSourceType: true},
				{Value: "9007199254740993", SourceType: types.New(types.T_decimal128, 20, 0),
					HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal},
			},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field reverse coalesce with binary protocol null keeps exact result",
			sql:  "prepare p from 'select field(coalesce(abs(?), ?), abs(cast(9007199254740992 as decimal(20,0))))'", fn: "field",
			values: []ParamValue{
				{Value: nil, IsBinaryProtocol: true},
				{Value: "9007199254740993", RuntimeType: types.T_uint64.ToType(),
					HasRuntimeType: true, IsBinaryProtocol: true},
			},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field reverse coalesce with double null remains approximate",
			sql:  "prepare p from 'select field(coalesce(abs(?), ?), abs(cast(9007199254740992 as decimal(20,0))))'", fn: "field",
			values: []ParamValue{
				{Value: nil, SourceType: types.T_float64.ToType(), HasSourceType: true},
				{Value: "9007199254740993", SourceType: types.New(types.T_decimal128, 20, 0),
					HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal},
			},
			want: []types.T{types.T_float64, types.T_float64},
		},
		{
			name: "field nested abs after null coalesce remains decimal",
			sql:  "prepare p from 'select field(coalesce(?, abs(?)), abs(cast(9007199254740992 as decimal(20,0))))'", fn: "field",
			values: []ParamValue{
				{Value: nil, SourceType: types.T_any.ToType(), HasSourceType: true},
				{Value: "9007199254740993", SourceType: types.New(types.T_decimal128, 20, 0),
					HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal},
			},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		{
			name: "field binary", sql: "prepare p from 'select field(?, ?)'", fn: "field",
			values: []ParamValue{
				{Value: []byte{0, 'b'}, SourceType: types.T_varbinary.ToType(), HasSourceType: true, IsBinaryString: true},
				{Value: []byte{'b'}, SourceType: types.T_varbinary.ToType(), HasSourceType: true, IsBinaryString: true},
			},
			want: []types.T{types.T_varbinary, types.T_varbinary},
		},
	}
	// Each relational owner keeps a different binding tag or physical output.
	// These cases must also trigger specialization through the public EXECUTE gate.
	for _, relation := range []struct {
		sql    string
		domain types.T
	}{
		{"(select ? as x limit 1) d", types.T_decimal128},
		{"(select x from (select ? as x limit 1) a limit 1) d", types.T_decimal128},
		{"(select max(?) as x) d", types.T_decimal128},
		{"(select sum(?) as x) d", types.T_decimal256},
		{"(select avg(?) as x) d", types.T_decimal128},
		{"(select ? as x group by x) d", types.T_decimal128},
		{"(select distinct ? as x) d", types.T_decimal128},
		{"(select max(?) over () as x, max(0) over () as y) d", types.T_decimal128},
		{"(select max(0) over () as y, max(?) over () as x) d", types.T_decimal128},
		{"(select ? as x union all select cast(9007199254740993 as decimal(20,0))) d", types.T_decimal128},
		{"(select cast(9007199254740993 as decimal(20,0)) as x union all select ?) d", types.T_decimal128},
	} {
		cases = append(cases, sourceDomainCase{
			name: relation.sql, sql: "prepare p from 'select field(x, abs(cast(9007199254740992 as decimal(20,0)))) from " + relation.sql + "'", fn: "field",
			values: []ParamValue{decimal("9007199254740993")}, want: []types.T{relation.domain, relation.domain},
		})
	}
	cases = append(cases, sourceDomainCase{
		name: "predicate consumer", sql: "prepare p from 'select 1 from (select ? as x) d where field(x, abs(cast(9007199254740992 as decimal(20,0))))=0'", fn: "field",
		values: []ParamValue{decimal("9007199254740993")}, want: []types.T{types.T_decimal128, types.T_decimal128},
	})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, tc.sql)
			require.NoError(t, err)
			plan := prepared.GetDcl().GetPrepare().Plan
			snapshot := plan.String()
			require.True(t, PreparedPlanNeedsRuntimeSpecialization(plan))
			params := make([]any, len(tc.values))
			for i, value := range tc.values {
				params[i] = value
			}
			filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), plan, params)
			require.NoError(t, err)
			require.True(t, specialized)
			require.Equal(t, snapshot, plan.String(), "cached PREPARE template changed")
			fn := findPlanFunctionExpr(filled, tc.fn)
			require.NotNil(t, fn)
			for i, want := range tc.want {
				require.Equal(t, want, types.T(fn.GetF().Args[i].Typ.Id), fn.String())
			}
		})
	}
}
