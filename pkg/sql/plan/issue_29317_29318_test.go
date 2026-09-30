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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
	cases := make([]sourceDomainCase, 0, 44)
	cases = append(cases, []sourceDomainCase{
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
			name: "field reverse coalesce with text null uses double domain",
			sql:  "prepare p from 'select field(coalesce(abs(?), ?), abs(cast(9007199254740992 as decimal(20,0))))'", fn: "field",
			values: []ParamValue{
				{Value: nil, SourceType: types.T_text.ToType(), HasSourceType: true},
				{Value: "9007199254740993", SourceType: types.New(types.T_decimal128, 20, 0),
					HasSourceType: true, PrepareParamKind: vector.PrepareParamDecimal},
			},
			want: []types.T{types.T_float64, types.T_float64},
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
	}...)
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
	cases = append(cases,
		sourceDomainCase{
			name: "projected greatest without operand casts",
			sql:  "prepare p from 'select greatest(x,y) from (select ? as x, ? as y limit 1) d'",
			fn:   "greatest", values: []ParamValue{decimal("2"), decimal("10")},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		sourceDomainCase{
			name: "projected field without operand casts",
			sql:  "prepare p from 'select field(x,y) from (select ? as x, ? as y limit 1) d'",
			fn:   "field", values: []ParamValue{decimal("1"), decimal("1")},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		sourceDomainCase{
			name: "projected comparison without operand casts",
			sql:  "prepare p from 'select if(x>y,x,y) from (select ? as x, ? as y limit 1) d'",
			fn:   ">", values: []ParamValue{decimal("2"), decimal("10")},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		sourceDomainCase{
			name: "projected IN with constant string list",
			sql:  "prepare p from 'select x in (''1'',''2'') from (select ? as x limit 1) d'",
			fn:   "=", values: []ParamValue{decimal("1.0")},
			want: []types.T{types.T_float64, types.T_float64},
		},
		sourceDomainCase{
			name: "retained BETWEEN with constant string bounds",
			sql:  "prepare p from 'select x between ''1'' and ''2'' from (select ? as x limit 1) d'",
			fn:   "between", values: []ParamValue{decimal("10")},
			want: []types.T{types.T_float64, types.T_float64, types.T_float64},
		},
		sourceDomainCase{
			name: "grouped coalesce without operand casts",
			sql:  "prepare p from 'select coalesce(x,y) from (select ? as x, ? as y from nation group by x,y) d'",
			fn:   "coalesce", values: []ParamValue{
				{Value: nil, SourceType: types.New(types.T_decimal128, 20, 0), HasSourceType: true},
				decimal("9007199254740993"),
			},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
		sourceDomainCase{
			name: "set output owns both source domains",
			sql:  "prepare p from 'select field(x, abs(cast(9007199254740992 as decimal(20,0)))) from (select ? as x union all select ?) d'",
			fn:   "field", values: []ParamValue{decimal("9007199254740993"),
				{Value: "9007199254740993", SourceType: types.T_varchar.ToType(), HasSourceType: true}},
			want: []types.T{types.T_float64, types.T_float64},
		},
		sourceDomainCase{
			name: "table scalar selected output",
			sql:  "prepare p from 'select field((select ? from nation r where r.n_nationkey=1 limit 1), abs(cast(9007199254740992 as decimal(20,0))))'",
			fn:   "field", values: []ParamValue{decimal("9007199254740993")},
			want: []types.T{types.T_decimal128, types.T_decimal128},
		},
	)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			plan := prepared.GetDcl().GetPrepare().Plan
			snapshot := plan.String()
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

func TestPreparedCommonValueStringMarkerWithFixedDecimalPeer(t *testing.T) {
	for _, name := range []string{"coalesce", "greatest", "least"} {
		t.Run(name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
				"prepare p from 'select "+name+"(?, cast(9007199254740992.0000000001 as decimal(38,10))) = cast(9007199254740992.0000000002 as decimal(38,10))'")
			require.NoError(t, err)
			template := prepared.GetDcl().GetPrepare().Plan
			before := template.String()
			values := []any{ParamValue{
				Value: "9007199254740992.0000000002", SourceType: types.T_varchar.ToType(),
				HasSourceType: true, EnableNumericPrefix: true,
			}}
			require.True(t, PreparedPlanNeedsNumericPrefixSpecialization(template, values))
			filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), template, values)
			require.NoError(t, err)
			require.True(t, specialized)
			require.Equal(t, before, template.String(), "cached PREPARE template changed")
			extrema := findPlanFunctionExpr(filled, name)
			require.NotNil(t, extrema)
			require.Equal(t, int32(types.T_decimal128), extrema.Typ.Id, extrema.String())
			require.Equal(t, int32(38), extrema.Typ.Width, extrema.String())
			require.Equal(t, int32(10), extrema.Typ.Scale, extrema.String())
			for _, arg := range extrema.GetF().Args {
				require.Equal(t, int32(types.T_decimal128), arg.Typ.Id, extrema.String())
			}
			comparison := findPlanFunctionExpr(filled, "=")
			require.NotNil(t, comparison)
			for _, arg := range comparison.GetF().Args {
				require.Equal(t, int32(types.T_decimal128), arg.Typ.Id, comparison.String())
			}
		})
	}
	for _, tc := range []struct {
		name, expr string
	}{
		{"explicit typed peer", "coalesce(?, cast(? as decimal(38,10)))"},
		{"nested fixed peer", "greatest(?, coalesce(?, cast(1 as decimal(38,10))))"},
		{"nested abs peer", "greatest(?, abs(coalesce(?, cast(1 as decimal(38,10)))))"},
		{"nested abs coalesce peer", "coalesce(?, abs(coalesce(?, cast(1 as decimal(38,10)))))"},
		{"nested arithmetic peer", "greatest(?, coalesce(?, cast(1 as decimal(38,10)))+0)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
				"prepare p from 'select "+tc.expr+" = cast(9007199254740992.0000000002 as decimal(38,10))'")
			require.NoError(t, err)
			template := prepared.GetDcl().GetPrepare().Plan
			before := template.String()
			values := []any{ParamValue{Value: "9007199254740992.0000000002", SourceType: types.T_varchar.ToType(),
				HasSourceType: true, EnableNumericPrefix: true},
				ParamValue{Value: "9007199254740992.0000000002", SourceType: types.T_varchar.ToType(),
					HasSourceType: true, EnableNumericPrefix: true}}
			require.True(t, PreparedPlanNeedsNumericPrefixSpecialization(template, values))
			filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), template, values)
			require.NoError(t, err)
			require.True(t, specialized)
			require.Equal(t, before, template.String())
			comparison := findPlanFunctionExpr(filled, "=")
			require.NotNil(t, comparison)
			for _, arg := range comparison.GetF().Args {
				require.True(t, types.T(arg.Typ.Id).IsDecimal(), comparison.String())
			}
		})
	}
}

func TestPreparedFixedDecimalPrefixReservesIntegralDigits(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
		"prepare p from 'select least(?, cast(1.25 as decimal(10,2)))'")
	require.NoError(t, err)
	template := prepared.GetDcl().GetPrepare().Plan
	before := template.String()
	value := "123456789." + strings.Repeat("0", 67) + "1"
	params := []any{ParamValue{Value: value, SourceType: types.T_varchar.ToType(),
		HasSourceType: true, EnableNumericPrefix: true}}
	filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), template, params)
	require.NoError(t, err)
	require.True(t, specialized)
	require.Equal(t, before, template.String())
	extrema := findPlanFunctionExpr(filled, "least")
	require.NotNil(t, extrema)
	require.Equal(t, int32(types.T_decimal256), extrema.Typ.Id, extrema.String())
	require.Equal(t, int32(76), extrema.Typ.Width, extrema.String())
	require.Equal(t, int32(67), extrema.Typ.Scale, extrema.String())
}

func TestPreparedFixedDecimalPrefixReservesRoundingCarry(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
		"prepare p from 'select least(?, cast(1.25 as decimal(10,2)))'")
	require.NoError(t, err)
	template := prepared.GetDcl().GetPrepare().Plan
	before := template.String()
	for _, tc := range []struct {
		value string
		scale int32
	}{
		{"999999999." + strings.Repeat("9", 67) + "4", 67},
		{"999999999." + strings.Repeat("9", 67) + "5", 66},
		{"-999999999." + strings.Repeat("9", 68), 66},
		{"9." + strings.Repeat("9", 76) + "e8", 66},
	} {
		params := []any{ParamValue{Value: tc.value, SourceType: types.T_varchar.ToType(),
			HasSourceType: true, EnableNumericPrefix: true}}
		filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), template, params)
		require.NoError(t, err)
		require.True(t, specialized)
		require.Equal(t, before, template.String(), "EXECUTE changed the PREPARE template")
		extrema := findPlanFunctionExpr(filled, "least")
		require.NotNil(t, extrema)
		require.Equal(t, int32(types.T_decimal256), extrema.Typ.Id, extrema.String())
		require.Equal(t, int32(76), extrema.Typ.Width, extrema.String())
		require.Equal(t, tc.scale, extrema.Typ.Scale, extrema.String())
	}
}

func TestPreparedRoundRebindsNestedFixedDecimalChild(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
		"prepare p from 'select greatest(?,round(coalesce(?,cast(9007199254740992.0000000001 as decimal(38,10))),10))'")
	require.NoError(t, err)
	template := prepared.GetDcl().GetPrepare().Plan
	before := template.String()
	p := ParamValue{Value: "9007199254740992.0000000002", SourceType: types.T_varchar.ToType(),
		HasSourceType: true, EnableNumericPrefix: true}
	filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), template, []any{p, p})
	require.NoError(t, err)
	require.True(t, specialized)
	require.Equal(t, before, template.String(), "EXECUTE changed the PREPARE template")
	round := findPlanFunctionExpr(filled, "round")
	require.NotNil(t, round)
	require.Equal(t, int32(types.T_decimal128), round.Typ.Id, round.String())
	require.Equal(t, int32(10), round.Typ.Scale, round.String())
	require.Equal(t, int32(types.T_decimal128), round.GetF().Args[0].Typ.Id, round.String())

	explicit, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
		"prepare p from 'select round(cast(coalesce(?,cast(9007199254740992.0000000001 as decimal(38,10))) as double),10)'")
	require.NoError(t, err)
	explicitPlan := explicit.GetDcl().GetPrepare().Plan
	filled, specialized, err = FillValuesOfParamsInPlanWithSpecialization(context.Background(), explicitPlan, []any{p})
	require.NoError(t, err)
	require.True(t, specialized)
	round = findPlanFunctionExpr(filled, "round")
	require.NotNil(t, round)
	require.Equal(t, int32(types.T_float64), round.GetF().Args[0].Typ.Id, round.String())
}

func TestPreparedRoundAndTruncateKeepRuntimeValueDomain(t *testing.T) {
	for _, name := range []string{"round", "truncate"} {
		t.Run(name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
				"prepare p from 'select "+name+"(?,?)'")
			require.NoError(t, err)
			template := prepared.GetDcl().GetPrepare().Plan
			require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(template),
				"only the value overload requires execution-time source decoding; precision stays INT64")
			fn := findPlanFunctionExpr(template, name)
			require.NotNil(t, fn)
			require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id, fn.String())
			require.Equal(t, int32(types.T_int64), fn.GetF().Args[1].Typ.Id, fn.String())

			textValue := ParamValue{
				Value: "1.46", SourceType: types.T_varchar.ToType(),
				HasSourceType: true, EnableNumericPrefix: true,
			}
			filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), template, []any{textValue, int64(1)})
			require.NoError(t, err)
			require.True(t, specialized)
			fn = findPlanFunctionExpr(filled, name)
			require.NotNil(t, fn)
			require.True(t, types.T(fn.GetF().Args[0].Typ.Id).IsDecimal(), fn.String(),
				"numeric text with a complete decimal spelling should keep its exact domain")

			decimalValue := ParamValue{
				Value: "1.46", PrepareParamKind: vector.PrepareParamDecimal,
			}
			filled, specialized, err = FillValuesOfParamsInPlanWithSpecialization(
				context.Background(), template, []any{decimalValue, int64(1)})
			require.NoError(t, err)
			require.True(t, specialized)
			fn = findPlanFunctionExpr(filled, name)
			require.NotNil(t, fn)
			require.True(t, types.T(fn.GetF().Args[0].Typ.Id).IsDecimal(), fn.String())

			explicit, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
				"prepare p from 'select "+name+"(cast(? as decimal(10,2)),1)'")
			require.NoError(t, err)
			explicitPlan := explicit.GetDcl().GetPrepare().Plan
			require.Empty(t, PreparedPlanNumericFallbackParamPositions(explicitPlan))

			for _, valueExpr := range []string{"cast(? as decimal(10,2))", "(select cast(? as decimal(10,2)))"} {
				stmt, parseErr := parsers.ParseOne(context.Background(), dialect.MYSQL,
					"select "+name+"("+valueExpr+",1)", 1)
				require.NoError(t, parseErr)
				mock := NewMockOptimizer(false, newPlanTestProcess(t))
				source := types.T_varchar.ToType()
				bound, bindErr := BuildPreparedExecutionPlan(&mock.ctxt, stmt,
					[]PreparedSourceBinding{{Position: 0, Type: source}},
					[]any{ParamValue{Value: "1.46", SourceType: source, HasSourceType: true}})
				require.NoError(t, bindErr)
				require.False(t, bound.ValueDependent,
					"an explicit numeric cast fixes the overload without inspecting text: %s", valueExpr)
				stmt.Free()
			}

			// A result cast may narrow the function's answer, not its input.
			// Inspect source casts instead of snapshotting a complete plan: a
			// premature scale=1 conversion destroys 1.46 before TRUNCATE sees it.
			for _, tc := range []struct {
				name, sql      string
				explicitNarrow bool
			}{
				{"outer_cast", "select cast(" + name + "(?,1) as decimal(20,1))", false},
				{"scalar_input", "select cast(" + name + "((select ?),1) as decimal(20,1))", false},
				{"scalar_result", "select cast((select " + name + "(?,1)) as decimal(20,1))", false},
				{"derived_input", "select cast(" + name + "(v,1) as decimal(20,1)) from (select ? as v) s", false},
				{"explicit_decimal_input", "select cast(" + name + "(cast(? as decimal(20,1)),1) as decimal(20,2))", true},
				{"explicit_double_input", "select cast(" + name + "(cast(? as double),1) as decimal(20,1))", false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
					require.NoError(t, err)
					t.Cleanup(stmt.Free)
					mock := NewMockOptimizer(false, newPlanTestProcess(t))
					proc := mock.ctxt.GetProcess()
					params := vector.NewVec(types.T_text.ToType())
					t.Cleanup(func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) })
					require.NoError(t, vector.AppendBytes(params, []byte("1.46"), false, proc.Mp()))
					proc.SetPrepareParams(params)
					source := types.T_varchar.ToType()
					bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt,
						[]PreparedSourceBinding{{Position: 0, Type: source}},
						[]any{ParamValue{Value: "1.46", SourceType: source, HasSourceType: true, IsBinaryProtocol: true}})
					require.NoError(t, err)
					require.NotNil(t, findPlanFunctionExpr(bound.Plan, name))
					narrowSourceCast := false
					require.NoError(t, planpb.VisitExpressionsInOwner(bound.Plan, func(root *Expr) error {
						return planpb.VisitExprTree(root, func(expr *Expr) error {
							fn := expr.GetF()
							if fn != nil && fn.Func.GetObjName() == "cast" && len(fn.Args) > 0 &&
								fn.Args[0].GetP() != nil && types.T(expr.Typ.Id).IsDecimal() && expr.Typ.Scale < 2 {
								narrowSourceCast = true
							}
							return nil
						})
					}))
					require.Equal(t, tc.explicitNarrow, narrowSourceCast,
						"only an explicit input cast may narrow the original decimal spelling: %s\n%s", tc.sql, bound.Plan.String())
				})
			}

			for _, tc := range []struct {
				name, precision string
				value           any
				wantErr         bool
			}{
				{"value", "1", "1.46", false},
				{"null", "1", nil, false},
				{"precision_error", "missing_precision_column", "1.46", true},
			} {
				t.Run("context_restored/"+tc.name, func(t *testing.T) {
					mock := NewMockOptimizer(false, newPlanTestProcess(t))
					source := types.T_varchar.ToType()
					ctx := withPreparedSourceBindings(context.Background(),
						[]PreparedSourceBinding{{Position: 0, Type: source}},
						[]any{ParamValue{Value: tc.value, SourceType: source, HasSourceType: true, IsBinaryProtocol: true}})
					mock.ctxt.SetContext(ctx)
					builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, true)
					binder := NewDefaultBinder(ctx, builder, NewBindContext(builder, nil), Type{}, nil)
					outer := Type{Id: int32(types.T_decimal128), Width: 20, Scale: 1}
					subquery := Type{Id: int32(types.T_decimal128), Width: 22, Scale: 1}
					binder.numericParamType, binder.numericSubqueryTarget = &outer, &subquery
					binder.numericFunctionTarget = true
					stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select "+name+"(?,"+tc.precision+")", 1)
					require.NoError(t, err)
					t.Cleanup(stmt.Free)
					ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.FuncExpr)
					_, err = binder.bindPreparedNumericPrecisionFuncExpr(name, ast.Exprs, 0, nil, -1)
					if tc.wantErr {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
					require.Same(t, &outer, binder.numericParamType)
					require.Same(t, &subquery, binder.numericSubqueryTarget)
					require.True(t, binder.numericFunctionTarget)
					// The next arithmetic sibling must still see its own outer
					// target after success, NULL fallback, or a precision error.
					sibling, err := binder.BindExpr(ast.Exprs[0], 0, false)
					require.NoError(t, err)
					require.Equal(t, outer.Id, sibling.Typ.Id)
					require.Equal(t, outer.Scale, sibling.Typ.Scale)
				})
			}
		})
	}
}
