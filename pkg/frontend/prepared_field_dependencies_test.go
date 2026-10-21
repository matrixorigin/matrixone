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

package frontend

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldNumericModeAffectsTypes(t *testing.T) {
	value := func(oid types.T) *plan.Expr { return &plan.Expr{Typ: plan.Type{Id: int32(oid)}} }
	call := func(name string, oid types.T, args ...*plan.Expr) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(oid)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: name}, Args: args}}}
	}
	unsigned := call("-", types.T_uint64, value(types.T_uint64), value(types.T_int64))
	folded := &plan.Expr{Typ: unsigned.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Src: unsigned}}}
	uncapped := value(types.T_decimal64)
	uncapped.Typ.Width, uncapped.Typ.Scale = 10, 2
	decimal := value(types.T_decimal256)
	decimal.Typ.Width, decimal.Typ.Scale = 65, 30
	transport := value(types.T_text)
	transport.Expr = &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}
	unsignedTransport := call("cast", types.T_uint64, transport)
	inferenceCast := call("cast", types.T_int64, unsignedTransport)
	adapter := call("cast", types.T_int8, value(types.T_bool))
	explicit := call("cast", types.T_int8, value(types.T_bool))
	explicit.GetF().SyntaxExplicitCast = true
	for _, tc := range []struct {
		name                                                     string
		expr                                                     *plan.Expr
		boolSumAvg, unsignedSubtraction, divisionPrecision, want bool
	}{
		{"unchanged modes", unsigned, false, false, false, false},
		{"unrelated expression", value(types.T_text), true, true, true, false},
		{"signed subtraction", call("-", types.T_int64, value(types.T_int64), value(types.T_int64)), false, true, false, false},
		{"unsigned subtraction", unsigned, false, true, false, true},
		{"runtime unsigned transport", call("-", types.T_int64, value(types.T_int64), inferenceCast), false, true, false, true},
		{"floating subtraction", call("-", types.T_float64, value(types.T_uint64), value(types.T_float64)), false, true, false, false},
		{"folded unsigned subtraction", folded, false, true, false, true},
		{"nested unsigned subtraction", call("coalesce", types.T_uint64, unsigned), false, true, false, true},
		{"decimal division", call("/", types.T_decimal64, uncapped, uncapped), false, false, true, true},
		{"decimal precision capped", call("/", types.T_decimal256, decimal, decimal), false, false, true, false},
		{"floating division", call("/", types.T_float64, value(types.T_float64), value(types.T_float64)), false, false, true, false},
		{"integer sum", call("sum", types.T_int64, value(types.T_int64)), true, false, false, false},
		{"boolean sum", call("sum", types.T_int64, value(types.T_bool)), true, false, false, true},
		{"boolean avg adapter", call("avg", types.T_decimal128, adapter), true, false, false, true},
		{"explicit numeric aggregate operand", call("sum", types.T_int64, explicit), true, false, false, false},
		{"window boolean aggregate", &plan.Expr{Expr: &plan.Expr_W{W: &plan.WindowSpec{WindowFunc: call("sum", types.T_int64, adapter)}}}, true, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{tc.expr}}}}}}
			previous := function.WithDivPrecisionIncrement(context.Background(), 4)
			previous = function.WithNoUnsignedSubtraction(previous, false)
			current := function.WithNoUnsignedSubtraction(previous, tc.unsignedSubtraction)
			if tc.divisionPrecision {
				current = function.WithDivPrecisionIncrement(current, 8)
			}
			require.Equal(t, tc.want, preparedFieldNumericModeAffectsTypes(p, previous, current, tc.boolSumAvg))
		})
	}
	require.False(t, preparedFieldNumericModeAffectsTypes(nil, context.Background(), context.Background(), true))
}

func TestPreparedFieldNumericModeActualCastContract(t *testing.T) {
	for _, tc := range []struct {
		name, decimal string
		changed       bool
	}{
		{"capped", "65,30", false},
		{"uncapped", "10,2", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := "select field(case when ? then ? else null end,'A','a')," +
				"cast(? as decimal(" + tc.decimal + "))/cast(1 as decimal(" + tc.decimal + "))"
			ses, prepared, cw, _ := newPreparedExecuteEnvForSQL(t, 141, sql)
			t.Cleanup(func() {
				cw.proc.SetPrepareParams(nil)
				prepared.Close()
				ses.Close()
			})
			previous := function.WithDivPrecisionIncrement(context.Background(), 4)
			stmt, err := parsers.ParseOne(previous, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			compiler := ses.txnCompileCtx
			old := compiler.GetContext()
			defer compiler.SetContext(old)
			bindings := []plan2.PreparedSourceBinding{
				{Position: 0, Type: types.T_int64.ToType()},
				{Position: 1, Type: types.T_text.ToType()},
				{Position: 2, Type: types.T_int64.ToType()},
			}
			for _, increment := range []int32{8, 0} {
				current := function.WithDivPrecisionIncrement(previous, increment)
				compiler.SetContext(current)
				bound, err := plan2.BuildPreparedExecutionPlan(compiler, stmt, bindings, []any{int64(1), "a", int64(5)})
				require.NoError(t, err)
				require.Equal(t, tc.changed, preparedFieldNumericModeAffectsTypes(bound.Plan, previous, current, false),
					"an execution CAST1 declaration must survive an inner typed CAST0(P), increment %d", increment)
			}
		})
	}
}

func TestPreparedFieldDependenciesChanged(t *testing.T) {
	previous := &plan.ObjectRef{Db: 1, Obj: 2, Server: 3, SchemaName: "d", ObjName: "t"}
	current := proto.Clone(previous).(*plan.ObjectRef)
	require.NotSame(t, previous, current)
	other := &plan.ObjectRef{Db: 1, Obj: 4, Server: 5, SchemaName: "d", ObjName: "other"}
	require.False(t, preparedFieldDependenciesChanged(
		[]*plan.ObjectRef{previous, other}, []*plan.ObjectRef{other, current}), "optimizer order is not a type change")
	require.True(t, preparedFieldDependenciesChanged(
		[]*plan.ObjectRef{previous, previous}, []*plan.ObjectRef{current, other}), "one dependency cannot satisfy two entries")
	require.False(t, preparedFieldDependenciesChanged([]*plan.ObjectRef{previous}, []*plan.ObjectRef{current}))
	require.False(t, preparedFieldDependenciesChanged(nil, []*plan.ObjectRef{}))
	require.False(t, preparedFieldDependenciesChanged([]*plan.ObjectRef{nil}, []*plan.ObjectRef{nil}))
	require.True(t, preparedFieldDependenciesChanged(nil, []*plan.ObjectRef{current}))
	require.True(t, preparedFieldDependenciesChanged([]*plan.ObjectRef{previous}, nil))
	require.True(t, preparedFieldDependenciesChanged([]*plan.ObjectRef{previous}, []*plan.ObjectRef{nil}))
	for _, tc := range []struct {
		name   string
		mutate func(*plan.ObjectRef)
	}{
		{"table replacement or temporary shadow", func(r *plan.ObjectRef) { r.Obj++ }},
		{"schema version", func(r *plan.ObjectRef) { r.Server++ }},
		{"database replacement", func(r *plan.ObjectRef) { r.Db++ }},
		{"logical name resolution", func(r *plan.ObjectRef) { r.ObjName = "other" }},
		{"subscription", func(r *plan.ObjectRef) { r.SubscriptionName = "sub" }},
		{"snapshot", func(r *plan.ObjectRef) { r.Snapshot = &plan.Snapshot{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := proto.Clone(previous).(*plan.ObjectRef)
			tc.mutate(next)
			require.True(t, preparedFieldDependenciesChanged([]*plan.ObjectRef{previous}, []*plan.ObjectRef{next}))
			require.Equal(t, int64(2), previous.Obj, "published dependency must stay immutable")
		})
	}
}
