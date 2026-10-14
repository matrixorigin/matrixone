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
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldNumericModeAffectsTypes(t *testing.T) {
	value := func(oid types.T) *plan.Expr { return &plan.Expr{Typ: plan.Type{Id: int32(oid)}} }
	call := func(name string, oid types.T, args ...*plan.Expr) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(oid)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: name}, Args: args}}}
	}
	unsigned := call("-", types.T_uint64, value(types.T_uint64), value(types.T_int64))
	folded := &plan.Expr{Typ: unsigned.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Src: unsigned}}}
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
		{"floating subtraction", call("-", types.T_float64, value(types.T_uint64), value(types.T_float64)), false, true, false, false},
		{"folded unsigned subtraction", folded, false, true, false, true},
		{"nested unsigned subtraction", call("coalesce", types.T_uint64, unsigned), false, true, false, true},
		{"decimal division", call("/", types.T_decimal128, value(types.T_int64), value(types.T_int64)), false, false, true, true},
		{"floating division", call("/", types.T_float64, value(types.T_float64), value(types.T_float64)), false, false, true, false},
		{"integer sum", call("sum", types.T_int64, value(types.T_int64)), true, false, false, false},
		{"boolean sum", call("sum", types.T_int64, value(types.T_bool)), true, false, false, true},
		{"boolean avg adapter", call("avg", types.T_decimal128, adapter), true, false, false, true},
		{"explicit numeric aggregate operand", call("sum", types.T_int64, explicit), true, false, false, false},
		{"window boolean aggregate", &plan.Expr{Expr: &plan.Expr_W{W: &plan.WindowSpec{WindowFunc: call("sum", types.T_int64, adapter)}}}, true, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{tc.expr}}}}}}
			require.Equal(t, tc.want, preparedFieldNumericModeAffectsTypes(p, tc.boolSumAvg, tc.unsignedSubtraction, tc.divisionPrecision))
		})
	}
	require.False(t, preparedFieldNumericModeAffectsTypes(nil, true, true, true))
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
