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

package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestIntegerPrecisionLiteralPlan(t *testing.T) {
	k := strings.Repeat("0", 69) + "2"
	for _, tc := range []struct {
		expr   string
		length uint32
	}{
		{"2*3", 3}, {"-0002*3", 6}, {"(+0002)+1", 6}, {"0002*3", 6}, {k + "+1", 72}, {k + "*1", 66}, {"(" + k + "+1)+1", 67}, {"cast(2 as signed)*2", 22},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			p, err := runOneExprStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "select "+tc.expr)
			require.NoError(t, err)
			q := p.GetQuery()
			require.NotNil(t, q.IntegerPrecision)
			require.NoError(t, q.ValidateIntegerPrecision())
			cols := GetResultColumnsFromPlan(p)
			require.Equal(t, int32(types.T_int64), cols[0].Typ.Id)
			require.NotNil(t, cols[0].IntegerResultPresentation)
			require.Equal(t, tc.length, cols[0].IntegerResultPresentation.DisplayLength)
			data, err := q.Marshal()
			require.NoError(t, err)
			decoded := new(pb.Query)
			require.NoError(t, decoded.Unmarshal(data))
			require.Equal(t, q.IntegerPrecision, decoded.IntegerPrecision)
			copy := DeepCopyQuery(q)
			copy.IntegerPrecision.Nodes[0].Precision++
			require.NotEqual(t, copy.IntegerPrecision.Nodes[0].Precision, q.IntegerPrecision.Nodes[0].Precision)
		})
	}
}

func TestIntegerPrecisionImportAndUnknown(t *testing.T) {
	source := newIntegerPrecisionBuilder()
	source.graph.SourceBudget = 70
	literal := source.append(&pb.IntegerPrecisionNode{Kind: pb.IntegerPrecisionNode_SQL_LITERAL, Precision: 70})
	one := source.append(&pb.IntegerPrecisionNode{Kind: pb.IntegerPrecisionNode_SQL_LITERAL, Precision: 1})
	add := source.append(&pb.IntegerPrecisionNode{Kind: pb.IntegerPrecisionNode_ADD, Inputs: []uint32{literal, one}, Arithmetic: true})
	outer := newIntegerPrecisionBuilder()
	outer.append(&pb.IntegerPrecisionNode{Precision: 3}) // Force imported ID remapping.
	imported := outer.importID(source.graph, add)
	require.NotEqual(t, add, imported)
	require.Equal(t, uint64(70), outer.graph.SourceBudget)
	ref := outer.reference(imported)
	outer.graph.Outputs = []*pb.IntegerPrecisionOutput{{Node: ref, RootClass: pb.IntegerPrecisionOutput_SQL_REFERENCE}}
	q := &pb.Query{Headings: []string{"x"}, IntegerPrecision: outer.graph}
	require.NoError(t, q.ValidateIntegerPrecision())
	p := evaluateIntegerPrecision(q.IntegerPrecision)[ref-1]
	require.Equal(t, uint64(72), p.DisplayLength())
	require.Equal(t, uint64(65), p.Offered(false))
	before := len(outer.graph.Nodes)
	require.Equal(t, imported, outer.importID(source.graph, add))
	require.Len(t, outer.graph.Nodes, before)
	source.graph.Nodes[0].Precision = 1
	require.Equal(t, uint64(72), evaluateIntegerPrecision(outer.graph)[ref-1].DisplayLength(), "import owns its recipes")
	common := outer.append(&pb.IntegerPrecisionNode{Kind: pb.IntegerPrecisionNode_COMMON_INTEGER, Inputs: []uint32{ref, 0}, Arithmetic: true})
	require.False(t, evaluateIntegerPrecision(outer.graph)[common-1].Known, "a required unknown branch cannot disappear")
}

func TestIntegerPrecisionOptionalMetadataDoesNotExecute(t *testing.T) {
	p, err := runOneExprStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "select cast(100 as signed)*cast(100 as signed)")
	require.NoError(t, err)
	q := p.GetQuery()
	require.NotNil(t, q.IntegerPrecision)
	expected := DeepCopyQuery(q)
	q.IntegerPrecision = nil
	// The old consumer sees exactly the same physical expression and literals.
	for i, node := range expected.Nodes {
		before, err := node.Marshal()
		require.NoError(t, err)
		after, err := q.Nodes[i].Marshal()
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
	require.NoError(t, q.ValidateIntegerPrecision())
	cols := GetResultColumnsFromPlan(p)
	require.Equal(t, int32(types.T_int64), cols[0].Typ.Id)
	require.Nil(t, cols[0].IntegerResultPresentation)
}

func TestIntegerPrecisionOldConsumerExecutesWidenedSignature(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	column := &Expr{Typ: pb.Type{Id: int32(types.T_int8)}, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 0, ColPos: 0}}}
	expr, err := BindFuncExprImplByPlanExpr(context.Background(), "*", []*Expr{column, DeepCopyExpr(column)})
	require.NoError(t, err)
	wire, err := expr.Marshal()
	require.NoError(t, err)
	var decoded Expr
	require.NoError(t, decoded.Unmarshal(wire))
	executor, err := colexec.NewExpressionExecutor(proc, &decoded)
	require.NoError(t, err)
	defer executor.Free()
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int8.ToType())
	defer input.Clean(proc.Mp())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int8(100), false, proc.Mp()))
	input.SetRowCount(1)
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, types.T_int64, result.GetType().Oid)
	require.Equal(t, []int64{10000}, vector.MustFixedColNoTypeCheck[int64](result))
}

func TestIntegerPrecisionReturningAndPresentationCopy(t *testing.T) {
	p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "update returning_bool set n=n returning n+n,flag")
	require.NoError(t, err)
	require.NoError(t, p.GetQuery().ValidateIntegerPrecision())
	cols := GetResultColumnsFromPlan(p)
	require.Len(t, cols, 2)
	require.NotNil(t, cols[0].IntegerResultPresentation)
	require.Nil(t, cols[1].IntegerResultPresentation)
	require.Equal(t, uint32(12), cols[0].IntegerResultPresentation.DisplayLength)
	copy := DeepCopyColDef(cols[0])
	copy.IntegerResultPresentation.DisplayLength = 99
	require.Equal(t, uint32(12), cols[0].IntegerResultPresentation.DisplayLength)
}

func TestIntegerPrecisionLegacyParameterRefresh(t *testing.T) {
	optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
	stmt, err := mysql.ParseOne(t.Context(), "select ?*cast(2 as signed)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	prepared, err := BuildPlan(optimizer.CurrentContext(), stmt, true)
	require.NoError(t, err)
	_, _, err = ResetPreparePlan(optimizer.CurrentContext(), prepared)
	require.NoError(t, err)
	original := cloneIntegerPrecisionGraph(prepared.GetQuery().IntegerPrecision)
	for _, oid := range []types.T{types.T_int64, types.T_uint64, types.T_decimal128, types.T_varchar} {
		t.Run(oid.String(), func(t *testing.T) {
			runtime, _, err := FillValuesOfParamsInPlanWithSpecialization(t.Context(), prepared, []any{ParamValue{Value: "2", RuntimeType: oid.ToType(), HasRuntimeType: true, IsBinaryProtocol: true}})
			require.NoError(t, err)
			require.NoError(t, runtime.GetQuery().ValidateIntegerPrecision())
			require.Equal(t, original, prepared.GetQuery().IntegerPrecision, "template recipes are immutable")
			values := evaluateIntegerPrecision(runtime.GetQuery().IntegerPrecision)
			out := runtime.GetQuery().IntegerPrecision.Outputs[0]
			got := values[out.Node-1]
			if oid.IsInteger() {
				require.True(t, got.Known)
				require.Equal(t, oid.IsUnsignedInt(), got.Unsigned)
			} else {
				require.False(t, got.Known)
			}
		})
	}
}

func TestIntegerPrecisionParameterOwnershipStopsAtCast(t *testing.T) {
	optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
	stmt, err := mysql.ParseOne(t.Context(), "select ?*cast(2 as signed),cast(? as signed)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(optimizer.CurrentContext(), stmt, true)
	require.NoError(t, err)
	bindings := IntegerPrecisionParameterBindings(p)
	require.Len(t, bindings, 1, "the independent cast marker does not own arithmetic history")
	require.Zero(t, bindings[0].Position)
}
