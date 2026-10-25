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
	"fmt"
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

func TestIntegerPrecisionSetOperationOutputs(t *testing.T) {
	for _, operator := range []string{"union all", "union", "intersect", "intersect all", "except", "except all"} {
		for _, prepare := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/prepare=%v", operator, prepare), func(t *testing.T) {
				for _, query := range []string{
					"select 2*3 as x,8 as y " + operator + " select 4*5 as other,9 as ignored",
					"select x,y from (select 2*3 as x,8 as y " + operator + " select 4*5,9) s",
					"(select 2*3 as x,8 as y " + operator + " select 4*5,9) union all select 6*7,10",
				} {
					stmt, err := mysql.ParseOne(t.Context(), query, 1)
					require.NoError(t, err)
					func() {
						defer stmt.Free()
						p, err := BuildPlan(NewMockOptimizer(false, newPlanTestProcess(t)).CurrentContext(), stmt, prepare)
						require.NoError(t, err, query)
						q := p.GetQuery()
						require.Equal(t, []string{"x", "y"}, q.Headings)
						require.NoError(t, q.ValidateIntegerPrecision())
						require.Len(t, q.IntegerPrecision.Outputs, 2)
						require.Len(t, GetResultColumnsFromPlan(p), 2)
						require.Equal(t, pb.IntegerPrecisionOutput_SQL_REFERENCE, q.IntegerPrecision.Outputs[0].RootClass)
						wire, err := q.Marshal()
						require.NoError(t, err)
						require.NoError(t, new(pb.Query).Unmarshal(wire))
					}()
				}
			})
		}
	}
	t.Run("prepared markers", func(t *testing.T) {
		stmt, err := mysql.ParseOne(t.Context(), "select ?*2 as x union all select ?*3", 1)
		require.NoError(t, err)
		defer stmt.Free()
		p, err := BuildPlan(NewMockOptimizer(false, newPlanTestProcess(t)).CurrentContext(), stmt, true)
		require.NoError(t, err)
		require.Equal(t, []string{"x"}, p.GetQuery().Headings)
		require.Len(t, GetResultColumnsFromPlan(p), 1)
	})
}

func TestIntegerPrecisionCollapsedUnionIsReference(t *testing.T) {
	projection := "select attr_seqnum*attr_seqnum*attr_seqnum as x from mo_catalog.mo_columns"
	p, err := runOneExprStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, projection+" union "+projection)
	require.NoError(t, err)
	q := p.GetQuery()
	require.Equal(t, []string{"x"}, q.Headings)
	require.NoError(t, q.ValidateIntegerPrecision())
	require.NotNil(t, q.IntegerPrecision)
	require.Equal(t, pb.IntegerPrecisionOutput_SQL_REFERENCE, q.IntegerPrecision.Outputs[0].RootClass)
	ctas, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "create table collapsed_integer as "+projection+" union "+projection)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_int64), ctas.GetDdl().GetCreateTable().TableDef.Cols[0].Typ.Id, "deduplication cannot turn a SQL reference into a direct CTAS root")
	raw, err := runOneExprStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "select 1 as x union all select 2")
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, raw.GetQuery().Headings)
	require.Nil(t, raw.GetQuery().IntegerPrecision)
}

func TestIntegerPrecisionGroupingSchemaRoots(t *testing.T) {
	for _, grouping := range []string{"attr_seqnum", "attr_seqnum with rollup", "cube(attr_seqnum)", "grouping sets ((attr_seqnum),())", "grouping sets ((),(attr_seqnum))"} {
		for _, tc := range []struct {
			expr, from string
			length     uint32
			root       pb.IntegerPrecisionOutput_RootClass
			target     types.T
		}{
			{"attr_seqnum*attr_seqnum*attr_seqnum", "mo_catalog.mo_columns", 10, pb.IntegerPrecisionOutput_DIRECT_FUNCTION, types.T_int32},
			{"attr_seqnum*attr_seqnum*attr_seqnum+1", "mo_catalog.mo_columns", 11, pb.IntegerPrecisionOutput_DIRECT_FUNCTION, types.T_int32},
			{"case when attr_seqnum>0 then attr_seqnum*attr_seqnum*attr_seqnum else attr_seqnum*attr_seqnum*attr_seqnum+1 end", "mo_catalog.mo_columns", 11, pb.IntegerPrecisionOutput_DIRECT_FUNCTION, types.T_int32},
			{"x", "(select attr_seqnum,attr_seqnum*attr_seqnum*attr_seqnum x from mo_catalog.mo_columns) s", 10, pb.IntegerPrecisionOutput_SQL_REFERENCE, types.T_int64},
		} {
			t.Run(grouping+"/"+tc.expr, func(t *testing.T) {
				query := "create table integer_grouped as select " + tc.expr + " as result,attr_seqnum as raw,2 as literal from " + tc.from + " group by " + strings.ReplaceAll(grouping, "attr_seqnum", "attr_seqnum,x")
				if tc.expr != "x" {
					query = "create table integer_grouped as select " + tc.expr + " as result,attr_seqnum as raw,2 as literal from " + tc.from + " group by " + grouping
				}
				stmt, err := mysql.ParseOne(t.Context(), query, 1)
				require.NoError(t, err)
				defer stmt.Free()
				p, err := BuildPlan(NewMockOptimizer(false, newPlanTestProcess(t)).CurrentContext(), stmt, false)
				require.NoError(t, err, query)
				ddl := p.GetDdl()
				selectStmt, err := mysql.ParseOne(t.Context(), strings.TrimPrefix(query, "create table integer_grouped as "), 1)
				require.NoError(t, err)
				defer selectStmt.Free()
				selectPlan, err := BuildPlan(NewMockOptimizer(false, newPlanTestProcess(t)).CurrentContext(), selectStmt, false)
				require.NoError(t, err)
				q := selectPlan.GetQuery()
				require.NoError(t, q.ValidateIntegerPrecision())
				require.Equal(t, []string{"result", "raw", "literal"}, q.Headings)
				require.Equal(t, tc.root, q.IntegerPrecision.Outputs[0].RootClass)
				require.Equal(t, tc.length, uint32(evaluateIntegerPrecision(q.IntegerPrecision)[q.IntegerPrecision.Outputs[0].Node-1].DisplayLength()))
				require.Equal(t, int32(tc.target), ddl.GetCreateTable().TableDef.Cols[0].Typ.Id)
				rawRoot := pb.IntegerPrecisionOutput_DECLARED_SOURCE
				if tc.expr == "x" {
					rawRoot = pb.IntegerPrecisionOutput_SQL_REFERENCE
				}
				require.Equal(t, rawRoot, q.IntegerPrecision.Outputs[1].RootClass)
				require.Equal(t, pb.IntegerPrecisionOutput_SQL_LITERAL, q.IntegerPrecision.Outputs[2].RootClass)
			})
		}
	}
}
