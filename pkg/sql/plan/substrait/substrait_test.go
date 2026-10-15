// Copyright 2026 Matrix Origin
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

package substrait

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	planbuilder "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

func TestCollationMetadataSubstraitBoundaries(t *testing.T) {
	for _, mutate := range []func(*planpb.TableDef){
		func(table *planpb.TableDef) { table.KeyFormat = 1 },
		func(table *planpb.TableDef) { table.Indexes = []*planpb.IndexDef{{KeyFormat: 1}} },
		func(table *planpb.TableDef) {
			table.Cols[0].Typ = planpb.Type{Id: int32(types.T_varchar), Width: 32, Charset: 3, CollationVersion: 1}
		},
	} {
		q := scanQuery()
		candidate, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
		require.NoError(t, err)
		table := q.Nodes[0].TableDef
		mutate(table)
		_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
		require.ErrorContains(t, err, "unsupported collation metadata")
		// Revalidate a candidate whose caller-owned plan changed after admission.
		_, err = candidate.BuildEmbedded(nil)
		require.ErrorContains(t, err, "unsupported collation metadata")

		q = embeddedProjectedScanQuery()
		embedded, err := ExportEmbeddedMO(q, EmbeddedExportProfile{})
		require.NoError(t, err)
		reads, err := embedded.EmbeddedMOReads()
		require.NoError(t, err)
		require.Len(t, reads, 1)
		bindings := map[int32]EmbeddedReadBinding{0: {BindingID: 1, Source: EmbeddedReadMO}}
		wire, err := embedded.BuildEmbedded(bindings)
		require.NoError(t, err)
		require.NotEmpty(t, wire)
		mutate(q.Nodes[0].TableDef)
		_, err = ExportEmbeddedMO(q, EmbeddedExportProfile{})
		require.ErrorContains(t, err, "unsupported collation metadata")
		reads, err = embedded.EmbeddedMOReads()
		require.ErrorContains(t, err, "unsupported collation metadata")
		require.Nil(t, reads)
		wire, err = embedded.BuildEmbedded(bindings)
		require.ErrorContains(t, err, "unsupported collation metadata")
		require.Nil(t, wire)
	}
}

func TestExportBuildSupportedSubset(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_FILTER, Children: []int32{0}, FilterList: []*planpb.Expr{fn(">", boolType(), col(0), i64(7))}})
	q.Steps[0] = 1
	c, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	require.Len(t, c.reads, 1)
	b, err := c.BuildEmbedded(testReadBindings(c))
	require.NoError(t, err)
	p := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(b, p))
	require.Equal(t, uint32(78), p.Version.MinorNumber)
	require.Equal(t, []string{"a"}, p.Relations[0].GetRoot().Names)
	require.NotEmpty(t, p.Relations[0].GetRoot().Input.GetFilter().Input.GetRead().GetNamedTable().Names)
}

func TestExportRejectsFullOuterBeforeSnapshotAccess(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_OUTER, Children: []int32{0, 0}})
	q.Steps[0] = 1
	_, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "unsupported join type")
}

func TestRightOuterJoinSurvivesMOPhysicalChildSwap(t *testing.T) {
	q := scanQuery()
	left := &planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}}
	right := &planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0}}}
	other := planbuilder.DeepCopyNode(q.Nodes[0])
	other.NodeId = 1
	q.Nodes = append(q.Nodes, other, &planpb.Node{
		NodeId: 2, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_RIGHT, IsRightJoin: true,
		Children: []int32{0, 1}, OnList: []*planpb.Expr{fn("=", boolType(), left, right)},
	})
	q.Steps[0] = 2
	q.Headings = []string{"left", "right"}
	candidate, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	wire, err := candidate.BuildEmbedded(testReadBindings(candidate))
	require.NoError(t, err)
	plan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, plan))
	require.Equal(t, spb.JoinRel_JOIN_TYPE_RIGHT, plan.Relations[0].GetRoot().Input.GetJoin().Type)
}

func TestExportRejectsOutOfRangeRootWithoutPanicking(t *testing.T) {
	q := scanQuery()
	q.Steps[0] = int32(len(q.Nodes))
	_, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "invalid root node id")
}

func TestUnprojectedSinkScanPreservesProducerWidth(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes,
		&planpb.Node{NodeId: 1, NodeType: planpb.Node_SINK, Children: []int32{0}},
		&planpb.Node{NodeId: 2, NodeType: planpb.Node_SINK_SCAN, SourceStep: []int32{0}},
	)
	q.Steps = []int32{1, 2}
	candidate, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	require.Equal(t, []planpb.Type{i64Type()}, candidate.OutputTypes())
	wire, err := candidate.BuildEmbedded(testReadBindings(candidate))
	require.NoError(t, err)
	plan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, plan))
	require.Len(t, plan.Relations, 2)
	require.Equal(t, int32(0), plan.Relations[1].GetRoot().Input.GetReference().SubtreeOrdinal)
}

func TestSemiAndAntiJoinProjectionCannotReferenceRightInput(t *testing.T) {
	for _, joinType := range []planpb.Node_JoinType{planpb.Node_SEMI, planpb.Node_ANTI} {
		t.Run(joinType.String(), func(t *testing.T) {
			q := scanQuery()
			left := &planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}}
			right := &planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0}}}
			q.Nodes = append(q.Nodes, &planpb.Node{
				NodeId: 1, NodeType: planpb.Node_JOIN, JoinType: joinType, Children: []int32{0, 0},
				OnList: []*planpb.Expr{fn("=", boolType(), left, right)}, ProjectList: []*planpb.Expr{left},
			})
			q.Steps[0] = 1
			_, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
			require.NoError(t, err)

			q.Nodes[1].ProjectList = []*planpb.Expr{right}
			_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
			require.ErrorContains(t, err, "outside join input 1 width 0")

			q.Nodes[1].IsRightJoin = true
			_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
			require.NoError(t, err)
			q.Nodes[1].ProjectList = []*planpb.Expr{left}
			_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
			require.ErrorContains(t, err, "outside join input 0 width 0")
		})
	}
}

func TestSharedScanNodeProducesOneAdmissionRead(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{
		NodeId: 1, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER, Children: []int32{0, 0},
		OnList: []*planpb.Expr{fn("=", boolType(),
			&planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}},
			&planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0}}},
		)},
	})
	q.Steps[0] = 1
	q.Headings = []string{"left", "right"}
	candidate, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	require.Len(t, candidate.reads, 1)
}

func TestProjectEmitsOnlyMOProjectColumns(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*planpb.Expr{col(0)}})
	q.Steps[0] = 1
	c, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	b, err := c.BuildEmbedded(testReadBindings(c))
	require.NoError(t, err)
	p := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(b, p))
	require.Equal(t, []int32{1}, p.Relations[0].GetRoot().Input.GetProject().Common.GetEmit().OutputMapping)
}

func TestExportRejectsOutOfRangeColumnsAndNonSuffixHiddenColumns(t *testing.T) {
	q := scanQuery()
	q.Nodes[0].ProjectList = []*planpb.Expr{col(1)}
	_, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "outside input width")

	q = scanQuery()
	q.Nodes[0].TableDef.Cols = []*planpb.ColDef{
		{Name: "hidden", Hidden: true, Typ: i64Type()},
		{Name: "visible", Typ: i64Type()},
	}
	_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "non-suffix hidden")
}

func TestExportRejectsLiteralTypeMismatch(t *testing.T) {
	q := scanQuery()
	bad := &planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "not-an-integer"}}}}
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_FILTER, Children: []int32{0}, FilterList: []*planpb.Expr{fn("=", boolType(), col(0), bad)}})
	q.Steps[0] = 1
	_, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "literal value does not match")
}

func TestAggregateDistinctAndBadArityAreRejected(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}, AggList: []*planpb.Expr{fn("sum", i64Type(), col(0))}})
	distinct := uint64(q.Nodes[1].AggList[0].GetF().Func.Obj) | uint64(function.Distinct)
	q.Nodes[1].AggList[0].GetF().Func.Obj = int64(distinct)
	q.Steps[0] = 1
	_, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "unsupported aggregate form")
	q = scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_FILTER, Children: []int32{0}, FilterList: []*planpb.Expr{fn("not", boolType(), fn(">", boolType(), col(0), i64(1)), fn(">", boolType(), col(0), i64(2)))}})
	q.Steps[0] = 1
	_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "requires 1 arguments")
}

func TestAggregateSortFetchLowering(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}, GroupBy: []*planpb.Expr{col(0)}, AggList: []*planpb.Expr{fn("min", i64Type(), col(0))}}, &planpb.Node{NodeId: 2, NodeType: planpb.Node_SORT, Children: []int32{1}, OrderBy: []*planpb.OrderBySpec{{Expr: col(0), Flag: planpb.OrderBySpec_ASC}}, Limit: u64(5)})
	q.Steps[0] = 2
	q.Headings = []string{"a", "min(a)"}
	c, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	b, err := c.BuildEmbedded(testReadBindings(c))
	require.NoError(t, err)
	p := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(b, p))
	fetch := p.Relations[0].GetRoot().Input.GetFetch()
	require.Equal(t, int64(5), fetch.GetCount())
	require.NotNil(t, fetch.Input.GetSort().Input.GetAggregate())
}

func TestAggregateProjectionDefinesExportedWidth(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{
		NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0},
		GroupBy:     []*planpb.Expr{col(0)},
		AggList:     []*planpb.Expr{fn("min", i64Type(), col(0))},
		ProjectList: []*planpb.Expr{col(1)},
	})
	q.Steps[0] = 1
	q.Headings = []string{"minimum"}

	candidate, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	require.Equal(t, []planpb.Type{i64Type()}, candidate.OutputTypes())
	wire, err := candidate.BuildEmbedded(testReadBindings(candidate))
	require.NoError(t, err)
	plan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, plan))
	require.Equal(t, []int32{2}, plan.Relations[0].GetRoot().Input.GetProject().Common.GetEmit().OutputMapping)
}

func TestOuterJoinOutputTypesNullExtendOnlyMissingSide(t *testing.T) {
	required := i64Type()
	required.NotNullable = true
	for _, test := range []struct {
		name     string
		joinType planpb.Node_JoinType
		want     []bool
	}{
		{name: "left", joinType: planpb.Node_LEFT, want: []bool{true, false}},
		{name: "right", joinType: planpb.Node_RIGHT, want: []bool{false, true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := scanQuery()
			q.Nodes[0].TableDef.Cols[0].Typ = required
			q.Nodes = append(q.Nodes, &planpb.Node{
				NodeId: 1, NodeType: planpb.Node_JOIN, JoinType: test.joinType,
				Children: []int32{0, 0},
			})
			q.Steps[0] = 1
			q.Headings = []string{"left", "right"}

			candidate, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
			require.NoError(t, err)
			got := candidate.OutputTypes()
			require.Len(t, got, 2)
			require.Equal(t, test.want, []bool{got[0].NotNullable, got[1].NotNullable})
		})
	}
}

func TestSortPreservesMatrixOneNullOrdering(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag planpb.OrderBySpec_OrderByFlag
		want spb.SortField_SortDirection
	}{
		{name: "implicit ascending", flag: planpb.OrderBySpec_INTERNAL, want: spb.SortField_SORT_DIRECTION_ASC_NULLS_FIRST},
		{name: "explicit ascending", flag: planpb.OrderBySpec_ASC, want: spb.SortField_SORT_DIRECTION_ASC_NULLS_FIRST},
		{name: "default descending", flag: planpb.OrderBySpec_INTERNAL | planpb.OrderBySpec_DESC, want: spb.SortField_SORT_DIRECTION_DESC_NULLS_LAST},
		{name: "explicit ascending last", flag: planpb.OrderBySpec_ASC | planpb.OrderBySpec_NULLS_LAST, want: spb.SortField_SORT_DIRECTION_ASC_NULLS_LAST},
		{name: "explicit descending first", flag: planpb.OrderBySpec_DESC | planpb.OrderBySpec_NULLS_FIRST, want: spb.SortField_SORT_DIRECTION_DESC_NULLS_FIRST},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := scanQuery()
			q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_SORT, Children: []int32{0}, OrderBy: []*planpb.OrderBySpec{{Expr: col(0), Flag: tc.flag}}})
			q.Steps[0] = 1
			candidate, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
			require.NoError(t, err)
			wire, err := candidate.BuildEmbedded(testReadBindings(candidate))
			require.NoError(t, err)
			plan := new(spb.Plan)
			require.NoError(t, proto.Unmarshal(wire, plan))
			require.Equal(t, tc.want, plan.Relations[0].GetRoot().Input.GetSort().Sorts[0].GetDirection())
		})
	}
}

func TestExportRejectsUnserializedOrderingAndScanMetadata(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*planpb.Query)
		want   string
	}{
		{name: "order outside sort", mutate: func(q *planpb.Query) {
			q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*planpb.Expr{col(0)}, OrderBy: []*planpb.OrderBySpec{{Expr: col(0)}}})
			q.Steps[0] = 1
		}, want: "sort semantics outside a SORT node"},
		{name: "index scan", mutate: func(q *planpb.Query) {
			q.Nodes[0].IndexScanInfo = planpb.IndexScanInfo{IsIndexScan: true, IndexName: "idx_a"}
		}, want: "index scan semantics"},
		{name: "index reader", mutate: func(q *planpb.Query) {
			q.Nodes[0].IndexReaderParam = &planpb.IndexReaderParam{Limit: u64(1)}
		}, want: "index scan semantics"},
		{name: "rank", mutate: func(q *planpb.Query) {
			q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_PROJECT, RankOption: &planpb.RankOption{Mode: "pre"}})
		}, want: "rank semantics"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := scanQuery()
			test.mutate(q)
			_, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
			require.ErrorContains(t, err, test.want)
			require.True(t, IsNotEligible(err))
		})
	}
}

func TestExportRejectsFloatSortButRetainsFloatTransport(t *testing.T) {
	q := scanQuery()
	q.Nodes[0].TableDef.Cols[0].Typ = planpb.Type{Id: int32(types.T_float64)}
	q.Nodes[0].ProjectList = []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}}}
	q.Nodes = append(q.Nodes, &planpb.Node{
		NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0},
		ProjectList: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}}},
	})
	q.Steps[0] = 1
	candidate, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	_, err = candidate.BuildEmbedded(testReadBindings(candidate))
	require.NoError(t, err)

	q.Nodes = append(q.Nodes, &planpb.Node{
		NodeId: 2, NodeType: planpb.Node_SORT, Children: []int32{1},
		OrderBy: []*planpb.OrderBySpec{{Expr: &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}}}},
	})
	q.Steps[0] = 2
	_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "floating-point sort")
	require.True(t, IsNotEligible(err))
}

func TestBoundImplicitSortExportsAsAscending(t *testing.T) {
	query := boundSQLQuery(t, "select a from select_test.bind_select order by a")
	sortNode := boundNode(t, query, planpb.Node_SORT)
	require.Len(t, sortNode.OrderBy, 1)
	require.Equal(t, planpb.OrderBySpec_INTERNAL, sortNode.OrderBy[0].Flag)

	plan := buildSubstraitPlan(t, query)
	sort := findSubstraitSort(plan.Relations[0].GetRoot().Input)
	require.NotNil(t, sort)
	require.Len(t, sort.Sorts, 1)
	require.Equal(t, spb.SortField_SORT_DIRECTION_ASC_NULLS_FIRST, sort.Sorts[0].GetDirection())
}

func TestBoundNullOnEmptyAggregateUsesNullableOutput(t *testing.T) {
	for _, tc := range []struct {
		name            string
		sql             string
		wantNullability spb.Type_Nullability
	}{
		{name: "global empty input", sql: "select min(a) from select_test.bind_select where false", wantNullability: spb.Type_NULLABILITY_NULLABLE},
		{name: "global max empty input", sql: "select max(a) from select_test.bind_select where false", wantNullability: spb.Type_NULLABILITY_NULLABLE},
		{name: "grouped", sql: "select a, min(a) from select_test.bind_select group by a", wantNullability: spb.Type_NULLABILITY_REQUIRED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := boundSQLQuery(t, tc.sql)
			aggregateNode := boundNode(t, query, planpb.Node_AGG)
			require.Len(t, aggregateNode.AggList, 1)
			require.True(t, aggregateNode.AggList[0].Typ.NotNullable)
			require.True(t, aggregateNode.AggList[0].GetF().Args[0].Typ.NotNullable)

			plan := buildSubstraitPlan(t, query)
			aggregate := findSubstraitAggregate(plan.Relations[0].GetRoot().Input)
			require.NotNil(t, aggregate)
			require.Len(t, aggregate.Measures, 1)
			require.Equal(t, tc.wantNullability, aggregate.Measures[0].Measure.OutputType.GetI64().GetNullability())
		})
	}
}

func TestBoundNullOnEmptyAggregateComparisonIsNullable(t *testing.T) {
	query := boundSQLQuery(t, "select min(a) = 1 from select_test.bind_select where false")
	aggregateNode := boundNode(t, query, planpb.Node_AGG)
	require.Len(t, aggregateNode.AggList, 1)
	require.True(t, aggregateNode.AggList[0].Typ.NotNullable, "the aggregate node carries the stale binder annotation corrected by export")
	projectNode := boundNode(t, query, planpb.Node_PROJECT)
	require.Len(t, projectNode.ProjectList, 1)
	addExpr := projectNode.ProjectList[0]
	require.False(t, addExpr.Typ.NotNullable)
	require.Len(t, addExpr.GetF().Args, 2)
	require.False(t, addExpr.GetF().Args[0].Typ.NotNullable, "the real bound aggregate ColRef is already nullable")

	plan := buildSubstraitPlan(t, query)
	aggregate := findSubstraitAggregate(plan.Relations[0].GetRoot().Input)
	require.NotNil(t, aggregate)
	require.Equal(t, spb.Type_NULLABILITY_NULLABLE, aggregate.Measures[0].Measure.OutputType.GetI64().GetNullability())
	project := plan.Relations[0].GetRoot().Input.GetProject()
	require.NotNil(t, project)
	require.Len(t, project.Expressions, 1)
	add := project.Expressions[0].GetScalarFunction()
	require.NotNil(t, add)
	require.Equal(t, spb.Type_NULLABILITY_NULLABLE, add.OutputType.GetBool().GetNullability())
}

func TestBoundInt64SumIsAdvertisedWithDecimalResult(t *testing.T) {
	query := boundSQLQuery(t, "select sum(a) from select_test.bind_select")
	aggregateNode := boundNode(t, query, planpb.Node_AGG)
	require.Len(t, aggregateNode.AggList, 1)
	require.Equal(t, int32(types.T_decimal128), aggregateNode.AggList[0].Typ.Id)

	_, err := ExportEmbeddedMO(query, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	require.NoError(t, err)
}

func TestBoundDecimalUnaryMinusLowersToSubtractFromTypedZero(t *testing.T) {
	decimalType := types.New(types.T_decimal64, 18, 2)
	resolved, err := function.GetFunctionByName(context.Background(), "unary_minus", []types.Type{decimalType})
	require.NoError(t, err)
	resultType := resolved.GetReturnType()
	typ := planpb.Type{Id: int32(resultType.Oid), Width: resultType.Width, Scale: resultType.Scale, NotNullable: true}
	argument := &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Decimal64Val{Decimal64Val: &planpb.Decimal64{A: 123}}}}}
	negated := &planpb.Expr{
		Typ: typ,
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "unary_minus", Obj: resolved.GetEncodedOverloadID()},
			Args: []*planpb.Expr{argument},
		}},
	}
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*planpb.Expr{negated}})
	q.Steps[0] = 1
	q.Headings = []string{"negated"}
	candidate, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	wire, err := candidate.BuildEmbedded(testReadBindings(candidate))
	require.NoError(t, err)
	plan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, plan))
	project := plan.Relations[0].GetRoot().Input.GetProject()
	require.NotNil(t, project)
	require.Len(t, project.Expressions, 1)
	subtract := project.Expressions[0].GetScalarFunction()
	require.NotNil(t, subtract)
	require.Equal(t, "subtract", scalarFunctionName(plan, subtract.FunctionReference))
	require.Len(t, subtract.Arguments, 2)
	require.NotNil(t, subtract.Arguments[0].GetValue().GetLiteral().GetDecimal())
}

func TestFetchAcceptsBoundUint64AndPreservesAbsentCount(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*planpb.Expr{col(0)}, Offset: u64(2)})
	q.Steps[0] = 1
	candidate, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	wire, err := candidate.BuildEmbedded(testReadBindings(candidate))
	require.NoError(t, err)
	plan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, plan))
	fetch := plan.Relations[0].GetRoot().Input.GetFetch()
	require.Equal(t, int64(2), fetch.GetOffset())
	require.NotNil(t, fetch.CountMode)
	require.Equal(t, int64(-1), fetch.GetCount())
}

func TestFetchFallsBackForBoundUint64OutsideSubstraitRange(t *testing.T) {
	query := boundSQLQuery(t, "select a from select_test.bind_select limit 18446744073709551615")
	var limit *planpb.Expr
	for _, node := range query.Nodes {
		if node != nil && node.Limit != nil {
			limit = node.Limit
			break
		}
	}
	require.NotNil(t, limit)
	require.Equal(t, uint64(math.MaxUint64), limit.GetLit().GetU64Val())

	// An above-scan fetch belongs to Sirius and must fit its signed count.
	root := query.Steps[len(query.Steps)-1]
	query.Nodes = append(query.Nodes, &planpb.Node{NodeId: int32(len(query.Nodes)), NodeType: planpb.Node_PROJECT, Children: []int32{root}, ProjectList: []*planpb.Expr{col(0)}, Limit: limit})
	query.Steps[len(query.Steps)-1] = int32(len(query.Nodes) - 1)
	_, err := ExportEmbeddedMO(query, NewEmbeddedExportProfile(0))
	require.True(t, IsNotEligible(err))
	reason, ok := NotEligibleReason(err)
	require.True(t, ok)
	require.Equal(t, EligibilityExpression, reason)
}

func TestPlannerModAliasUsesExactSemanticRegistry(t *testing.T) {
	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*planpb.Expr{fn("mod", i64Type(), col(0), i64(2))}})
	q.Steps[0] = 1
	candidate, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	wire, err := candidate.BuildEmbedded(testReadBindings(candidate))
	require.NoError(t, err)
	plan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, plan))
	require.Equal(t, "modulus", plan.Extensions[0].GetExtensionFunction().Name)

	floatInputs := []types.Type{types.T_float64.ToType(), types.T_float64.ToType()}
	floatMod, err := function.GetFunctionByName(context.Background(), "mod", floatInputs)
	require.NoError(t, err)
	floatResult := floatMod.GetReturnType()
	q = scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, ProjectList: []*planpb.Expr{{
		Typ:  planpb.Type{Id: int32(floatResult.Oid), Width: floatResult.Width, Scale: floatResult.Scale},
		Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "mod", Obj: floatMod.GetEncodedOverloadID()}, Args: []*planpb.Expr{f64(7.5), f64(2)}}},
	}}})
	q.Steps[0] = 1
	_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "no declared Sirius semantic equivalence")
}

func TestSemanticRegistryRejectsForgedNullability(t *testing.T) {
	registry, err := loadSemanticRegistry()
	require.NoError(t, err)
	require.NotEmpty(t, registry)
	resolved, err := function.GetFunctionByName(
		context.Background(),
		"mod",
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
	)
	require.NoError(t, err)
	ref := &planpb.ObjectRef{ObjName: "mod", Obj: resolved.GetEncodedOverloadID()}
	args := []*planpb.Expr{{Typ: i64Type()}, {Typ: i64Type()}}

	supported, err := hasSemanticCapability(semanticScalar, "modulus", ref, args, ptrType(i64Type()))
	require.NoError(t, err)
	require.True(t, supported)
	forgedResult := i64Type()
	forgedResult.NotNullable = true
	supported, err = hasSemanticCapability(semanticScalar, "modulus", ref, args, &forgedResult)
	require.NoError(t, err)
	require.False(t, supported)

	for i := range args {
		args[i].Typ.NotNullable = true
	}
	supported, err = hasSemanticCapability(semanticScalar, "modulus", ref, args, &forgedResult)
	require.NoError(t, err)
	require.False(t, supported)
	supported, err = hasSemanticCapability(semanticScalar, "modulus", ref, args, ptrType(i64Type()))
	require.NoError(t, err)
	require.True(t, supported)
}

func TestSemanticNullabilityUsesConcreteNonNullLiteralFact(t *testing.T) {
	resolved, err := function.GetFunctionByName(
		context.Background(),
		"=",
		[]types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
	)
	require.NoError(t, err)

	notNullColumn := col(0)
	notNullColumn.Typ.NotNullable = true
	nonNullLiteral := i64(1)
	require.False(t, nonNullLiteral.Typ.NotNullable, "the planner literal annotation is deliberately conservative")
	require.True(t, semanticNotNullable(
		resolved.GetEncodedOverloadID(), []*planpb.Expr{notNullColumn, nonNullLiteral}))
	// Folding a previously nullable expression need not rewrite its parent's
	// conservative annotation. Both annotations describe the same safe call.
	ref := &planpb.ObjectRef{ObjName: "=", Obj: resolved.GetEncodedOverloadID()}
	for _, notNullable := range []bool{false, true} {
		out := boolType()
		out.NotNullable = notNullable
		supported, err := hasSemanticCapability(semanticScalar, "equal", ref,
			[]*planpb.Expr{notNullColumn, nonNullLiteral}, &out)
		require.NoError(t, err)
		require.True(t, supported)
	}

	nullLiteral := i64(1)
	nullLiteral.GetLit().Isnull = true
	require.False(t, semanticNotNullable(
		resolved.GetEncodedOverloadID(), []*planpb.Expr{notNullColumn, nullLiteral}))
	nullableColumn := col(0)
	require.False(t, semanticNotNullable(
		resolved.GetEncodedOverloadID(), []*planpb.Expr{nullableColumn, nonNullLiteral}))

	query := boundSQLQuery(t, "select a = 1, case when a > 0 then a else 0 end from select_test.bind_select")
	_, err = ExportEmbeddedMO(query, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
}

func TestExportRequiresCompleteBoundOutputHeadings(t *testing.T) {
	q := scanQuery()
	q.Headings = nil
	_, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "headings")
}

func TestStarCountAggregateLowering(t *testing.T) {
	q := scanQuery()
	countType := i64Type()
	countType.NotNullable = true
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}, AggList: []*planpb.Expr{fn("starcount", countType, i64(1))}})
	q.Steps[0] = 1
	c, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	b, err := c.BuildEmbedded(testReadBindings(c))
	require.NoError(t, err)
	p := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(b, p))
	require.Len(t, p.Relations[0].GetRoot().Input.GetAggregate().Measures[0].Measure.Arguments, 1)

	q = scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}, AggList: []*planpb.Expr{fn("starcount", countType, col(0))}})
	q.Steps[0] = 1
	_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "non-NULL literal")
}

func TestSubstraitTypeAndLiteralMappings(t *testing.T) {
	typesAndLiterals := []struct {
		typ planpb.Type
		lit *planpb.Literal
	}{
		{typ: planpb.Type{Id: int32(types.T_bool)}, lit: &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}}},
		{typ: planpb.Type{Id: int32(types.T_int8)}, lit: &planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: 1}}},
		{typ: planpb.Type{Id: int32(types.T_int16)}, lit: &planpb.Literal{Value: &planpb.Literal_I16Val{I16Val: 2}}},
		{typ: planpb.Type{Id: int32(types.T_int32)}, lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 3}}},
		{typ: planpb.Type{Id: int32(types.T_int64)}, lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 4}}},
		{typ: planpb.Type{Id: int32(types.T_float32)}, lit: &planpb.Literal{Value: &planpb.Literal_Fval{Fval: 1.5}}},
		{typ: planpb.Type{Id: int32(types.T_float64)}, lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: 2.5}}},
		{typ: planpb.Type{Id: int32(types.T_varchar), Width: 12}, lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "varchar"}}},
	}
	for _, tc := range typesAndLiterals {
		_, err := substraitType(&tc.typ)
		require.NoError(t, err)
		_, err = literal(tc.lit, &tc.typ)
		require.NoError(t, err)
	}

	nullType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	_, err := literal(&planpb.Literal{Isnull: true}, &nullType)
	require.NoError(t, err)
	_, err = literal(nil, &nullType)
	require.ErrorContains(t, err, "nil literal")
	_, err = literal(&planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 1}}, &nullType)
	require.ErrorContains(t, err, "unsupported literal")
	_, err = literal(&planpb.Literal{Value: &planpb.Literal_Sval{Sval: "wrong"}}, &nullType)
	require.ErrorContains(t, err, "does not match")
	_, err = substraitType(nil)
	require.ErrorContains(t, err, "missing type")
	_, err = substraitType(&planpb.Type{Id: int32(types.T_varchar), Width: -1})
	require.ErrorContains(t, err, "negative varchar")
	_, err = substraitType(&planpb.Type{Id: int32(types.T_decimal64)})
	require.ErrorContains(t, err, "outside the supported bound")

	charType := planpb.Type{Id: int32(types.T_char), Width: 5}
	mappedChar, err := substraitType(&charType)
	require.NoError(t, err)
	require.Equal(t, int32(5), mappedChar.GetVarchar().GetLength())
	charLiteral, err := literal(
		&planpb.Literal{Value: &planpb.Literal_Sval{Sval: "x  "}}, &charType)
	require.NoError(t, err)
	require.Equal(t, "x  ", charLiteral.GetLiteral().GetVarChar().GetValue(), "wire canonicalization must not trim or pad CHAR bytes")
	require.Equal(t, uint32(5), charLiteral.GetLiteral().GetVarChar().GetLength())
	_, err = substraitType(&planpb.Type{Id: int32(types.T_char), Width: -1})
	require.ErrorContains(t, err, "negative char width")
}

func TestBoundCharacterSubstringResultWidths(t *testing.T) {
	query := boundSQLQuery(t, "select substring(c_phone, 1, 2) as prefix from tpch.customer")
	project := boundNode(t, query, planpb.Node_PROJECT)
	require.Equal(t, int32(2), project.ProjectList[0].Typ.Width)
	buildSubstraitPlan(t, query)

	for _, oid := range []types.T{types.T_char, types.T_varchar} {
		t.Run(oid.String(), func(t *testing.T) {
			source := &planpb.Expr{Typ: planpb.Type{Id: int32(oid), Width: 15}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
			for _, literalLength := range []bool{true, false} {
				length := col(1)
				if literalLength {
					length = i64(2)
				}
				bound, err := planbuilder.BindFuncExprImplByPlanExpr(context.Background(), "substring", []*planpb.Expr{source, i64(1), length})
				require.NoError(t, err)
				f := bound.GetF()
				require.NotNil(t, f)
				wantWidth := int32(15)
				if literalLength {
					wantWidth = 2
				}
				require.Equal(t, wantWidth, bound.Typ.Width)
				for _, width := range []int32{2, 3, 15} {
					out := bound.Typ
					out.Width = width
					supported, err := hasSemanticCapability(semanticScalar, "substring", f.Func, f.Args, &out)
					require.NoError(t, err)
					require.Equal(t, width == 15 || literalLength && width == 2, supported, "literal=%t width=%d", literalLength, width)
				}
				for _, mutate := range []func(*planpb.Type){
					func(out *planpb.Type) { out.Id = int32(types.T_varbinary) },
					func(out *planpb.Type) { out.Scale++ },
					func(out *planpb.Type) { out.Charset = uint32(types.CharsetBinary) },
					func(out *planpb.Type) { out.NotNullable = !out.NotNullable },
				} {
					out := bound.Typ
					mutate(&out)
					supported, err := hasSemanticCapability(semanticScalar, "substring", f.Func, f.Args, &out)
					require.NoError(t, err)
					require.False(t, supported)
				}
				wrongOverload := *f.Func
				wrongOverload.Obj = function.EncodeOverloadID(function.SUBSTRING, 999)
				supported, err := hasSemanticCapability(semanticScalar, "substring", &wrongOverload, f.Args, &bound.Typ)
				require.NoError(t, err)
				require.False(t, supported)
			}
		})
	}
}

func TestCharacterSubstringExportDomainProof(t *testing.T) {
	for _, tc := range []struct {
		name     string
		source   string
		eligible bool
	}{
		{"column", "c_phone", true},
		{"nested", "substring(c_phone, 1, 5)", true},
		{"three slices", "substring(substring(c_phone, 1, 8), 1, 5)", true},
		// Full cast admission still requires the exact overload result width;
		// the non-narrowing type predicate alone is not an export proof.
		{"undeclared widening cast", "cast(substring(c_phone, 1, 5) as varchar(15))", false},
		{"text case", "case when c_custkey > 1 then substring(c_phone, 1, 5) else substring(c_phone, 1, 5) end", true},
		{"binary", "substring(cast(c_phone as binary), 1, 5)", false},
		{"mixed case", "case when c_custkey > 1 then c_phone else cast(c_phone as binary) end", false},
		{"truncating cast", "substring(cast(c_phone as varchar(3)), 1, 2)", false},
		{"truncating after slice", "cast(substring(c_phone, 1, 5) as varchar(3))", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := boundSQLQuery(t, "select substring("+tc.source+", 1, 2) as prefix from tpch.customer")
			if tc.eligible {
				buildSubstraitPlan(t, query)
			} else {
				_, err := ExportEmbeddedMO(query, NewEmbeddedExportProfile(0))
				require.True(t, IsNotEligible(err), err)
			}
		})
	}
	query := boundSQLQuery(t, "select substring(case when c_custkey > 1 then c_phone else c_phone end, 1, 2) as prefix from tpch.customer")
	buildSubstraitPlan(t, query)
	caseSource := boundNode(t, query, planpb.Node_PROJECT).ProjectList[0].GetF().Args[0]
	require.True(t, tpchCharacterSliceSourceIsText(caseSource))
	caseSource.GetF().Args[2] = &planpb.Expr{Typ: caseSource.Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	require.True(t, tpchCharacterSliceSourceIsText(caseSource), "a folded NULL arm contributes no runtime string domain")
	// BuildPlan retains CAST(ANY NULL AS VARCHAR) in this spelling. That
	// child cast was already ineligible even with the legacy parent width.
	query = boundSQLQuery(t, "select substring(case when c_custkey > 1 then c_phone else NULL end, 1, 2) as prefix from tpch.customer")
	boundNode(t, query, planpb.Node_PROJECT).ProjectList[0].Typ.Width = 15
	_, err := ExportEmbeddedMO(query, NewEmbeddedExportProfile(0))
	require.Error(t, err)
	require.True(t, IsNotEligible(err))
	query = boundSQLQuery(t, "select substring(cast(c_phone as varchar(3)), 1, 2) as prefix from tpch.customer")
	_, err = ExportEmbeddedMO(query, NewEmbeddedExportProfile(0))
	require.Error(t, err, "narrow substring must not hide an ineligible truncating child cast")
	require.True(t, IsNotEligible(err))

	source := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 15}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
	bound, err := planbuilder.BindFuncExprImplByPlanExpr(context.Background(), "substring", []*planpb.Expr{source, i64(1), i64(2)})
	require.NoError(t, err)
	binary := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varbinary), Width: 15}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
	for _, tc := range []struct {
		name   string
		source *planpb.Expr
		text   bool
	}{
		{"retained text", source, true},
		{"retained binary", binary, false},
		{"declaration without value", &planpb.Expr{Typ: source.Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			boundary := planbuilder.DeepCopyExpr(source)
			boundary.PreparedNumeric = &planpb.PreparedNumericMetadata{StringDomainSource: tc.source}
			require.Equal(t, tc.text, tpchCharacterSliceSourceIsText(boundary), "columns still require retained input provenance")
		})
	}
	for _, mutate := range []func(*planpb.Expr){
		func(e *planpb.Expr) { e.Typ.Charset = uint32(types.CharsetBinary) },
		func(e *planpb.Expr) { e.Typ.Charset = 256 },
		func(e *planpb.Expr) { e.Expr = &planpb.Expr_P{P: &planpb.ParamRef{}} },
		func(e *planpb.Expr) { e.PreparedNumeric = &planpb.PreparedNumericMetadata{StringDomainSource: binary} },
		func(e *planpb.Expr) {
			e.Expr = &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "abcd"}, LiteralForm: planpb.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER}}
		},
		func(e *planpb.Expr) {
			e.Expr = &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "abcd"}, Src: binary}}
		},
	} {
		forged := planbuilder.DeepCopyExpr(bound)
		mutate(forged.GetF().Args[0])
		f := forged.GetF()
		supported, err := hasSemanticCapability(semanticScalar, "substring", f.Func, f.Args, &forged.Typ)
		require.NoError(t, err)
		require.False(t, supported)
	}
	forged := planbuilder.DeepCopyExpr(bound)
	forged.GetF().Args[2].Expr = &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}
	for _, width := range []int32{2, 15} {
		forged.Typ.Width = width
		f := forged.GetF()
		supported, err := hasSemanticCapability(semanticScalar, "substring", f.Func, f.Args, &forged.Typ)
		require.NoError(t, err)
		require.Equal(t, width == 15, supported)
	}
}

func TestCharUsesTheVarcharSemanticFamily(t *testing.T) {
	charType := planpb.Type{Id: int32(types.T_char), Width: 10, NotNullable: true}
	varcharType := planpb.Type{Id: int32(types.T_varchar), Width: 10, NotNullable: true}
	charExpr := &planpb.Expr{Typ: charType, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	varcharExpr := &planpb.Expr{Typ: varcharType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
		Value: &planpb.Literal_Sval{Sval: "AIR "},
	}}}
	out := boolType()
	out.NotNullable = true
	require.NoError(t, validateScalarSignature("equal", &out, []*planpb.Expr{charExpr, varcharExpr}))
	require.NoError(t, validateScalarSignature("like", &out, []*planpb.Expr{charExpr, varcharExpr}))

	resolved, err := function.GetFunctionByName(
		context.Background(), "=", []types.Type{types.New(types.T_char, 10, 0), types.New(types.T_char, 10, 0)})
	require.NoError(t, err)
	charLiteral := *varcharExpr
	charLiteral.Typ = charType
	supported, err := hasSemanticCapability(
		semanticScalar,
		"equal",
		&planpb.ObjectRef{Obj: resolved.GetEncodedOverloadID(), ObjName: "="},
		[]*planpb.Expr{charExpr, &charLiteral},
		&out,
	)
	require.NoError(t, err)
	require.True(t, supported)
}

func TestExportRejectsWidthSensitiveStringCasts(t *testing.T) {
	for _, target := range []string{"char(3)", "varchar(3)"} {
		t.Run(target, func(t *testing.T) {
			// MatrixOne truncates this public SQL cast to three runes. Sirius
			// executes its Substrait VARCHAR target without enforcing the bound,
			// so Export must decline rather than expose divergent result bytes.
			query := boundSQLQuery(t, "select cast('abcd' as "+target+") from tpch.nation")
			_, err := ExportEmbeddedMO(query, NewEmbeddedExportProfile(0))
			require.True(t, IsNotEligible(err))
			require.ErrorContains(t, err, "cast overload")
		})
	}
}

func TestTPCHCastStringWidthAdmission(t *testing.T) {
	varchar6 := &planpb.Type{Id: int32(types.T_varchar), Width: 6}
	char6 := &planpb.Type{Id: int32(types.T_char), Width: 6}
	for _, tc := range []struct {
		name   string
		input  *planpb.Type
		output *planpb.Type
		want   bool
	}{
		{name: "varchar widening", input: varchar6, output: &planpb.Type{Id: int32(types.T_varchar), Width: 25}, want: true},
		{name: "varchar unbounded", input: varchar6, output: &planpb.Type{Id: int32(types.T_varchar)}, want: true},
		{name: "varchar narrowing", input: varchar6, output: &planpb.Type{Id: int32(types.T_varchar), Width: 3}, want: false},
		{name: "char target", input: varchar6, output: &planpb.Type{Id: int32(types.T_char), Width: 25}, want: false},
		{name: "char source", input: char6, output: &planpb.Type{Id: int32(types.T_varchar), Width: 25}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tpchCastTypes(tc.input, tc.output))
		})
	}
}

func TestExportAcceptsDateAndRejectsTimestamp(t *testing.T) {
	dateType := planpb.Type{Id: int32(types.T_date)}
	q := scanQuery()
	q.Nodes[0].TableDef.Cols[0].Typ = dateType
	_, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)

	q = scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{
		NodeId:   1,
		NodeType: planpb.Node_PROJECT,
		Children: []int32{0},
		ProjectList: []*planpb.Expr{{Typ: dateType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Dateval{Dateval: int32(types.DateFromCalendar(1970, 1, 1))},
		}}}},
	})
	q.Steps[0] = 1
	_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.NoError(t, err)

	timestampType := planpb.Type{Id: int32(types.T_timestamp), Scale: 6}
	q = scanQuery()
	q.Nodes[0].TableDef.Cols[0].Typ = timestampType
	_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.True(t, IsNotEligible(err))
	reason, ok := NotEligibleReason(err)
	require.True(t, ok)
	require.Equal(t, EligibilityType, reason)
}

func TestEligibilityDeclinesAreDistinctFromMalformedAndOperationalErrors(t *testing.T) {
	unsupported := scanQuery()
	unsupported.Nodes = append(unsupported.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_JOIN})
	unsupported.Steps[0] = 1
	_, err := ExportEmbeddedMO(unsupported, NewEmbeddedExportProfile(0))
	require.True(t, IsNotEligible(err))
	reason, ok := NotEligibleReason(err)
	require.True(t, ok)
	require.Equal(t, EligibilityOperator, reason)

	malformed := scanQuery()
	malformed.Nodes[0].ObjRef = nil
	_, err = ExportEmbeddedMO(malformed, NewEmbeddedExportProfile(0))
	require.Error(t, err)
	require.False(t, IsNotEligible(err))

	missingDatabaseID := scanQuery()
	missingDatabaseID.Nodes[0].ObjRef.Db = 7
	missingDatabaseID.Nodes[0].TableDef.DbId = 0
	_, err = ExportEmbeddedMO(missingDatabaseID, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "malformed table identity")
	require.False(t, IsNotEligible(err))

	candidate, err := ExportEmbeddedMO(scanQuery(), NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	_, err = candidate.BuildEmbedded(nil)
	require.Error(t, err)
	require.False(t, IsNotEligible(err), "post-export build failures are operational")
}

func TestScalarSignatureContracts(t *testing.T) {
	boolExpr := &planpb.Expr{Typ: boolType()}
	intExpr := &planpb.Expr{Typ: i64Type()}
	require.NoError(t, validateScalarSignature("and", ptrType(boolType()), []*planpb.Expr{boolExpr, boolExpr}))
	require.NoError(t, validateScalarSignature("equal", ptrType(boolType()), []*planpb.Expr{intExpr, intExpr}))
	require.NoError(t, validateScalarSignature("is_null", ptrType(boolType()), []*planpb.Expr{intExpr}))
	require.NoError(t, validateScalarSignature("add", ptrType(i64Type()), []*planpb.Expr{intExpr, intExpr}))
	require.ErrorContains(t, validateScalarSignature("and", ptrType(i64Type()), []*planpb.Expr{boolExpr, boolExpr}), "non-boolean result")
	require.ErrorContains(t, validateScalarSignature("or", ptrType(boolType()), []*planpb.Expr{intExpr, intExpr}), "non-boolean argument")
	require.ErrorContains(t, validateScalarSignature("equal", ptrType(boolType()), []*planpb.Expr{intExpr, boolExpr}), "unsupported equal")
	require.ErrorContains(t, validateScalarSignature("is_not_null", ptrType(i64Type()), []*planpb.Expr{intExpr}), "unsupported is_not_null")
	require.ErrorContains(t, validateScalarSignature("divide", ptrType(boolType()), []*planpb.Expr{boolExpr, boolExpr}), "unsupported divide")
}

func TestExportAcceptsBinderNullPredicateAliases(t *testing.T) {
	for _, name := range []string{"isnull", "isnotnull"} {
		t.Run(name, func(t *testing.T) {
			q := scanQuery()
			resultType := boolType()
			resultType.NotNullable = true
			q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_FILTER, Children: []int32{0}, FilterList: []*planpb.Expr{fn(name, resultType, col(0))}})
			q.Steps[0] = 1
			_, err := ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
			require.NoError(t, err)
		})
	}
}

func TestNonnegativeIntegerLiteralForms(t *testing.T) {
	for _, expr := range []*planpb.Expr{
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: 1}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I16Val{I16Val: 2}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 3}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 4}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: math.MaxInt64}}}},
	} {
		value, err := nonnegativeIntLiteral(expr, -1)
		require.NoError(t, err)
		require.Positive(t, value)
	}
	value, err := nonnegativeIntLiteral(nil, 7)
	require.NoError(t, err)
	require.Equal(t, int64(7), value)
	_, err = nonnegativeIntLiteral(&planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}, 0)
	require.ErrorContains(t, err, "constant integer")
	_, err = nonnegativeIntLiteral(&planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "1"}}}}, 0)
	require.ErrorContains(t, err, "integer")
	_, err = nonnegativeIntLiteral(&planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: -1}}}}, 0)
	require.ErrorContains(t, err, "non-negative")
	_, err = nonnegativeIntLiteral(&planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: math.MaxInt64 + 1}}}}, 0)
	require.ErrorContains(t, err, "signed integer range")
	require.True(t, IsNotEligible(err))
}

func TestExporterValidationGuards(t *testing.T) {
	_, err := (*Candidate)(nil).BuildEmbedded(nil)
	require.ErrorContains(t, err, "nil embedded candidate")
	candidate, err := ExportEmbeddedMO(scanQuery(), NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	_, err = candidate.BuildEmbedded(nil)
	require.ErrorContains(t, err, "binding count")

	widthExporter := &exporter{query: &planpb.Query{Nodes: []*planpb.Node{
		{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, ProjectList: []*planpb.Expr{col(0)}},
		{NodeId: 1, NodeType: planpb.Node_TABLE_SCAN},
		{NodeId: 2, NodeType: planpb.Node_FILTER},
		{NodeId: 3, NodeType: planpb.Node_JOIN},
	}}}
	width, err := widthExporter.nodeWidth(0)
	require.NoError(t, err)
	require.Equal(t, 1, width)
	_, err = widthExporter.nodeWidth(-1)
	require.ErrorContains(t, err, "invalid node id")
	_, err = widthExporter.nodeWidth(1)
	require.ErrorContains(t, err, "scan has no table")
	_, err = widthExporter.nodeWidth(2)
	require.ErrorContains(t, err, "requires one child")
	_, err = widthExporter.nodeWidth(3)
	require.ErrorContains(t, err, "unsupported width")

	exprExporter := &exporter{functions: make(map[string]uint32)}
	_, err = exprExporter.conjunction(nil, []int{1})
	require.ErrorContains(t, err, "empty filter")
	_, err = exprExporter.conjunction([]*planpb.Expr{intExpr(1)}, []int{1})
	require.ErrorContains(t, err, "not boolean")
	predicate := fn(">", boolType(), col(0), i64(1))
	_, err = exprExporter.conjunction([]*planpb.Expr{predicate, predicate}, []int{1})
	require.NoError(t, err)
	require.ErrorContains(t, validateExprFields([]*planpb.Expr{nil}, []int{1}), "nil expression")
	require.ErrorContains(t, validateExprFields([]*planpb.Expr{{Expr: &planpb.Expr_F{}}}, []int{1}), "malformed function")
	require.ErrorContains(t, validateExprFields([]*planpb.Expr{{}}, []int{1}), "unsupported expression")

	q := scanQuery()
	q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}, GroupingFlag: []bool{true}})
	q.Steps[0] = 1
	_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
	require.ErrorContains(t, err, "grouping sets")

	for _, order := range []*planpb.OrderBySpec{
		nil,
		{Expr: col(0), Collation: "utf8"},
		{Expr: col(0), Flag: planpb.OrderBySpec_UNIQUE},
		{Expr: col(0), Flag: planpb.OrderBySpec_ASC | planpb.OrderBySpec_DESC},
		{Expr: col(0), Flag: planpb.OrderBySpec_NULLS_FIRST | planpb.OrderBySpec_NULLS_LAST},
		{Expr: col(0), Flag: planpb.OrderBySpec_OrderByFlag(1 << 15)},
	} {
		q = scanQuery()
		q.Nodes = append(q.Nodes, &planpb.Node{NodeId: 1, NodeType: planpb.Node_SORT, Children: []int32{0}, OrderBy: []*planpb.OrderBySpec{order}})
		q.Steps[0] = 1
		_, err = ExportEmbeddedMO(q, NewEmbeddedExportProfile(0))
		require.Error(t, err)
	}
}

func ptrType(value planpb.Type) *planpb.Type { return &value }

func intExpr(value int64) *planpb.Expr { return i64(value) }

func boundSQLQuery(t *testing.T, sql string) *planpb.Query {
	t.Helper()
	statement, err := mysql.ParseOne(context.Background(), sql, 1)
	require.NoError(t, err)
	built, err := planbuilder.BuildPlan(planbuilder.NewMockCompilerContext(false, newPlanTestProcess(t)), statement, false)
	require.NoError(t, err)
	query := built.GetQuery()
	require.NotNil(t, query)
	for _, node := range query.Nodes {
		if node == nil || node.NodeType != planpb.Node_TABLE_SCAN {
			continue
		}
		require.NotNil(t, node.ObjRef)
		require.NotNil(t, node.TableDef)
		// The mock compiler intentionally leaves catalog IDs unset. Supply only
		// those physical identities so Export sees an otherwise real bound plan.
		node.TableDef.DbId = 7
		if node.TableDef.TblId == 0 {
			node.TableDef.TblId = uint64(node.NodeId) + 42
		}
		node.ObjRef.Obj = int64(node.TableDef.TblId)
	}
	return query
}

func boundNode(t *testing.T, query *planpb.Query, nodeType planpb.Node_NodeType) *planpb.Node {
	t.Helper()
	var found *planpb.Node
	for _, node := range query.Nodes {
		if node != nil && node.NodeType == nodeType {
			require.Nil(t, found, "multiple %s nodes", nodeType.String())
			found = node
		}
	}
	require.NotNil(t, found, "missing %s node", nodeType.String())
	return found
}

func buildSubstraitPlan(t *testing.T, query *planpb.Query) *spb.Plan {
	t.Helper()
	candidate, err := ExportEmbeddedMO(query, NewEmbeddedExportProfile(0))
	require.NoError(t, err)
	wire, err := candidate.BuildEmbedded(testReadBindings(candidate))
	require.NoError(t, err)
	plan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, plan))
	return plan
}

func findSubstraitAggregate(rel *spb.Rel) *spb.AggregateRel {
	for rel != nil {
		if aggregate := rel.GetAggregate(); aggregate != nil {
			return aggregate
		}
		switch {
		case rel.GetProject() != nil:
			rel = rel.GetProject().Input
		case rel.GetFilter() != nil:
			rel = rel.GetFilter().Input
		case rel.GetSort() != nil:
			rel = rel.GetSort().Input
		case rel.GetFetch() != nil:
			rel = rel.GetFetch().Input
		default:
			return nil
		}
	}
	return nil
}

func findSubstraitSort(rel *spb.Rel) *spb.SortRel {
	for rel != nil {
		if sort := rel.GetSort(); sort != nil {
			return sort
		}
		switch {
		case rel.GetProject() != nil:
			rel = rel.GetProject().Input
		case rel.GetFilter() != nil:
			rel = rel.GetFilter().Input
		case rel.GetAggregate() != nil:
			rel = rel.GetAggregate().Input
		case rel.GetFetch() != nil:
			rel = rel.GetFetch().Input
		default:
			return nil
		}
	}
	return nil
}

func scalarFunctionName(plan *spb.Plan, anchor uint32) string {
	for _, declaration := range plan.Extensions {
		function := declaration.GetExtensionFunction()
		if function != nil && function.FunctionAnchor == anchor {
			return function.Name
		}
	}
	return ""
}

func scanQuery() *planpb.Query {
	return &planpb.Query{StmtType: planpb.Query_SELECT, Steps: []int32{0}, Headings: []string{"a"}, Nodes: []*planpb.Node{{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Obj: 42, ObjName: "t"}, TableDef: &planpb.TableDef{DbId: 7, TblId: 42, Version: 3, Name: "t", TableType: "r", Cols: []*planpb.ColDef{{Name: "a", ColId: 11, Seqnum: 5, Typ: i64Type()}}}}}}
}
func i64Type() planpb.Type  { return planpb.Type{Id: int32(types.T_int64)} }
func boolType() planpb.Type { return planpb.Type{Id: int32(types.T_bool)} }
func f64Type() planpb.Type  { return planpb.Type{Id: int32(types.T_float64)} }
func col(pos int32) *planpb.Expr {
	return &planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos}}}
}
func i64(v int64) *planpb.Expr {
	return &planpb.Expr{Typ: i64Type(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: v}}}}
}
func u64(v uint64) *planpb.Expr {
	return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: v}}}}
}
func f64(v float64) *planpb.Expr {
	return &planpb.Expr{Typ: f64Type(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: v}}}}
}
func fn(name string, typ planpb.Type, args ...*planpb.Expr) *planpb.Expr {
	ids := map[string]int32{"=": function.EQUAL, ">": function.GREAT_THAN, "not": function.NOT, "isnull": function.ISNULL, "isnotnull": function.ISNOTNULL, "mod": function.MOD, "sum": function.SUM, "min": function.MIN, "starcount": function.STARCOUNT}
	id, ok := ids[name]
	if !ok {
		panic("missing test function id: " + name)
	}
	return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: name, Obj: function.EncodeOverloadID(id, 0)}, Args: args}}}
}

func testReadBindings(candidate *Candidate) map[int32]EmbeddedReadBinding {
	if candidate == nil {
		return nil
	}
	bindings := make(map[int32]EmbeddedReadBinding, len(candidate.reads))
	for i, read := range candidate.reads {
		bindings[read.NodeID] = EmbeddedReadBinding{BindingID: uint64(i + 1), Source: EmbeddedReadMO}
	}
	return bindings
}
