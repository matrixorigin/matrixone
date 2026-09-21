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
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

func TestEmbeddedMOReadReplacesCompleteScan(t *testing.T) {
	query := embeddedProjectedScanQuery()
	candidate, err := Export(query)
	require.NoError(t, err)

	reads, err := candidate.EmbeddedMOReads()
	require.NoError(t, err)
	require.Equal(t, []EmbeddedMORead{{
		NodeID: 0, Database: "object_db", Table: "object_table", Schema: "object_schema",
		Columns: []EmbeddedMOColumn{
			{Name: "col_0", Type: i64Type(), PhysicalID: 22, Sequence: 6},
			{Name: "col_1", Type: i64Type(), PhysicalID: 11, Sequence: 5},
		},
	}}, reads)

	wire, err := candidate.BuildEmbedded(map[int32]EmbeddedReadBinding{0: {BindingID: 1, Source: EmbeddedReadMO}})
	require.NoError(t, err)
	plan := embeddedPlan(t, wire)
	require.Empty(t, plan.ExpectedTypeUrls)
	input := plan.Relations[0].GetRoot().Input
	require.NotNil(t, input.GetRead(), "MO input replaces filter, project, and fetch")
	require.Equal(t, []string{embeddedNamedTable, "1"}, input.GetRead().GetNamedTable().Names)
	require.Equal(t, []string{"col_0", "col_1"}, input.GetRead().BaseSchema.Names)
	require.Len(t, input.GetRead().BaseSchema.Struct.Types, 2)
}

func TestEmbeddedMOReadIdentityFallsBackToTableDefinition(t *testing.T) {
	query := embeddedProjectedScanQuery()
	query.Nodes[0].ObjRef.DbName = ""
	query.Nodes[0].ObjRef.ObjName = ""
	query.Nodes[0].ObjRef.SchemaName = ""
	candidate, err := Export(query)
	require.NoError(t, err)
	reads, err := candidate.EmbeddedMOReads()
	require.NoError(t, err)
	require.Equal(t, "table_db", reads[0].Database)
	require.Equal(t, "table_name", reads[0].Table)
	require.Empty(t, reads[0].Schema)
}

func TestEmbeddedTAEPreservesScanOperationsAndPhysicalSchema(t *testing.T) {
	candidate, err := Export(embeddedProjectedScanQuery())
	require.NoError(t, err)
	wire, err := candidate.BuildEmbedded(map[int32]EmbeddedReadBinding{0: {BindingID: 1, Source: EmbeddedReadTAE}})
	require.NoError(t, err)

	input := embeddedPlan(t, wire).Relations[0].GetRoot().Input
	require.NotNil(t, input.GetFetch())
	project := input.GetFetch().Input.GetProject()
	require.NotNil(t, project)
	filter := project.Input.GetFilter()
	require.NotNil(t, filter)
	read := filter.Input.GetRead()
	require.NotNil(t, read)
	require.Equal(t, []string{embeddedNamedTable, "1"}, read.GetNamedTable().Names)
	require.Equal(t, []string{"a", "b"}, read.BaseSchema.Names)
}

func TestBuildEmbeddedSupportsMixedReadSources(t *testing.T) {
	candidate, err := Export(embeddedJoinQuery(false))
	require.NoError(t, err)
	wire, err := candidate.BuildEmbedded(map[int32]EmbeddedReadBinding{
		0: {BindingID: 1, Source: EmbeddedReadMO},
		1: {BindingID: 2, Source: EmbeddedReadTAE},
	})
	require.NoError(t, err)

	join := embeddedPlan(t, wire).Relations[0].GetRoot().Input.GetJoin()
	require.NotNil(t, join)
	require.Equal(t, []string{embeddedNamedTable, "1"}, join.Left.GetRead().GetNamedTable().Names)
	require.Equal(t, []string{"col_0"}, join.Left.GetRead().BaseSchema.Names)
	require.Equal(t, []string{embeddedNamedTable, "2"}, join.Right.GetRead().GetNamedTable().Names)
	require.Equal(t, []string{"right_value"}, join.Right.GetRead().BaseSchema.Names)
}

func TestBuildEmbeddedRejectsInvalidBindings(t *testing.T) {
	candidate, err := Export(embeddedJoinQuery(false))
	require.NoError(t, err)
	tests := []struct {
		name     string
		bindings map[int32]EmbeddedReadBinding
		contains string
	}{
		{name: "count mismatch", bindings: map[int32]EmbeddedReadBinding{0: {BindingID: 1, Source: EmbeddedReadMO}}, contains: "binding count mismatch"},
		{name: "missing scan", bindings: map[int32]EmbeddedReadBinding{1: {BindingID: 2, Source: EmbeddedReadTAE}, 9: {BindingID: 1, Source: EmbeddedReadMO}}, contains: "missing embedded binding for node 0"},
		{name: "zero", bindings: map[int32]EmbeddedReadBinding{0: {Source: EmbeddedReadMO}, 1: {BindingID: 2, Source: EmbeddedReadTAE}}, contains: "is zero"},
		{name: "duplicate", bindings: map[int32]EmbeddedReadBinding{0: {BindingID: 1, Source: EmbeddedReadMO}, 1: {BindingID: 1, Source: EmbeddedReadTAE}}, contains: "duplicate embedded binding"},
		{name: "not deterministic", bindings: map[int32]EmbeddedReadBinding{0: {BindingID: 2, Source: EmbeddedReadMO}, 1: {BindingID: 1, Source: EmbeddedReadTAE}}, contains: "binding mismatch"},
		{name: "unsupported source", bindings: map[int32]EmbeddedReadBinding{0: {BindingID: 1, Source: 99}, 1: {BindingID: 2, Source: EmbeddedReadTAE}}, contains: "unsupported source"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := candidate.BuildEmbedded(test.bindings)
			require.ErrorContains(t, err, test.contains)
		})
	}
}

func TestEmbeddedReadsRejectReplayAndUnsupportedMOProjection(t *testing.T) {
	t.Run("replayed scan", func(t *testing.T) {
		candidate, err := Export(embeddedJoinQuery(true))
		require.NoError(t, err)
		_, err = candidate.EmbeddedMOReads()
		require.ErrorContains(t, err, "replayed")
		_, err = candidate.BuildEmbedded(map[int32]EmbeddedReadBinding{0: {BindingID: 1, Source: EmbeddedReadMO}})
		require.ErrorContains(t, err, "replayed")
	})

	t.Run("computed scan projection", func(t *testing.T) {
		query := embeddedProjectedScanQuery()
		query.Nodes[0].ProjectList = []*planpb.Expr{i64(1)}
		query.Headings = []string{"constant"}
		candidate, err := Export(query)
		require.NoError(t, err)
		_, err = candidate.EmbeddedMOReads()
		require.ErrorContains(t, err, "not a direct source column")
		_, err = candidate.BuildEmbedded(map[int32]EmbeddedReadBinding{0: {BindingID: 1, Source: EmbeddedReadMO}})
		require.ErrorContains(t, err, "not a direct source column")
		_, err = candidate.BuildEmbedded(map[int32]EmbeddedReadBinding{0: {BindingID: 1, Source: EmbeddedReadTAE}})
		require.NoError(t, err, "the same projection remains supported by direct TAE")
	})

	t.Run("projection type mismatch", func(t *testing.T) {
		query := embeddedProjectedScanQuery()
		query.Nodes[0].ProjectList[0].Typ = f64Type()
		candidate, err := Export(query)
		require.NoError(t, err)
		_, err = candidate.EmbeddedMOReads()
		require.ErrorContains(t, err, "type does not match source column")
		_, err = candidate.BuildEmbedded(map[int32]EmbeddedReadBinding{0: {BindingID: 1, Source: EmbeddedReadMO}})
		require.ErrorContains(t, err, "type does not match source column")
	})
}

func TestEmbeddedBuilderDoesNotChangeDirectBuild(t *testing.T) {
	candidate, err := Export(embeddedProjectedScanQuery())
	require.NoError(t, err)
	wire, err := candidate.Build(map[int32][]byte{0: {1, 2, 3}})
	require.NoError(t, err)

	input := embeddedPlan(t, wire).Relations[0].GetRoot().Input
	require.NotNil(t, input.GetFetch())
	read := input.GetFetch().Input.GetProject().Input.GetFilter().Input.GetRead()
	require.NotNil(t, read.GetExtensionTable())
	require.Equal(t, TaeReadTypeURL, read.GetExtensionTable().Detail.TypeUrl)
	require.Equal(t, []string{"a", "b"}, read.BaseSchema.Names)
}

func embeddedProjectedScanQuery() *planpb.Query {
	return &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Headings: []string{"second", "first"},
		Nodes: []*planpb.Node{{
			NodeId:   0,
			NodeType: planpb.Node_TABLE_SCAN,
			ObjRef: &planpb.ObjectRef{
				DbName: "object_db", SchemaName: "object_schema", ObjName: "object_table", Obj: 42,
			},
			TableDef: &planpb.TableDef{
				DbId: 7, TblId: 42, DbName: "table_db", Name: "table_name", TableType: "r",
				Cols: []*planpb.ColDef{
					{Name: "a", ColId: 11, Seqnum: 5, Typ: i64Type()},
					{Name: "b", ColId: 22, Seqnum: 6, Typ: planpb.Type{Id: i64Type().Id, NotNullable: true}},
					{Name: "hidden", ColId: 99, Seqnum: 7, Hidden: true, Typ: i64Type()},
				},
			},
			FilterList:  []*planpb.Expr{fn(">", boolType(), col(0), i64(7))},
			ProjectList: []*planpb.Expr{col(1), col(0)},
			Limit:       u64(5),
			Offset:      u64(1),
		}},
	}
}

func embeddedJoinQuery(replay bool) *planpb.Query {
	left := &planpb.Node{
		NodeId: 0, NodeType: planpb.Node_TABLE_SCAN,
		ObjRef: &planpb.ObjectRef{DbName: "db", ObjName: "left", Obj: 41},
		TableDef: &planpb.TableDef{DbId: 7, TblId: 41, DbName: "db", Name: "left", TableType: "r", Cols: []*planpb.ColDef{
			{Name: "left_value", ColId: 11, Seqnum: 1, Typ: i64Type()},
		}},
	}
	right := &planpb.Node{
		NodeId: 1, NodeType: planpb.Node_TABLE_SCAN,
		ObjRef: &planpb.ObjectRef{DbName: "db", ObjName: "right", Obj: 42},
		TableDef: &planpb.TableDef{DbId: 7, TblId: 42, DbName: "db", Name: "right", TableType: "r", Cols: []*planpb.ColDef{
			{Name: "right_value", ColId: 22, Seqnum: 2, Typ: i64Type()},
		}},
	}
	children := []int32{0, 1}
	nodes := []*planpb.Node{left, right}
	if replay {
		children[1] = 0
		nodes = nodes[:1]
	}
	nodes = append(nodes, &planpb.Node{NodeId: int32(len(nodes)), NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER, Children: children})
	return &planpb.Query{StmtType: planpb.Query_SELECT, Steps: []int32{int32(len(nodes) - 1)}, Headings: []string{"left", "right"}, Nodes: nodes}
}

func embeddedPlan(t *testing.T, wire []byte) *spb.Plan {
	t.Helper()
	plan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, plan))
	return plan
}
