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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type temporalIndexResolveSpy struct {
	*MockCompilerContext
	calls int
}

func (s *temporalIndexResolveSpy) ResolveIndexTableByRef(*ObjectRef, string, *Snapshot) (*ObjectRef, *TableDef, error) {
	s.calls++
	return nil, nil, errors.New("unexpected catalog lookup")
}

func TestTemporalIndexOnlyKeepsOriginalResidual(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	mock := NewMockCompilerContext(true, proc)
	mock.SetContext(proc.Ctx)
	builder := NewQueryBuilder(plan.Query_SELECT, mock, false, true)
	baseTag := builder.genNewBindTag()
	datetime := Type{Id: int32(types.T_datetime), Scale: 6}
	integer := Type{Id: int32(types.T_int64), NotNullable: true}
	col := GetColExpr(datetime, baseTag, 0)
	col.GetCol().Name = "d"
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	predicate, err := BindFuncExprImplByPlanExpr(proc.Ctx, "<", []*Expr{col, now})
	require.NoError(t, err)
	base := &plan.TableDef{Name: "base", Name2ColIndex: map[string]int32{"d": 0, "id": 1},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
		Cols: []*plan.ColDef{{Name: "d", Typ: datetime}, {Name: "id", Typ: integer}}}
	scan := &plan.Node{NodeType: plan.Node_TABLE_SCAN, NodeId: 0, BindingTags: []int32{baseTag},
		ObjRef: &plan.ObjectRef{SchemaName: "tpch", ObjName: "base"}, TableDef: base, FilterList: []*Expr{predicate},
		Stats: &plan.Stats{TableCnt: 1000, Outcnt: 1000, Selectivity: 1, Cost: 1000, BlockNum: 1}}
	builder.qry.Nodes = []*plan.Node{scan}
	builder.qry.Steps = []int32{0}
	builder.ctxByNode = []*BindContext{nil}
	index := &plan.IndexDef{IndexName: "idx_d", IndexTableName: "temporal_idx", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(),
		TableExist: true, Parts: []string{"d", "id"}}
	keyType := Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	indexTable := &plan.TableDef{Name: "temporal_idx", TableType: catalog.SystemIndexRel,
		Pkey:          &plan.PrimaryKeyDef{PkeyColName: "key", Names: []string{"key"}},
		Name2ColIndex: map[string]int32{"key": 0, "id": 1},
		Cols:          []*plan.ColDef{{Name: "key", Typ: keyType, Primary: true}, {Name: "id", Typ: integer}}}
	mock.tables[indexTable.Name] = indexTable
	mock.objects[indexTable.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: indexTable.Name}
	cost := &encodedRegularIndexCostContext{node: scan, coveredCols: make([]bool, len(base.Cols)), requiredCols: []int32{0}}
	mapping := make(map[[2]int32]*plan.Expr)
	id, ok, err := builder.applyTemporalIndexOnlyScan(0, index, true, nil, 0, mapping, cost)
	require.NoError(t, err)
	require.True(t, ok)
	candidateScan := builder.qry.Nodes[id]
	require.Len(t, candidateScan.FilterList, 2)
	require.True(t, candidateScan.FilterList[0].TemporalCandidateFilter)
	require.NotSame(t, candidateScan.FilterList[0], candidateScan.BlockFilterList[0])
	require.Equal(t, "<", candidateScan.FilterList[1].GetF().Func.ObjName)
	require.Equal(t, "serial_extract", candidateScan.FilterList[1].GetF().Args[0].GetF().Func.ObjName)
	require.False(t, function.ContainsTemporalBound(candidateScan.FilterList[1]))
	require.Len(t, candidateScan.BlockFilterList, 1)
	require.True(t, candidateScan.BlockFilterList[0].TemporalCandidateFilter)
	require.True(t, function.ContainsTemporalBound(candidateScan.BlockFilterList[0]), "stats recalculation must preserve the hint")
	require.NotNil(t, mapping[[2]int32{baseTag, 0}])
	require.Same(t, predicate, scan.FilterList[0], "the original semantic owner remains untouched")

	uncheckedID, applied, err := builder.applyTemporalIndexOnlyScanResolved(0, index, true, nil, 0, make(map[[2]int32]*plan.Expr), cost,
		&temporalResolvedIndex{ref: mock.objects[indexTable.Name], table: indexTable})
	require.NoError(t, err)
	require.True(t, applied)
	require.Len(t, builder.qry.Nodes[uncheckedID].FilterList, 2, "metadata without a verifier grants no storage-only permission")
	require.True(t, builder.qry.Nodes[uncheckedID].FilterList[0].TemporalCandidateFilter)

	joinID, ok, err := builder.applyTemporalIndexBackfill(0, index, true, nil, 0)
	require.NoError(t, err)
	require.True(t, ok)
	join := builder.qry.Nodes[joinID]
	require.Equal(t, plan.Node_INDEX, join.JoinType)
	require.Equal(t, int32(0), join.Children[0])
	require.Same(t, predicate, scan.FilterList[0], "backfill must retain the original base residual")
	lookup := builder.qry.Nodes[join.Children[1]]
	require.Len(t, lookup.FilterList, 1)
	require.True(t, lookup.FilterList[0].TemporalCandidateFilter)
	require.NotSame(t, lookup.FilterList[0], lookup.BlockFilterList[0])
	require.Len(t, lookup.BlockFilterList, 1)
	require.True(t, lookup.BlockFilterList[0].TemporalCandidateFilter)
	require.Len(t, join.OnList, 1)
	require.Equal(t, "=", join.OnList[0].GetF().Func.ObjName)

	spy := &temporalIndexResolveSpy{MockCompilerContext: mock}
	builder.compCtx = spy
	_, ok, err = builder.applyTemporalIndexBackfill(0, index, false, nil, 0)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, spy.calls, "unproved candidates must not touch optional index metadata")

	// The join needs only the physical PK, while the row candidate needs the
	// encoded key. Real remapping must read both but project only the PK.
	tag := lookup.BindingTags[0]
	_, err = builder.remapAllColRefs(join.Children[1], 0,
		map[[2]int32]int{{tag, 1}: 1}, map[[2]int32]bool{}, map[[2]int32]int{})
	require.NoError(t, err)
	require.Len(t, lookup.TableDef.Cols, 2)
	require.Equal(t, "key", lookup.TableDef.Cols[0].Name)
	require.Len(t, lookup.ProjectList, 1)
	require.Equal(t, int32(1), lookup.ProjectList[0].GetCol().ColPos)
}
