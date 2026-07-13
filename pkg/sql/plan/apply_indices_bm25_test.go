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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// makeBm25TestTableDef builds a table with an int64 pk, a text column, and a bm25
// index (its storage + metadata sibling defs on the text column).
func makeBm25TestTableDef(name string, withBm25 bool) *planpb.TableDef {
	td := &planpb.TableDef{
		Name: name,
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "body", Typ: planpb.Type{Id: int32(types.T_text)}},
		},
		Name2ColIndex: map[string]int32{"id": 0, "body": 1},
		Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
	}
	if withBm25 {
		td.Indexes = []*planpb.IndexDef{
			{
				IndexName:          "bm25idx",
				IndexAlgo:          catalog.MoIndexBm25Algo.ToString(),
				IndexAlgoTableType: catalog.Bm25Index_TblType_Storage,
				IndexTableName:     "__mo_bm25_store_" + name,
				Parts:              []string{"body"},
				TableExist:         true,
			},
			{
				IndexName:          "bm25idx",
				IndexAlgo:          catalog.MoIndexBm25Algo.ToString(),
				IndexAlgoTableType: catalog.Bm25Index_TblType_Metadata,
				IndexTableName:     "__mo_bm25_meta_" + name,
				Parts:              []string{"body"},
				TableExist:         true,
			},
		}
	}
	return td
}

// makeBm25MatchExpr builds a bound bm25_match(pattern, mode=0, cols...) function expr
// (the form BM25(col) AGAINST('pattern') binds to).
func makeBm25MatchExpr(pattern string, tableDef *planpb.TableDef, tag int32, colPositions []int32) *planpb.Expr {
	args := []*planpb.Expr{
		makePlan2StringConstExprWithType(pattern, false),
		makePlan2Int64ConstExprWithType(0),
	}
	for _, pos := range colPositions {
		args = append(args, ftjColExpr(tableDef, tag, pos))
	}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_float32)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "bm25_match"},
			Args: args,
		}},
	}
}

func TestFindMatchBm25Index(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	td := makeBm25TestTableDef("t", true)
	tag := builder.genNewBindTag()
	scan := makeFullTextJoinTestScan(td, tag, nil)

	// matched -> the storage index def
	idx := builder.findMatchBm25Index(makeBm25MatchExpr("apple", td, tag, []int32{1}).GetF(), scan)
	require.NotNil(t, idx)
	require.Equal(t, catalog.Bm25Index_TblType_Storage, idx.IndexAlgoTableType)

	// no bm25 index on the table -> nil
	tdNo := makeBm25TestTableDef("t", false)
	scanNo := makeFullTextJoinTestScan(tdNo, tag, nil)
	require.Nil(t, builder.findMatchBm25Index(makeBm25MatchExpr("apple", tdNo, tag, []int32{1}).GetF(), scanNo))

	// column referencing a different binding tag (cross-table) -> nil
	crossTag := builder.genNewBindTag()
	require.Nil(t, builder.findMatchBm25Index(makeBm25MatchExpr("apple", td, crossTag, []int32{1}).GetF(), scan))

	// dynamic (non-literal) pattern -> nil
	dyn := makeBm25MatchExpr("apple", td, tag, []int32{1})
	textTyp := types.T_text.ToType()
	dyn.GetF().Args[0] = &planpb.Expr{
		Typ:  makePlan2Type(&textTyp),
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	require.Nil(t, builder.findMatchBm25Index(dyn.GetF(), scan))

	// too few args -> nil
	short := makeBm25MatchExpr("apple", td, tag, nil)
	require.Nil(t, builder.findMatchBm25Index(short.GetF(), scan))
}

func TestFindBm25IndexTables(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	td := makeBm25TestTableDef("t", true)
	tag := builder.genNewBindTag()
	scan := makeFullTextJoinTestScan(td, tag, nil)
	storeDef := td.Indexes[0]

	store, meta, ok := builder.findBm25IndexTables(scan, storeDef)
	require.True(t, ok)
	require.Equal(t, "__mo_bm25_store_t", store)
	require.Equal(t, "__mo_bm25_meta_t", meta)

	// drop the metadata sibling -> not ok
	tdPartial := makeBm25TestTableDef("t", true)
	tdPartial.Indexes = tdPartial.Indexes[:1] // storage only
	scanPartial := makeFullTextJoinTestScan(tdPartial, tag, nil)
	_, _, ok = builder.findBm25IndexTables(scanPartial, tdPartial.Indexes[0])
	require.False(t, ok)

	// nil inputs
	_, _, ok = builder.findBm25IndexTables(nil, storeDef)
	require.False(t, ok)
}

func TestBuildBm25SearchTableFunc(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	td := makeBm25TestTableDef("t", true)
	tag := builder.genNewBindTag()
	scan := makeFullTextJoinTestScan(td, tag, nil)
	storeDef := td.Indexes[0]

	tf, err := builder.buildBm25SearchTableFunc(scan, storeDef, "apple", "mo_bm25_alias_0")
	require.NoError(t, err)
	require.NotNil(t, tf)
	require.Equal(t, "mo_bm25_alias_0", string(tf.As.Alias))

	// missing sibling tables -> error
	tdPartial := makeBm25TestTableDef("t", true)
	tdPartial.Indexes = tdPartial.Indexes[:1]
	scanPartial := makeFullTextJoinTestScan(tdPartial, tag, nil)
	_, err = builder.buildBm25SearchTableFunc(scanPartial, tdPartial.Indexes[0], "apple", "a")
	require.Error(t, err)
}

// --- apply_indices_match.go shared helpers ---

func TestEqualsMatchFunc(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	td := makeBm25TestTableDef("t", true)
	tag := builder.genNewBindTag()

	a := makeBm25MatchExpr("apple", td, tag, []int32{1}).GetF()
	b := makeBm25MatchExpr("apple", td, tag, []int32{1}).GetF()
	require.True(t, builder.equalsMatchFunc(a, b))

	// different pattern
	c := makeBm25MatchExpr("banana", td, tag, []int32{1}).GetF()
	require.False(t, builder.equalsMatchFunc(a, c))

	// different function name (bm25_match vs fulltext_match) must NOT be equal
	ft := makeFullTextMatchExpr("apple", 0, td, tag, []int32{1}).GetF()
	require.False(t, builder.equalsMatchFunc(a, ft))

	// different arg count
	d := makeBm25MatchExpr("apple", td, tag, nil).GetF()
	require.False(t, builder.equalsMatchFunc(a, d))
}

func TestFindEqualMatchFunc(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	td := makeBm25TestTableDef("t", true)
	tag := builder.genNewBindTag()

	projNode := &planpb.Node{ProjectList: []*planpb.Expr{makeBm25MatchExpr("apple", td, tag, []int32{1})}}
	scanNode := &planpb.Node{FilterList: []*planpb.Expr{makeBm25MatchExpr("apple", td, tag, []int32{1})}}

	eqmap := builder.findEqualMatchFunc(projNode, scanNode, []int32{0}, []int32{0})
	require.Equal(t, map[int32]int32{0: 0}, eqmap)

	// non-equal (different pattern) -> empty map
	scanNode2 := &planpb.Node{FilterList: []*planpb.Expr{makeBm25MatchExpr("banana", td, tag, []int32{1})}}
	require.Empty(t, builder.findEqualMatchFunc(projNode, scanNode2, []int32{0}, []int32{0}))
}

func TestMatchRewriteContextNodeID(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(builder, nil)
	td := makeBm25TestTableDef("t", true)
	tag := builder.genNewBindTag()
	scanID := builder.appendNode(makeFullTextJoinTestScan(td, tag, nil), ctx)
	scan := builder.qry.Nodes[scanID]

	// preferred id valid -> returned as-is
	require.Equal(t, scanID, builder.matchRewriteContextNodeID(scanID, scan))
	// preferred id invalid (-1) -> falls back to the scan node's id
	require.Equal(t, scanID, builder.matchRewriteContextNodeID(-1, scan))
}
