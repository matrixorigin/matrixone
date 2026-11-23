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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type pointSampleTestContext struct {
	CompilerContext
	sample func(uint64, uint64, int, [][]engine.IndexPointKey) (engine.IndexPointSample, bool)
}

func (c pointSampleTestContext) SamplePointIndexMatches(db, table uint64, rows int, keys [][]engine.IndexPointKey) (engine.IndexPointSample, bool) {
	return c.sample(db, table, rows, keys)
}

func TestObservePointIndexMatchesRejectsUnsafeComparisons(t *testing.T) {
	const dbID, tableID, tag = 1200, 1201, 2
	const rows = 2 * objectio.BlockMaxRows
	intType := pb.Type{Id: int32(types.T_int32)}
	keys := []engine.IndexPointKey{{Seqnum: 3, Value: 5}, {Seqnum: 7, Value: 6}, {Seqnum: 9, Value: 7}}
	columns := []string{"a", "b", "c"}
	node := &pb.Node{
		BindingTags: []int32{tag},
		ObjRef:      &pb.ObjectRef{Obj: tableID},
		Stats:       &pb.Stats{TableCnt: rows},
		TableDef:    &pb.TableDef{DbId: dbID, TblId: tableID, Pkey: &pb.PrimaryKeyDef{PkeyColName: "id"}},
	}
	for i, key := range keys {
		node.TableDef.Cols = append(node.TableDef.Cols, &pb.ColDef{Name: columns[i], Seqnum: uint32(key.Seqnum), Typ: intType})
		node.FilterList = append(node.FilterList, &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{
			Func: &pb.ObjectRef{ObjName: "="},
			Args: []*pb.Expr{
				GetColExpr(intType, tag, int32(i)),
				{Typ: intType, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_I32Val{I32Val: key.Value}}}},
			},
		}}})
	}
	indexes := []*IndexDef{{Parts: []string{"a", "b", "id"}}, {Parts: []string{"c", "id"}}}
	group := []indexPointAccess{{index: 0, filters: []int32{0, 1}}, {index: 1, filters: []int32{2}}}
	b := &QueryBuilder{qry: &pb.Query{StmtType: pb.Query_SELECT}}
	_, ok := b.observePointIndexMatches(node, indexes, group)
	require.False(t, ok, "compiler context without a provider")

	sample := engine.IndexPointSample{Observed: 200, Matches: [engine.MaxPointSampleCandidates]int{150, 0}}
	available, calls := true, 0
	b.compCtx = pointSampleTestContext{sample: func(db, table uint64, n int, got [][]engine.IndexPointKey) (engine.IndexPointSample, bool) {
		calls++
		require.Equal(t, uint64(dbID), db)
		require.Equal(t, uint64(tableID), table)
		require.Equal(t, rows, n)
		require.Equal(t, [][]engine.IndexPointKey{keys[:2], keys[2:]}, got)
		return sample, available
	}}

	intervals, ok := b.observePointIndexMatches(node, indexes, group)
	require.True(t, ok)
	require.Equal(t, 1, separatedPointSampleWinner(intervals[:2]))
	// The same observed skew is no longer enough if the unseen rows could
	// bring the minority path into the majority path's interval.
	sample.Observed = 150
	sample.Matches[0] = 112
	intervals, ok = b.observePointIndexMatches(node, indexes, group)
	require.True(t, ok)
	require.Equal(t, -1, separatedPointSampleWinner(intervals[:2]))
	sample.Matches[0] = sample.Observed + 1
	_, ok = b.observePointIndexMatches(node, indexes, group)
	require.False(t, ok, "counts beyond the observed sample")
	sample.Observed = 0
	_, ok = b.observePointIndexMatches(node, indexes, group)
	require.False(t, ok, "cold cache")
	available = false
	_, ok = b.observePointIndexMatches(node, indexes, group)
	require.False(t, ok, "unavailable observation")
	require.Equal(t, 5, calls)

	node.TableDef.Pkey.PkeyColName = "c"
	_, ok = b.observePointIndexMatches(node, indexes, group)
	require.False(t, ok, "a residual primary-key filter changes the backfill work")
	node.TableDef.Pkey.PkeyColName = "id"
	indexes[0].Parts = []string{"a", "b", "c", "id"}
	_, ok = b.observePointIndexMatches(node, indexes, group)
	require.False(t, ok, "a residual filter stored in one candidate changes the backfill work")
	require.Equal(t, 5, calls, "declined comparisons must never request a sample")
}

func TestPointSampleRequiresDisjointIntervals(t *testing.T) {
	rare := pointSampleInterval(0, 0, 2)
	rareSibling := pointSampleInterval(28, 0, 2)
	broad := pointSampleInterval(182, 0, 2)
	require.Greater(t, rare.upper, 0.0)
	require.Equal(t, 1, separatedPointSampleWinner([]indexPointInterval{broad, rare}))
	require.Equal(t, 0, separatedPointSampleWinner([]indexPointInterval{rare, broad}))
	require.Equal(t, -1, separatedPointSampleWinner([]indexPointInterval{rare, rareSibling}))
	require.Equal(t, -1, separatedPointSampleWinner([]indexPointInterval{broad, rare, rareSibling}))
}
