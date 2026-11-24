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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestPointSampleRequiresDisjointIntervals(t *testing.T) {
	rare := pointSampleInterval(0, 2)
	rareSibling := pointSampleInterval(28, 2)
	broad := pointSampleInterval(182, 2)
	require.Greater(t, rare.upper, 0.0)
	require.Equal(t, 1, separatedPointSampleWinner([]indexPointInterval{broad, rare}))
	require.Equal(t, 0, separatedPointSampleWinner([]indexPointInterval{rare, broad}))
	require.Equal(t, -1, separatedPointSampleWinner([]indexPointInterval{rare, rareSibling}))
	require.Equal(t, -1, separatedPointSampleWinner([]indexPointInterval{broad, rare, rareSibling}))
}

type pointSampleSelectorTestContext struct {
	*encodedIndexCostTestContext
	matches [engine.MaxPointSampleCandidates]int
	keys    []engine.IndexPointKey
}

func (c *pointSampleSelectorTestContext) SamplePointIndexMatches(_ uint64, _ uint64, _ int, candidates [][]engine.IndexPointKey) ([engine.MaxPointSampleCandidates]int, bool) {
	for _, candidate := range candidates {
		c.keys = append(c.keys, candidate...)
	}
	return c.matches, true
}

func TestPointSampleSelectorPreservesBackfillKeyCapacity(t *testing.T) {
	const rows = 65_536
	for _, tc := range []struct {
		name          string
		sample        bool
		narrowMatches int
		want          string
	}{
		{name: "native control", want: "wide"},
		{name: "sampled lower bound exceeds capacity", sample: true, narrowMatches: 110, want: "wide"},
		{name: "only sampled upper bound exceeds capacity", sample: true, narrowMatches: 55, want: "narrow"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pk := catalog.CreateAlias("event_id")
			builder, scanID, wide, refs := newEncodedIndexCostTestCase(t,
				[]string{"tenant_id", "event_time", pk}, nil,
				&planpb.Stats{TableCnt: rows, Outcnt: 128, Selectivity: 128.0 / rows, Cost: rows},
				map[int32]int{0: 1, 1: 1, 2: 1, 3: 1}, false,
			)
			node := builder.qry.Nodes[scanID]
			node.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "event_id", Names: []string{"event_id"}}
			int32Type := planpb.Type{Id: int32(types.T_int32)}
			node.TableDef.Cols[2].Typ = int32Type
			node.TableDef.Cols[0].Seqnum = 1
			node.TableDef.Cols[2].Seqnum = 2
			for _, key := range []struct {
				column int32
				sel    float64
			}{{0, 0.005}, {2, 0.03}} {
				filter := makeRangeFilterExpr(node.BindingTags[0], key.column, "=", 7)
				filter.Selectivity = key.sel
				args := filter.GetF().Args
				args[0].Typ, args[1].Typ = int32Type, int32Type
				args[1].GetLit().Value = &planpb.Literal_I32Val{I32Val: 7}
				node.FilterList = append(node.FilterList, filter)
			}
			wide.IndexName = "wide"
			narrow := &planpb.IndexDef{
				IndexName: "narrow", IndexAlgo: wide.IndexAlgo, IndexTableName: "__mo_idx_point_narrow",
				Parts: []string{"category", pk}, TableExist: true,
			}
			node.TableDef.Indexes = []*planpb.IndexDef{wide, narrow}
			mock := builder.compCtx.(*encodedIndexCostTestContext)
			narrowTable := makeTestIndexTableDef()
			narrowTable.Name = narrow.IndexTableName
			mock.tables[narrow.IndexTableName] = narrowTable
			mock.objects[narrow.IndexTableName] = &planpb.ObjectRef{SchemaName: "test", ObjName: narrow.IndexTableName}

			sample := &pointSampleSelectorTestContext{
				encodedIndexCostTestContext: mock,
				matches:                     [engine.MaxPointSampleCandidates]int{170, tc.narrowMatches},
			}
			if tc.sample {
				builder.compCtx = sample
				limit := float64(GetInFilterCardLimitOnPK(sample.GetProcess().GetService(), rows))
				narrowInterval := pointSampleInterval(tc.narrowMatches, len(node.TableDef.Indexes))
				require.Greater(t, narrowInterval.upper*rows, limit)
				require.Equal(t, tc.want == "wide", narrowInterval.lower*rows > limit)
			}
			id, err := builder.applyIndicesForFiltersRegularIndex(scanID, node, refs, map[[2]int32]*planpb.Expr{})
			require.NoError(t, err)
			if tc.sample {
				require.Equal(t, []engine.IndexPointKey{{Seqnum: 1, Value: 7}, {Seqnum: 2, Value: 7}}, sample.keys)
			}
			requireIndexBackfill(t, builder, builder.qry.Nodes[id], tc.want)
		})
	}
}
