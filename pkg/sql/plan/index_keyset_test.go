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
	"fmt"
	"strings"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func compositeKeySetScan(p *Plan) *planpb.Node {
	if p == nil || p.GetQuery() == nil {
		return nil
	}
	for _, node := range p.GetQuery().Nodes {
		if !node.IndexScanInfo.IsIndexScan || len(node.FilterList) == 0 {
			continue
		}
		fn := node.FilterList[0].GetF()
		if fn == nil || fn.Func.ObjName != "prefix_in" || len(fn.Args) != 2 {
			continue
		}
		if list := fn.Args[1].GetList(); list != nil && len(list.List) > 0 {
			return node
		}
		serial := fn.Args[1].GetF()
		if serial != nil && len(serial.Args) > 1 {
			return node
		}
	}
	return nil
}

func addCompositeKeySetPayloadColumn(mock *MockOptimizer) {
	table := mock.ctxt.tables["index_hint_t"]
	payload := *table.Cols[1]
	payload.ColId = table.Cols[len(table.Cols)-1].ColId + 1
	payload.Name = "payload"
	payload.OriginName = "payload"
	table.Name2ColIndex[payload.Name] = int32(len(table.Cols))
	table.Cols = append(table.Cols, &payload)
}

func TestCompositeIndexKeySetAccess(t *testing.T) {
	for _, tc := range []struct {
		name      string
		predicate string
		index     string
		points    int
	}{
		{name: "unique trailing IN", predicate: "a = 1 and b in (2, 3)", index: "uk_ab", points: 2},
		{name: "unique leading IN and equality suffix", predicate: "a in (1, 2) and b = 3", index: "uk_ab", points: 2},
		{name: "nonunique full user key", predicate: "a = 1 and b in (2, 3)", index: "idx_ab", points: 2},
		{name: "duplicate IN values", predicate: "a = 1 and b in (2, 2, 3)", index: "uk_ab", points: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			addCompositeKeySetPayloadColumn(mock)
			p, err := runOneStmt(mock, t, fmt.Sprintf(
				"select * from index_hint_t force index(%s) where %s", tc.index, tc.predicate,
			))
			require.NoError(t, err)
			scan := compositeKeySetScan(p)
			require.NotNil(t, scan)
			require.Equal(t, tc.index, scan.IndexScanInfo.IndexName)
			require.True(t, planHasIndexJoin(p))
			tuples := scan.FilterList[0].GetF().Args[1].GetList()
			require.NotNil(t, tuples)
			require.Len(t, tuples.List, tc.points)
			for _, tuple := range tuples.List {
				require.NotNil(t, tuple.GetVec())
				require.NotEmpty(t, tuple.GetVec().GetData())
			}
		})
	}
}

func TestCompositeIndexKeySetFallbacks(t *testing.T) {
	values := make([]string, maxCompositePoints+1)
	for i := range values {
		values[i] = fmt.Sprint(i + 1)
	}
	for _, tc := range []struct {
		name      string
		predicate string
	}{
		{name: "multiple set dimensions are not zipped", predicate: "a in (1, 2) and b in (3, 4)"},
		{name: "NULL IN value", predicate: "a = 1 and b in (2, null)"},
		{name: "oversized IN list", predicate: "a = 1 and b in (" + strings.Join(values, ",") + ")"},
		{name: "incomplete unique key", predicate: "b in (2, 3)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			addIndexHintChoiceTableForTest(mock)
			p, err := runOneStmt(mock, t,
				"select * from index_hint_t force index(uk_ab) where "+tc.predicate,
			)
			require.NoError(t, err)
			require.Nil(t, compositeKeySetScan(p))
		})
	}
}

func TestCompositeIndexKeySetKeepsEqualityLowering(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	p, err := runOneStmt(mock, t,
		"select * from index_hint_t force index(uk_ab) where a = 1 and b = 2",
	)
	require.NoError(t, err)
	scan := findFirstIndexScanNode(p)
	require.NotNil(t, scan)
	require.Equal(t, "uk_ab", scan.IndexScanInfo.IndexName)
	require.Nil(t, compositeKeySetScan(p))
	require.Equal(t, "=", scan.FilterList[0].GetF().Func.ObjName)
}
