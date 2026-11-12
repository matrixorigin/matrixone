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

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/stretchr/testify/require"
)

// The scan a DEDUP join checks keys against is narrow, for the primary key and unique
// indexes of INSERT, UPDATE and REPLACE; every other scan, such as the source of an
// INSERT ... SELECT, is not.
func TestDedupTargetScanIsNarrow(t *testing.T) {
	for _, sql := range []string{
		"insert into constraint_test.emp values (1, 'a', 'b', 1, '2020-01-01', 1.0, 1.0, 1)",
		"insert into constraint_test.emp select * from constraint_test.emp",
		"update constraint_test.emp set empno = empno + 1",
		"update constraint_test.emp set ename = 'x'",
		"update ignore constraint_test.emp a join constraint_test.emp b on a.empno = b.empno set a.ename = 'x', b.job = 'y'",
		"replace into constraint_test.emp values (1, 'a', 'b', 1, '2020-01-01', 1.0, 1.0, 1)",
	} {
		t.Run(sql, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
			require.NoError(t, err)
			query := p.GetQuery()
			target := make(map[int32]bool)
			dedups := 0
			for _, n := range query.Nodes {
				if n.NodeType != plan.Node_JOIN || n.JoinType != plan.Node_DEDUP {
					continue
				}
				dedups++
				narrow := false
				for _, c := range n.Children {
					if query.Nodes[c].NodeType == plan.Node_TABLE_SCAN && query.Nodes[c].NarrowScan {
						target[c], narrow = true, true
					}
				}
				require.True(t, narrow, "DEDUP join on %s has a narrow target scan", n.DedupColName)
			}
			require.NotZero(t, dedups)
			for i, n := range query.Nodes {
				if n.NodeType == plan.Node_TABLE_SCAN && !target[int32(i)] {
					require.False(t, n.NarrowScan, "scan of %s outside a DEDUP join", n.TableDef.GetName())
				}
			}
		})
	}
}

// A scan reads column ranges when it leaves unread a payload column, a vector here, that
// holds at least half of the table's bytes by its stats; it reads whole objects when it
// reads that column, when the dominant column is not a payload type, for every visible
// column, and without the column sizes.
func TestScanColumnRangesSkipPayloadColumn(t *testing.T) {
	newOptimizer := func(sizes map[string]uint64) Optimizer {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		docs := mock.ctxt.tables["docs_ft"]
		stats := NewStatsInfo()
		stats.TableCnt = 1000
		stats.BlockNumber = 1
		stats.SizeMap = sizes
		cache := NewStatsCache()
		cache.Set(docs.TblId, stats)
		return &updateIndexBuildTestOptimizer{ctx: &updateIndexBuildTestContext{
			MockCompilerContext: &mock.ctxt,
			statsCache:          cache,
			statsByID:           map[uint64]*statspb.StatsInfo{docs.TblId: stats},
		}}
	}
	vectorDominant := map[string]uint64{"id": 1000, "body": 5000, "payload": 1000, "embedding": 90000}
	varcharDominant := map[string]uint64{"id": 1000, "body": 90000, "payload": 1000, "embedding": 5000}
	for _, tc := range []struct {
		name    string
		sizes   map[string]uint64
		sql     string
		columns bool
	}{
		{"skips the vector", vectorDominant, "select id, payload from constraint_test.docs_ft", true},
		{"skips the vector, filter column read", vectorDominant, "select id from constraint_test.docs_ft where payload > 1", true},
		{"reads the vector", vectorDominant, "select id, embedding from constraint_test.docs_ft", false},
		{"every visible column", vectorDominant, "select * from constraint_test.docs_ft", false},
		{"dominant column is varchar", varcharDominant, "select id, payload from constraint_test.docs_ft", false},
		{"no stats", nil, "select id, payload from constraint_test.docs_ft", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opt Optimizer = NewMockOptimizer(false, newPlanTestProcess(t))
			if tc.sizes != nil {
				opt = newOptimizer(tc.sizes)
			}
			p, err := runOneStmt(opt, t, tc.sql)
			require.NoError(t, err)
			scans := 0
			for _, n := range p.GetQuery().Nodes {
				if n.NodeType != plan.Node_TABLE_SCAN || n.TableDef.GetName() != "docs_ft" {
					continue
				}
				scans++
				got := fileservice.Policy(n.ReadPolicy)
				require.Equal(t, tc.columns, got.Any(fileservice.ColumnPrefetch), "policy %b", got)
				require.Equal(t, tc.columns, got.Any(fileservice.SkipFullFilePreloads), "policy %b", got)
			}
			require.NotZero(t, scans)
		})
	}
}
