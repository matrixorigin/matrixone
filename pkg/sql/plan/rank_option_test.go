// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestRankOptionSQLPropagation(t *testing.T) {
	for _, sql := range []string{
		"select 1 limit 1 by rank with option 'nprobe=3'",
		"select 1 union all select 2 limit 1 by rank with option 'nprobe=3'",
	} {
		t.Run(sql, func(t *testing.T) {
			p, err := buildMySQLDMLCompatibilityPlan(t, sql)
			require.NoError(t, err)
			var found bool
			for _, node := range p.GetQuery().Nodes {
				if node.RankOption != nil {
					found = true
					require.Equal(t, int64(3), node.RankOption.Nprobe)
					require.Empty(t, node.RankOption.Mode)
				}
			}
			require.True(t, found, "mode-less rank option lost during planning")
		})
	}
	option := &plan.RankOption{Mode: "post", Nprobe: 3}
	copy := DeepCopyRankOption(option)
	require.Equal(t, option, copy)
	copy.Nprobe = 4
	require.Equal(t, int64(3), option.Nprobe)
}

func TestResolveVectorNprobe(t *testing.T) {
	for _, fallback := range []int64{5, 20} {
		ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
		builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, true)
		var value interface{}
		var resolveErr error
		ctx.ResolveVariableFunc = func(name string, _, _ bool) (interface{}, error) {
			require.Equal(t, "probe_limit", name)
			return value, resolveErr
		}
		got, err := builder.resolveVectorNprobe(nil, fallback)
		require.NoError(t, err)
		require.Equal(t, fallback, got)
		value = int64(7)
		got, err = builder.resolveVectorNprobe(&plan.RankOption{Mode: "post"}, fallback)
		require.NoError(t, err)
		require.Equal(t, int64(7), got)
		value = "7"
		_, err = builder.resolveVectorNprobe(nil, fallback)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
		resolveErr = moerr.NewInternalError(context.Background(), "session lookup failed")
		_, err = builder.resolveVectorNprobe(nil, fallback)
		require.ErrorIs(t, err, resolveErr)
		ctx.ResolveVariableFunc = func(string, bool, bool) (interface{}, error) {
			t.Fatal("explicit nprobe must bypass session resolution")
			return nil, resolveErr
		}
		got, err = builder.resolveVectorNprobe(&plan.RankOption{Nprobe: 3}, fallback)
		require.NoError(t, err)
		require.Equal(t, int64(3), got)
	}
}

func TestIvfflatQueryNprobeConsumer(t *testing.T) {
	for _, mode := range []string{"post", "", "force"} {
		t.Run(mode, func(t *testing.T) {
			builder, _, scan, scanID, index := newIvfIncludeModeTestBuilder(t)
			v := newIvfIncludeModeVectorSortContext(scan, scanID, mode, 0)
			v.rankOption.Nprobe = 3 // The fixture session probe_limit is 10.
			_, err := builder.applyIndicesForSortUsingIvfflat(scanID, v, index, nil, nil)
			require.NoError(t, err)
			var scans []*plan.Node
			for _, node := range builder.qry.Nodes {
				if node.NodeType == plan.Node_INDEX_SEARCH_SCAN && node.IndexSearchScan != nil &&
					node.IndexSearchScan.Index != nil && node.IndexSearchScan.Index.IndexAlgo == catalog.MoIndexIvfFlatAlgo.ToString() {
					scans = append(scans, node)
				}
			}
			if mode == "force" {
				require.Empty(t, scans)
			} else {
				require.NotEmpty(t, scans)
				for _, node := range scans {
					require.Equal(t, uint32(3), testIvfScanOptions(t, node.IndexSearchScan).InitialProbeCount)
				}
			}
		})
	}
}
