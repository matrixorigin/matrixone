// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/assert"
)

func TestGetValidIndexesKeepsRegularIndexThatCoversPrimaryKey(t *testing.T) {
	tableDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"user_id", "session_id", "id"},
			PkeyColName: catalog.CPrimaryKeyColName,
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:  "idx_ret",
				IndexAlgo:  catalog.MoIndexDefaultAlgo.ToString(),
				TableExist: true,
				Unique:     false,
				Parts: []string{
					"status",
					"due",
					"user_id",
					"session_id",
					"id",
					catalog.CreateAlias(catalog.CPrimaryKeyColName),
				},
			},
			{
				IndexName:  "uq_token_pk",
				IndexAlgo:  catalog.MoIndexDefaultAlgo.ToString(),
				TableExist: true,
				Unique:     true,
				Parts: []string{
					"token",
					"user_id",
					"session_id",
					"id",
				},
			},
			{
				IndexName:  "idx_pending",
				IndexAlgo:  catalog.MoIndexDefaultAlgo.ToString(),
				TableExist: false,
				Parts:      []string{"status"},
			},
		},
	}

	got, hasIrregular := getValidIndexes(tableDef)

	assert.False(t, hasIrregular)
	if assert.Len(t, got, 2) {
		assert.Equal(t, "idx_ret", got[0].IndexName)
		assert.Equal(t, "uq_token_pk", got[1].IndexName)
	}
}

// TestGetIrregularIndexes verifies that existing IVF / fulltext / master indexes
// (synchronously maintained) are returned, while regular indexes, indexes whose
// hidden table has not been created, and HNSW/CAGRA/IVF-PQ indexes (cron-maintained)
// are filtered out.
func TestGetIrregularIndexes(t *testing.T) {
	t.Run("nil/empty table", func(t *testing.T) {
		assert.Nil(t, getIrregularIndexes(nil))
		assert.Nil(t, getIrregularIndexes(&plan.TableDef{}))
	})

	t.Run("mixed indexes", func(t *testing.T) {
		tableDef := &plan.TableDef{
			Indexes: []*plan.IndexDef{
				// regular unique index -> filtered out
				{IndexName: "uk", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true},
				// regular btree index -> filtered out
				{IndexName: "bt", IndexAlgo: catalog.MoIndexBTreeAlgo.ToString(), TableExist: true},
				// ivf index, table exists -> kept
				{IndexName: "ivf", IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), TableExist: true},
				// fulltext index, table exists -> kept
				{IndexName: "ft", IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(), TableExist: true},
				// master index -> kept (full synchronous modern maintenance)
				{IndexName: "mst", IndexAlgo: catalog.MOIndexMasterAlgo.ToString(), TableExist: true},
				// hnsw index -> filtered out (cron-maintained, no inline sub-plan)
				{IndexName: "hnsw", IndexAlgo: catalog.MoIndexHnswAlgo.ToString(), TableExist: true},
				// ivf index but hidden table not yet created -> filtered out
				{IndexName: "ivf_pending", IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), TableExist: false},
			},
		}

		got := getIrregularIndexes(tableDef)
		names := make([]string, 0, len(got))
		for _, idx := range got {
			names = append(names, idx.IndexName)
			assert.True(t, isModernMaintainedIrregularAlgo(idx.IndexAlgo))
			assert.True(t, idx.TableExist)
		}
		assert.ElementsMatch(t, []string{"ivf", "ft", "mst"}, names)
	})

	t.Run("only regular indexes returns nil", func(t *testing.T) {
		tableDef := &plan.TableDef{
			Indexes: []*plan.IndexDef{
				{IndexName: "uk", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true},
			},
		}
		assert.Nil(t, getIrregularIndexes(tableDef))
	})
}
