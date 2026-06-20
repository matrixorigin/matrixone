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

// TestGetIrregularIndexes verifies that only existing IVF / fulltext indexes are
// returned for synchronous maintenance. Regular indexes, indexes whose hidden
// table has not been created, and MASTER / HNSW indexes (which lack modern delete
// maintenance) are filtered out.
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
				// master index -> filtered out (legacy-only, no modern delete maintenance)
				{IndexName: "mst", IndexAlgo: catalog.MOIndexMasterAlgo.ToString(), TableExist: true},
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
		assert.ElementsMatch(t, []string{"ivf", "ft"}, names)
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

// TestHasLegacyOnlyIrregularIndex verifies that tables carrying a MASTER / HNSW
// index (no modern delete maintenance) are flagged for the legacy planner, while
// IVF / fulltext / regular-only tables are not.
func TestHasLegacyOnlyIrregularIndex(t *testing.T) {
	assert.False(t, hasLegacyOnlyIrregularIndex(nil))

	master := &plan.TableDef{Indexes: []*plan.IndexDef{
		{IndexName: "ivf", IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), TableExist: true},
		{IndexName: "mst", IndexAlgo: catalog.MOIndexMasterAlgo.ToString(), TableExist: true},
	}}
	assert.True(t, hasLegacyOnlyIrregularIndex(master))

	hnsw := &plan.TableDef{Indexes: []*plan.IndexDef{
		{IndexName: "hnsw", IndexAlgo: catalog.MoIndexHnswAlgo.ToString(), TableExist: true},
	}}
	assert.True(t, hasLegacyOnlyIrregularIndex(hnsw))

	// a master index whose hidden table is not yet created does not force fallback
	pending := &plan.TableDef{Indexes: []*plan.IndexDef{
		{IndexName: "mst", IndexAlgo: catalog.MOIndexMasterAlgo.ToString(), TableExist: false},
	}}
	assert.False(t, hasLegacyOnlyIrregularIndex(pending))

	modern := &plan.TableDef{Indexes: []*plan.IndexDef{
		{IndexName: "ivf", IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), TableExist: true},
		{IndexName: "ft", IndexAlgo: catalog.MOIndexFullTextAlgo.ToString(), TableExist: true},
		{IndexName: "uk", IndexAlgo: catalog.MoIndexDefaultAlgo.ToString(), TableExist: true},
	}}
	assert.False(t, hasLegacyOnlyIrregularIndex(modern))
}
