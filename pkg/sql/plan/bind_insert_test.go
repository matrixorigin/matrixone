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

// TestGetIrregularIndexes verifies that only existing irregular (IVF / fulltext /
// hnsw) indexes are returned for synchronous maintenance, while regular indexes
// and irregular indexes whose hidden table has not been created are filtered out.
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
				// ivf index but hidden table not yet created -> filtered out
				{IndexName: "ivf_pending", IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), TableExist: false},
			},
		}

		got := getIrregularIndexes(tableDef)
		names := make([]string, 0, len(got))
		for _, idx := range got {
			names = append(names, idx.IndexName)
			assert.False(t, catalog.IsRegularIndexAlgo(idx.IndexAlgo))
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
