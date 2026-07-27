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

package compile

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// stubIndexSearch is a VectorIndexSearchIf that only records destruction — the
// cache calls Destroy() on eviction, which is the signal these tests assert on.
type stubIndexSearch struct {
	destroyed bool
}

func (s *stubIndexSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return nil, nil, nil
}

func (s *stubIndexSearch) SearchFloat32(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, []int64, []float32) error {
	return nil
}
func (s *stubIndexSearch) Load(*sqlexec.SqlProcess) error                  { return nil }
func (s *stubIndexSearch) UpdateConfig(veccache.VectorIndexSearchIf) error { return nil }
func (s *stubIndexSearch) Destroy()                                        { s.destroyed = true }

// seedCachedIndex puts an entry in the vector-index cache under key, as a
// completed search would, and returns the stub so the test can see Destroy().
// The Cond is built exactly as VectorIndexCache.Search builds it — Destroy()
// broadcasts on it.
func seedCachedIndex(t *testing.T, key string) *stubIndexSearch {
	t.Helper()
	algo := &stubIndexSearch{}
	entry := &veccache.VectorIndexSearch{Algo: algo}
	entry.Cond = sync.NewCond(entry.Mutex.RLocker())
	veccache.Cache.IndexMap.Store(key, entry)
	t.Cleanup(func() { veccache.Cache.IndexMap.Delete(key) })
	return algo
}

func cachedIndexPresent(key string) bool {
	_, ok := veccache.Cache.IndexMap.Load(key)
	return ok
}

// ivfflatIndexDefs builds the three hidden-table defs an IVF-FLAT index carries.
func ivfflatIndexDefs(indexName, tablePrefix string) []*plan.IndexDef {
	return []*plan.IndexDef{
		{
			IndexName:          indexName,
			IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
			IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Metadata,
			IndexTableName:     tablePrefix + "_meta",
			TableExist:         true,
		},
		{
			IndexName:          indexName,
			IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
			IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Centroids,
			IndexTableName:     tablePrefix + "_centroids",
			TableExist:         true,
		},
		{
			IndexName:          indexName,
			IndexAlgo:          catalog.MoIndexIvfFlatAlgo.ToString(),
			IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
			IndexTableName:     tablePrefix + "_entries",
			TableExist:         true,
		},
	}
}

// DROP TABLE must evict the cached search index of EVERY plugin index on the
// table — and for IVF-FLAT that means every cached generation, since the search
// key is "<centroidsTable>:<version>" and only the DDL-side prefix eviction can
// name a rebuilt index's key.
func TestDispatchPluginDropIndexesDropTableEvictsAllIndexes(t *testing.T) {
	tableDef := &plan.TableDef{TblId: 1000}
	tableDef.Indexes = append(tableDef.Indexes, ivfflatIndexDefs("idx1", "__mo_idx1")...)
	tableDef.Indexes = append(tableDef.Indexes, ivfflatIndexDefs("idx2", "__mo_idx2")...)
	// a unique (non-plugin) index on the same table must be ignored
	tableDef.Indexes = append(tableDef.Indexes, &plan.IndexDef{
		IndexName: "uk", Unique: true, TableExist: true,
	})

	v0 := seedCachedIndex(t, "__mo_idx1_centroids:0")
	v3 := seedCachedIndex(t, "__mo_idx1_centroids:3")        // rebuilt generation
	split := seedCachedIndex(t, "__mo_idx1_centroids:3/1/2") // split-CN read
	second := seedCachedIndex(t, "__mo_idx2_centroids:1")    // sibling index
	other := seedCachedIndex(t, "__mo_other_centroids:0")    // different table

	dispatchPluginDropIndexes(nil, &Compile{}, nil, "db", "tbl", tableDef, "")

	require.False(t, cachedIndexPresent("__mo_idx1_centroids:0"))
	require.False(t, cachedIndexPresent("__mo_idx1_centroids:3"))
	require.False(t, cachedIndexPresent("__mo_idx1_centroids:3/1/2"))
	// the sibling index of the same algo must not be lost to map collision
	require.False(t, cachedIndexPresent("__mo_idx2_centroids:1"))
	require.True(t, cachedIndexPresent("__mo_other_centroids:0"))

	require.True(t, v0.destroyed)
	require.True(t, v3.destroyed)
	require.True(t, split.destroyed)
	require.True(t, second.destroyed)
	require.False(t, other.destroyed)
}

// DROP INDEX names one index: its siblings on the same table stay cached.
func TestDispatchPluginDropIndexesDropIndexEvictsOnlyNamed(t *testing.T) {
	tableDef := &plan.TableDef{TblId: 1001}
	tableDef.Indexes = append(tableDef.Indexes, ivfflatIndexDefs("idx1", "__mo_one")...)
	tableDef.Indexes = append(tableDef.Indexes, ivfflatIndexDefs("idx2", "__mo_two")...)

	dropped := seedCachedIndex(t, "__mo_one_centroids:2")
	kept := seedCachedIndex(t, "__mo_two_centroids:2")

	dispatchPluginDropIndexes(nil, &Compile{}, nil, "db", "tbl", tableDef, "idx1")

	require.False(t, cachedIndexPresent("__mo_one_centroids:2"))
	require.True(t, cachedIndexPresent("__mo_two_centroids:2"))
	require.True(t, dropped.destroyed)
	require.False(t, kept.destroyed)
}

// A table with no plugin indexes (or no table def at all) must be a cheap no-op
// — dropTableSingle calls this for every table dropped, including catalog ones.
func TestDispatchPluginDropIndexesNoPluginIndexes(t *testing.T) {
	kept := seedCachedIndex(t, "__mo_untouched_centroids:0")

	dispatchPluginDropIndexes(nil, &Compile{}, nil, "db", "tbl", nil, "")
	dispatchPluginDropIndexes(nil, &Compile{}, nil, "db", "tbl", &plan.TableDef{}, "")
	dispatchPluginDropIndexes(nil, &Compile{}, nil, "db", "tbl", &plan.TableDef{
		Indexes: []*plan.IndexDef{{IndexName: "uk", Unique: true}},
	}, "")

	require.True(t, cachedIndexPresent("__mo_untouched_centroids:0"))
	require.False(t, kept.destroyed)
}
