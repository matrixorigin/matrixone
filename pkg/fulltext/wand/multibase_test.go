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

package wand

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSubIndexIdDistinct: each sub-index of one build gets a unique id under the
// build's uid, so multiple tag=0 bases never collide in the shared store.
func TestSubIndexIdDistinct(t *testing.T) {
	uid := "__ft_idx:1700000000000000"
	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		id := SubIndexId(uid, i)
		require.Falsef(t, seen[id], "duplicate sub-index id %q", id)
		seen[id] = true
		require.True(t, strings.HasPrefix(id, uid+":"))
	}
	require.Equal(t, "__ft_idx:1700000000000000:0", SubIndexId(uid, 0))
	require.Equal(t, "__ft_idx:1700000000000000:5", SubIndexId(uid, 5))
}

// TestDeleteAllBasesSqls: clears every tag=0 chunk + all metadata rows, leaving the
// tag=1 CdcTail untouched.
func TestDeleteAllBasesSqls(t *testing.T) {
	cfg := TableConfig{DbName: "db", IndexTable: "idxtbl", MetadataTable: "metatbl"}
	sqls := DeleteAllBasesSqls(cfg)
	require.Len(t, sqls, 2)
	// storage delete is scoped to tag=0 (must NOT be an unqualified DELETE that would
	// also wipe the tag=1 tail)
	require.Contains(t, sqls[0], "idxtbl")
	require.Contains(t, sqls[0], "tag")
	require.Contains(t, sqls[0], "= 0")
	require.NotContains(t, sqls[0], "metatbl")
	// metadata delete removes all base rows
	require.Contains(t, sqls[1], "metatbl")
	require.NotContains(t, sqls[1], "tag")
}

// TestWandMultiBaseBuildInsertSqls mirrors the CREATE build's multi-base path: a corpus
// past capacity splits into several sub-models (FinishSegments), each assigned a distinct
// SubIndexId, and each sub-model's INSERTs (metadata + chunks) must carry ONLY its own
// id — so the sub-indexes never collide in the shared store.
func TestWandMultiBaseBuildInsertSqls(t *testing.T) {
	b := NewBuilder("seg", testPkType)
	for i := int64(1); i <= 6; i++ {
		require.NoError(t, b.Add("term", i)) // 6 distinct-pk docs
	}
	models := b.FinishSegments(2)
	require.Len(t, models, 3, "cap=2 over 6 docs => 3 sub-indexes")

	cfg := TableConfig{DbName: "db", IndexTable: "ft_index", MetadataTable: "ft_meta"}
	uid := "ft_index:1700000000000000"
	ids := make([]string, len(models))
	for i := range models {
		ids[i] = SubIndexId(uid, i)
	}
	for i, m := range models {
		m.Id = ids[i]
		sqls, cleanup, err := m.ToInsertSqls(cfg, 123, 0)
		require.NoError(t, err)
		self := "'" + ids[i] + "'"
		found := false
		for _, s := range sqls {
			if strings.Contains(s, self) {
				found = true
			}
			for j, other := range ids {
				if j != i {
					require.NotContainsf(t, s, "'"+other+"'",
						"sub-index %s SQL leaked sibling id %s", ids[i], other)
				}
			}
		}
		require.Truef(t, found, "sub-index %s: no INSERT carried its own id", ids[i])
		cleanup()
	}
}
