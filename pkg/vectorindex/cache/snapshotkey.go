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

package cache

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// Cache keys for named-snapshot index reads (#27927 / #27941).
//
// A snapshot is a CLONE: reading a table `{snapshot=...}` resolves the catalog at that TS,
// so the hidden index tables keep the SAME names they have now (verified: mo_indexes and
// mo_indexes{snapshot=...} return identical __mo_index_secondary_<uuid> values, one logical
// row read at two timestamps -- MVCC, not duplication). The storage contents at those two
// timestamps differ.
//
// Every other layer resolves that with a timestamp dimension. This cache is a sync.Map keyed
// by a bare string and has none, so it is the ONE place where the current and historical
// generations of an index would collide. The TS suffix is that missing dimension -- not
// decoration -- which is why it must carry identity: a flattened "<table>@snapshot" would
// serve snapshot A's index to a query for snapshot B.
const snapshotKeySep = "@"

// SnapshotKey returns the cache key under which indexTable's contents AS OF ts are cached.
// It is the single source of truth for the format: the search TVFs build keys with it and
// IsSnapshotKey/historicalCount recognise them with it, so the writers and the readers of
// the key space cannot drift apart.
//
// ts must be the EFFECTIVE snapshot TS (sqlexec.SqlProcess.EffectiveSnapshotTS) -- the same
// authority that decides whether the read txn is cloned -- so the key and the read can never
// disagree about which generation is being addressed.
func SnapshotKey(indexTable string, ts timestamp.Timestamp) string {
	return fmt.Sprintf("%s%s%d-%d", indexTable, snapshotKeySep, ts.PhysicalTime, ts.LogicalTime)
}

// IsSnapshotKey reports whether key addresses a historical generation rather than the
// current one. Current-generation entries are keyed by the bare index table name.
//
// Safe as a pure string test: index table names are __mo_index_secondary_<uuidv7>
// (pkg/sql/util.BuildIndexTableName), and neither that prefix nor a UUID contains '@'.
func IsSnapshotKey(key string) bool {
	return strings.Contains(key, snapshotKeySep)
}
