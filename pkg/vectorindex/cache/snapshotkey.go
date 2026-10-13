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

// snapshotKeySep separates the index table name from the snapshot timestamp in a cache key.
// Index table names are __mo_index_secondary_<uuidv7> and contain no '@'.
const snapshotKeySep = "@"

// SnapshotKey returns the cache key for indexTable read at ts. ts must be the effective
// snapshot TS (sqlexec.SqlProcess.EffectiveSnapshotTS).
func SnapshotKey(indexTable string, ts timestamp.Timestamp) string {
	return fmt.Sprintf("%s%s%d-%d", indexTable, snapshotKeySep, ts.PhysicalTime, ts.LogicalTime)
}

// IsSnapshotKey reports whether key is a snapshot key. Current-generation entries are keyed by
// the bare index table name.
func IsSnapshotKey(key string) bool {
	return strings.Contains(key, snapshotKeySep)
}
