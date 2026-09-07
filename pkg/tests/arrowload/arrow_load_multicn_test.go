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

package arrowload

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestArrowLoadMultiCN covers distributed record-batch fan-out through the
// public path. Shutdown/cancellation coverage uses deterministic request and
// cluster-lifecycle fault injection in the dedicated rollout and MinIO tests.
func TestArrowLoadMultiCN(t *testing.T) {
	c := startArrowLoadCluster(t, 2, true, false, true)
	db := openArrowLoadDB(t, c, 0)
	mustExec(t, db, "create database if not exists arrow_multicn")
	mustExec(t, db, "use arrow_multicn")
	path, ddl := fixtureLarge(t)

	t.Run("DistributedRecordBatchFanout", func(t *testing.T) { testArrowMultiCNFanout(t, db, path, ddl) })
}

// testArrowMultiCNFanout loads the "large" multi-record-batch fixture with
// `PARALLEL 'true'` against the 2-CN cluster and checks full row-count and content
// correctness. Shard-routing internals are already unit-tested in
// pkg/sql/compile's arrow_scope_test.go; this test's job is proving the whole thing
// produces correct data on a real multi-CN cluster, not re-deriving that routing.
func testArrowMultiCNFanout(t *testing.T, db *sql.DB, path, ddl string) {
	mustExec(t, db, "drop table if exists large_fanout")
	mustExec(t, db, fmt.Sprintf("create table large_fanout(%s)", ddl))
	mustExec(t, db, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table large_fanout parallel 'true'", path))

	require.Equal(t, int64(largeFixtureRows), queryCount(t, db, "select count(*) from large_fanout"))
	require.Equal(t, int64(largeFixtureRows), queryCount(t, db, "select count(distinct id) from large_fanout"))
	require.Equal(t, int64(0), queryCount(t, db,
		fmt.Sprintf("select count(*) from large_fanout where id < 0 or id >= %d", largeFixtureRows)))
}
