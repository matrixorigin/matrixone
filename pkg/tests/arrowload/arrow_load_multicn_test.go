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
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// testArrowRecordBatchFanout loads a multi-batch local Arrow file with
// `PARALLEL 'true'` and checks row identity and content. The local file is split
// into worker scopes on the connected CN; separate MinIO cases cover object-store
// inputs. Planner routing is independently checked in
// pkg/sql/compile/arrow_scope_test.go.
func testArrowRecordBatchFanout(t *testing.T, db *sql.DB, path, ddl string) {
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(plan.LoadWriteS3MinSize), "fixture must retain the LoadWriteS3 path")

	mustExec(t, db, "drop table if exists large_fanout")
	mustExec(t, db, fmt.Sprintf("create table large_fanout(%s)", ddl))
	mustExec(t, db, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table large_fanout parallel 'true'", path))

	require.Equal(t, int64(fanoutFixtureRows), queryCount(t, db, "select count(*) from large_fanout"))
	require.Equal(t, int64(fanoutFixtureRows), queryCount(t, db, "select count(distinct id) from large_fanout"))
	require.Equal(t, int64(0), queryCount(t, db, fmt.Sprintf(
		"select count(*) from large_fanout where id < 0 or id >= %d or payload is null or payload <> concat('payload-row-', lpad(cast(id as char), 8, '0'), '-', repeat('x', 32))",
		fanoutFixtureRows,
	)))
}
