// Copyright 2021 - 2025 Matrix Origin
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

package partition

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestAffectedRows(t *testing.T) {
	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			sql := fmt.Sprintf(
				"create table %s (c int primary key, d int) partition by hash(c) partitions 2",
				t.Name(),
			)

			testutils.ExecSQL(
				t,
				db,
				cn,
				sql,
			)

			sql = fmt.Sprintf("insert into %s values (1, 1)", t.Name())
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					require.Equal(t, uint64(1), r.AffectedRows)
				},
				sql,
			)

			sql = fmt.Sprintf("update %s set d = 2 where c in (2,3)", t.Name())
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					require.Equal(t, uint64(0), r.AffectedRows)
				},
				sql,
			)

			sql = fmt.Sprintf("update %s set d = 2 where c in (1,2)", t.Name())
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					require.Equal(t, uint64(1), r.AffectedRows)
				},
				sql,
			)

			sql = fmt.Sprintf("delete from %s where c in (2,3)", t.Name())
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					require.Equal(t, uint64(0), r.AffectedRows)
				},
				sql,
			)

			sql = fmt.Sprintf("delete from %s where c in (1,2)", t.Name())
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					require.Equal(t, uint64(1), r.AffectedRows)
				},
				sql,
			)
		},
	)
}
