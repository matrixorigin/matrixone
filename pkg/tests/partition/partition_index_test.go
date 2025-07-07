// Copyright 2021 - 2024 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestInsertAndDeleteWithIndex(
	t *testing.T,
) {
	runPartitionClusterTest(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			sql := fmt.Sprintf(
				"create table %s (c int comment 'abc') partition by list (c) (partition p1 values in (1,2), partition p2 values in (3,4))",
				t.Name(),
			)

			testutils.ExecSQL(
				t,
				db,
				cn,
				sql,
			)

			fn := func() int64 {
				n := int64(0)
				for i := 0; i < partitionCount[idx]; i++ {
					testutils.ExecSQLWithReadResult(
						t,
						db,
						cn,
						func(i int, s string, r executor.Result) {
							r.ReadRows(
								func(rows int, cols []*vector.Vector) bool {
									n += executor.GetFixedRows[int64](cols[0])[0]
									return true
								},
							)
						},
						fmt.Sprintf("select count(1) from %s_p%d", table, i),
					)
				}
				return n
			}

			testutils.ExecSQL(
				t,
				db,
				cn,
				insert,
			)
			require.Equal(t, int64(results[idx][0]), fn())

			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					r.ReadRows(
						func(rows int, cols []*vector.Vector) bool {
							require.Equal(t, int64(results[idx][0]), executor.GetFixedRows[int64](cols[0])[0])
							return true
						},
					)
				},
				fmt.Sprintf("select count(1) from %s", table),
			)

			testutils.ExecSQL(
				t,
				db,
				cn,
				delete,
			)
			require.Equal(t, int64(results[idx][1]), fn())

			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					r.ReadRows(
						func(rows int, cols []*vector.Vector) bool {
							require.Equal(t, int64(results[idx][1]), executor.GetFixedRows[int64](cols[0])[0])
							return true
						},
					)
				},
				fmt.Sprintf("select count(1) from %s", table),
			)
		},
	)
}
