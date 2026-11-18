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

package partition

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// The partition executor formats and reparses the parent ALTER for each child.
// Unrelated legacy no-ops must remain recognizable on that second parse, while
// a mixed charset ALTER must fail before either parent or child changes.
func TestAlterPartitionLegacyNoopRoundTrip(t *testing.T) {
	runPartitionTableCreateAndDeleteTestsWithAware(t,
		func(embed.Cluster) int32 { return 0 },
		"create table %s (c int, b int) partition by hash(c) partitions 2",
		partition.PartitionMethod_Hash,
		func(int, partition.Partition) {},
		func(db, table string, cn embed.ServiceOperator, metadata partition.PartitionMetadata) {
			testutils.ExecSQL(t, db, cn, fmt.Sprintf("insert into %s values (1,10),(2,20)", table))
			for _, action := range []string{"disable keys", "enable keys", "force"} {
				testutils.ExecSQL(t, db, cn, fmt.Sprintf("alter table %s %s", table, action))
			}
			ctx := context.Background()
			exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
			res, err := exec.Exec(ctx, fmt.Sprintf("alter table %s add e enum('a'), character set utf8mb4", table), executor.Options{}.WithDatabase(db).WithAccountID(0))
			res.Close()
			require.ErrorContains(t, err, "ALTER TABLE charset/collation defaults")
			var count int64
			testutils.ExecSQLWithReadResult(t, db, cn, func(_ int, _ string, result executor.Result) {
				result.ReadRows(func(_ int, cols []*vector.Vector) bool {
					count = executor.GetFixedRows[int64](cols[0])[0]
					return false
				})
			}, fmt.Sprintf("select count(*) from %s", table))
			require.Equal(t, int64(2), count)
			res, err = exec.Exec(ctx, fmt.Sprintf("select e from %s", table), executor.Options{}.WithDatabase(db).WithAccountID(0))
			res.Close()
			require.Error(t, err, "the rejected mixed ALTER must not publish e")
			require.Len(t, metadata.Partitions, 2)
		},
		func(embed.ServiceOperator, partition.PartitionMetadata) {},
	)
}
