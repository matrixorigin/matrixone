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

package features

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

var (
	once         sync.Once
	shareCluster embed.Cluster
	mu           sync.Mutex
)

func TestTableFeatures(t *testing.T) {
	runFeaturesTests(
		t,
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			// Create table with primary key and an additional index
			sql := "create table %s (id int primary key, b int, unique key(b))"
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					// No need to check error as ExecSQLWithReadResult already handles it
				},
				fmt.Sprintf(sql, t.Name()),
			)

			// Check table extra information using mo_ctl
			var index uint64
			checkSQL := fmt.Sprintf(
				"select mo_ctl('cn', 'table-extra', '%s.%s')",
				strings.ToLower(db),
				strings.ToLower(t.Name()),
			)
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					var result string
					var extra api.SchemaExtra
					r.ReadRows(
						func(rows int, cols []*vector.Vector) bool {
							result = executor.GetStringRows(cols[0])[0]
							return false
						},
					)
					require.NoError(t, json.Unmarshal([]byte(testutils.MustParseMOCtlResult(t, result)), &extra))
					require.Equal(t, 1, len(extra.IndexTables))
					index = extra.IndexTables[0]
				},
				checkSQL,
			)

			checkSQL = fmt.Sprintf(
				"select mo_ctl('cn', 'table-extra', '%d')",
				index,
			)
			testutils.ExecSQLWithReadResult(
				t,
				db,
				cn,
				func(i int, s string, r executor.Result) {
					var result string
					var extra api.SchemaExtra
					r.ReadRows(
						func(rows int, cols []*vector.Vector) bool {
							result = executor.GetStringRows(cols[0])[0]
							return false
						},
					)
					require.NoError(t, json.Unmarshal([]byte(testutils.MustParseMOCtlResult(t, result)), &extra))
					require.Equal(t, 0, len(extra.IndexTables))
					require.NotEqual(t, uint64(0), extra.ParentTableID)
					require.True(t, features.IsIndexTable(extra.FeatureFlag))
				},
				checkSQL,
			)
		},
	)
}

func runFeaturesTests(
	t *testing.T,
	fn func(embed.Cluster),
) error {
	mu.Lock()
	defer mu.Unlock()

	var c embed.Cluster
	createFunc := func() embed.Cluster {
		new, err := embed.NewCluster(
			embed.WithCNCount(3),
			embed.WithTesting(),
		)
		require.NoError(t, err)
		require.NoError(t, new.Start())
		return new
	}

	once.Do(
		func() {
			c = createFunc()
			shareCluster = c
		},
	)
	c = shareCluster
	fn(c)
	return nil
}
