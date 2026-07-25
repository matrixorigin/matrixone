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

package hnsw

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// TestClearIndexSqls pins the in-place clear: two DELETEs (metadata + storage), each carrying
// WHERE TRUE so MO does NOT rewrite them to a TRUNCATE (which would swap the physical table id).
func TestClearIndexSqls(t *testing.T) {
	cfg := vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "__meta", IndexTable: "__store"}
	sqls := ClearIndexSqls(cfg)
	require.Len(t, sqls, 2)
	require.Contains(t, sqls[0], "`db`.`__meta`")
	require.Contains(t, sqls[1], "`db`.`__store`")
	for _, s := range sqls {
		require.Contains(t, s, "DELETE FROM")
		require.Contains(t, s, "WHERE TRUE", "bare DELETE FROM would truncate and churn the table id")
	}
}

// TestMaxMetadataTimestamp covers the rebuild generation-floor reader: it returns the metadata
// MAX(timestamp) on success and 0 when the read fails (best-effort → floor 0 → plain wall-clock).
func TestMaxMetadataTimestamp(t *testing.T) {
	mp := mpool.MustNewZero()
	cfg := vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "m", IndexTable: "s"}

	old := runSql
	defer func() { runSql = old }()

	// happy: loadHnswGeneration reads timestamp then count; MaxMetadataTimestamp returns the ts.
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		return genInt64Result(t, mp, 4242), nil
	}
	require.Equal(t, int64(4242), MaxMetadataTimestamp(nil, cfg))

	// read error → 0 (never blocks the rebuild).
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("read failed")
	}
	require.Equal(t, int64(0), MaxMetadataTimestamp(nil, cfg))
}
