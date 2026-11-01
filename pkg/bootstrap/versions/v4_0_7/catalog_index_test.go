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

package v4_0_7

import (
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func indexCatalogCounts(t *testing.T, counts [3]int64) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	result := executor.NewMemResult([]types.Type{types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType()}, mp)
	result.NewBatchWithRowCount(1)
	for i, count := range counts {
		require.NoError(t, executor.AppendFixedRows(result, i, []int64{count}))
	}
	return result.GetResult()
}

func TestCatalogIndexValidation(t *testing.T) {
	for _, tc := range []struct {
		name, ddl string
		unique    bool
		counts    [3]int64
		found     bool
		conflict  bool
	}{
		{name: "valid nonunique", ddl: "create table t (a int, b int, key x(a,b))", counts: [3]int64{1, 1, 1}, found: true},
		{name: "valid unique", ddl: "create table t (a int, b int, unique key x(a,b))", unique: true, counts: [3]int64{1, 1, 1}, found: true},
		{name: "missing", ddl: "create table t (a int, b int, key other(a,b))"},
		{name: "wrong uniqueness", ddl: "create table t (a int, b int, key x(a,b))", unique: true, conflict: true},
		{name: "wrong order", ddl: "create table t (a int, b int, key x(b,a))", conflict: true},
		{name: "wrong column count", ddl: "create table t (a int, b int, key x(a))", conflict: true},
		{name: "descending part", ddl: "create table t (a int, b int, key x(a,b desc))", conflict: true},
		{name: "missing physical table", ddl: "create table t (a int, b int, key x(a,b))", counts: [3]int64{1, 1, 0}, conflict: true},
		{name: "multiple physical tables", ddl: "create table t (a int, b int, key x(a,b))", counts: [3]int64{1, 2, 2}, conflict: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
				if strings.HasPrefix(sql, "SHOW CREATE TABLE") {
					return newShowCreateTableResult(t, "t", tc.ddl), nil
				}
				require.Contains(t, sql, "from mo_catalog.mo_indexes")
				return indexCatalogCounts(t, tc.counts), nil
			})
			found, err := checkCatalogIndex(txn, 7, "mo_catalog", "t", "x", tc.unique, "a", "b")
			require.Equal(t, tc.found, found)
			var conflict *IndexConflict
			if tc.conflict {
				require.ErrorAs(t, err, &conflict)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCatalogIndexValidationPropagatesPhysicalLookupError(t *testing.T) {
	injected := errors.New("catalog read failed")
	txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "SHOW CREATE TABLE") {
			return newShowCreateTableResult(t, "t", "create table t (a int, key x(a))"), nil
		}
		return executor.Result{}, injected
	})
	_, err := checkCatalogIndex(txn, 7, "mo_catalog", "t", "x", false, "a")
	require.ErrorIs(t, err, injected)
}

func TestPublicationIndexRejectsStaleDatabaseReference(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		entry := publicationDatabaseIndex()
		writes := 0
		txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
			if strings.HasPrefix(sql, "select 1 from mo_catalog.mo_pubs p ") {
				result := executor.NewMemResult(nil, nil)
				result.NewBatchWithRowCount(1)
				return result.GetResult(), nil
			}
			writes++
			return executor.Result{}, nil
		})
		err := entry.Upgrade(txn, 0)
		require.ErrorContains(t, err, "repair publication catalog")
		require.Zero(t, writes, "a stale publication must not reach index creation")
	})
}
