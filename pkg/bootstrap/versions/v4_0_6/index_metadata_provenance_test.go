// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package v4_0_6

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// fulltext2 already had nrow, so only build_ts is added there; the vector algorithms need both.
func TestIndexMetadataProvenanceColumns(t *testing.T) {
	require.Equal(t,
		[]string{catalog.FullText2Index_TblCol_Metadata_Build_Ts},
		indexMetadataProvenanceColumns(catalog.FullText2Index_TblType_Metadata))

	for _, typ := range []string{
		catalog.Hnsw_TblType_Metadata,
		catalog.Cagra_TblType_Metadata,
		catalog.Ivfpq_TblType_Metadata,
	} {
		require.Equal(t,
			[]string{catalog.Hnsw_TblCol_Metadata_Nrow, catalog.Hnsw_TblCol_Metadata_Build_Ts},
			indexMetadataProvenanceColumns(typ), typ)
	}
}

// tableListResult fakes the (database, table, algo_table_type) enumeration.
func tableListResult(t *testing.T, rows [][3]string) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	res := executor.NewMemResult(
		[]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}, mp)
	res.NewBatchWithRowCount(len(rows))
	cols := make([][]string, 3)
	for _, r := range rows {
		cols[0] = append(cols[0], r[0])
		cols[1] = append(cols[1], r[1])
		cols[2] = append(cols[2], r[2])
	}
	for i, v := range cols {
		require.NoError(t, executor.AppendStringRows(res, i, v))
	}
	return res.GetResult()
}

// countResult fakes the mo_columns existence probe.
func countResult(t *testing.T, n int64) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	res := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, mp)
	res.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(res, 0, []int64{n}))
	return res.GetResult()
}

func TestListIndexMetadataTables(t *testing.T) {
	var seen string
	txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		seen = sql
		return tableListResult(t, [][3]string{
			{"db1", "meta_a", catalog.Hnsw_TblType_Metadata},
			{"db2", "meta_b", catalog.FullText2Index_TblType_Metadata},
		}), nil
	})

	got, err := listIndexMetadataTables(txn, 0)
	require.NoError(t, err)
	require.Equal(t, [][3]string{
		{"db1", "meta_a", catalog.Hnsw_TblType_Metadata},
		{"db2", "meta_b", catalog.FullText2Index_TblType_Metadata},
	}, got)

	// It must enumerate through the catalog rather than guess names, and cover every algorithm
	// that has a metadata table -- a missed type silently leaves those indexes unmigrated.
	for _, typ := range indexMetadataProvenanceTypes {
		require.Contains(t, seen, typ)
	}
	require.Contains(t, seen, catalog.MO_INDEXES)
}

func TestListIndexMetadataTablesPropagatesError(t *testing.T) {
	want := errors.New("catalog read failed")
	txn := newVersionTxnExecutor(t, func(string) (executor.Result, error) {
		return executor.Result{}, want
	})
	_, err := listIndexMetadataTables(txn, 0)
	require.ErrorIs(t, err, want)
}

func TestHasIndexMetadataColumn(t *testing.T) {
	present := newVersionTxnExecutor(t, func(string) (executor.Result, error) { return countResult(t, 1), nil })
	ok, err := hasIndexMetadataColumn(present, 0, "db", "tbl", "build_ts")
	require.NoError(t, err)
	require.True(t, ok)

	absent := newVersionTxnExecutor(t, func(string) (executor.Result, error) { return countResult(t, 0), nil })
	ok, err = hasIndexMetadataColumn(absent, 0, "db", "tbl", "build_ts")
	require.NoError(t, err)
	require.False(t, ok)

	want := errors.New("boom")
	failing := newVersionTxnExecutor(t, func(string) (executor.Result, error) { return executor.Result{}, want })
	_, err = hasIndexMetadataColumn(failing, 0, "db", "tbl", "build_ts")
	require.ErrorIs(t, err, want)
}

// The migration alters only what is missing. MO has no ADD COLUMN IF NOT EXISTS, and a partially
// applied entry is retried whole, so re-altering an existing column would fail the retry.
func TestUpgradeIndexMetadataProvenanceAltersOnlyMissingColumns(t *testing.T) {
	var alters []string
	txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		lower := strings.ToLower(sql)
		switch {
		case strings.Contains(lower, "alter table"):
			alters = append(alters, sql)
			return executor.Result{}, nil
		case strings.Contains(lower, "mo_columns"):
			// nrow already present, build_ts missing.
			if strings.Contains(sql, catalog.Hnsw_TblCol_Metadata_Nrow) {
				return countResult(t, 1), nil
			}
			return countResult(t, 0), nil
		default:
			return tableListResult(t, [][3]string{{"db1", "meta_a", catalog.Hnsw_TblType_Metadata}}), nil
		}
	})

	require.NoError(t, upgradeIndexMetadataProvenance(context.Background(), 0, txn))
	require.Len(t, alters, 1, "only the missing column is altered: %v", alters)
	require.Contains(t, alters[0], catalog.Hnsw_TblCol_Metadata_Build_Ts)
	require.Contains(t, alters[0], "`db1`.`meta_a`")
	require.Contains(t, strings.ToLower(alters[0]), "bigint")
}

// Nothing to migrate is a no-op, not an error: a fresh cluster has every column already.
func TestUpgradeIndexMetadataProvenanceIsANoopWhenComplete(t *testing.T) {
	var alters int
	txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		lower := strings.ToLower(sql)
		switch {
		case strings.Contains(lower, "alter table"):
			alters++
			return executor.Result{}, nil
		case strings.Contains(lower, "mo_columns"):
			return countResult(t, 1), nil
		default:
			return tableListResult(t, [][3]string{{"db1", "meta_a", catalog.Ivfpq_TblType_Metadata}}), nil
		}
	})
	require.NoError(t, upgradeIndexMetadataProvenance(context.Background(), 0, txn))
	require.Zero(t, alters)
}

func TestUpgradeIndexMetadataProvenancePropagatesAlterError(t *testing.T) {
	want := errors.New("alter refused")
	txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		lower := strings.ToLower(sql)
		switch {
		case strings.Contains(lower, "alter table"):
			return executor.Result{}, want
		case strings.Contains(lower, "mo_columns"):
			return countResult(t, 0), nil
		default:
			return tableListResult(t, [][3]string{{"db1", "meta_a", catalog.Cagra_TblType_Metadata}}), nil
		}
	})
	require.ErrorIs(t, upgradeIndexMetadataProvenance(context.Background(), 0, txn), want)
}

// An account with no vector or fulltext2 index has nothing to enumerate and must not error.
func TestUpgradeIndexMetadataProvenanceNoIndexes(t *testing.T) {
	txn := newVersionTxnExecutor(t, func(string) (executor.Result, error) {
		return tableListResult(t, nil), nil
	})
	require.NoError(t, upgradeIndexMetadataProvenance(context.Background(), 0, txn))
}
