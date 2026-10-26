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

package v4_0_18

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func newVersionTxnExecutor(t *testing.T, mocker func(string) (executor.Result, error)) executor.TxnExecutor {
	t.Helper()
	txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	return executor.NewMemTxnExecutor(mocker, txnOperator)
}

func tableListResult(t *testing.T, rows [][2]string) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	res := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()}, mp)
	res.NewBatchWithRowCount(len(rows))
	cols := make([][]string, 2)
	for _, r := range rows {
		cols[0] = append(cols[0], r[0])
		cols[1] = append(cols[1], r[1])
	}
	for i, v := range cols {
		require.NoError(t, executor.AppendStringRows(res, i, v))
	}
	return res.GetResult()
}

func countResult(t *testing.T, n int64) executor.Result {
	t.Helper()
	mp := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	res := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, mp)
	res.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(res, 0, []int64{n}))
	return res.GetResult()
}

func TestHandlerMetadata(t *testing.T) {
	m := Handler.Metadata()
	require.Equal(t, "4.0.18", m.Version)
	require.Equal(t, "4.0.17", m.MinUpgradeVersion)
	require.Equal(t, versions.No, m.UpgradeCluster)
	require.Equal(t, versions.Yes, m.UpgradeTenant)
}

// Only HNSW metadata tables are listed.
func TestListHnswMetadataTables(t *testing.T) {
	var seen string
	txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		seen = sql
		return tableListResult(t, [][2]string{{"db1", "meta_a"}}), nil
	})
	got, err := listHnswMetadataTables(txn, 0)
	require.NoError(t, err)
	require.Equal(t, [][2]string{{"db1", "meta_a"}}, got)
	require.Contains(t, seen, "'"+catalog.Hnsw_TblType_Metadata+"'")
}

func TestUpgradeHnswMetadataQuantizeAltersOnlyMissingColumns(t *testing.T) {
	var alters []string
	txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		lower := strings.ToLower(sql)
		switch {
		case strings.Contains(lower, "alter table"):
			alters = append(alters, sql)
			return executor.Result{}, nil
		case strings.Contains(lower, "mo_columns"):
			if strings.Contains(sql, "'"+catalog.Hnsw_TblCol_Metadata_Quantize_Min+"'") {
				return countResult(t, 1), nil
			}
			return countResult(t, 0), nil
		default:
			return tableListResult(t, [][2]string{{"db1", "meta_a"}}), nil
		}
	})
	require.NoError(t, upgradeHnswMetadataQuantize(context.Background(), 0, txn))
	require.Len(t, alters, 1, "only the missing column is added: %v", alters)
	require.Contains(t, alters[0], "`db1`.`meta_a`")
	require.Contains(t, alters[0], catalog.Hnsw_TblCol_Metadata_Quantize_Max)
	require.Contains(t, strings.ToLower(alters[0]), "double default null")
}

// A table that already has both columns, or an account without HNSW indexes, is a no-op.
func TestUpgradeHnswMetadataQuantizeIsANoop(t *testing.T) {
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
			return tableListResult(t, [][2]string{{"db1", "meta_a"}}), nil
		}
	})
	require.NoError(t, upgradeHnswMetadataQuantize(context.Background(), 0, txn))
	require.Zero(t, alters)

	txn = newVersionTxnExecutor(t, func(string) (executor.Result, error) {
		return tableListResult(t, nil), nil
	})
	require.NoError(t, upgradeHnswMetadataQuantize(context.Background(), 0, txn))
}

func TestUpgradeHnswMetadataQuantizePropagatesErrors(t *testing.T) {
	want := errors.New("refused")
	txn := newVersionTxnExecutor(t, func(sql string) (executor.Result, error) {
		lower := strings.ToLower(sql)
		switch {
		case strings.Contains(lower, "alter table"):
			return executor.Result{}, want
		case strings.Contains(lower, "mo_columns"):
			return countResult(t, 0), nil
		default:
			return tableListResult(t, [][2]string{{"db1", "meta_a"}}), nil
		}
	})
	require.ErrorIs(t, upgradeHnswMetadataQuantize(context.Background(), 0, txn), want)

	txn = newVersionTxnExecutor(t, func(string) (executor.Result, error) { return executor.Result{}, want })
	require.ErrorIs(t, upgradeHnswMetadataQuantize(context.Background(), 0, txn), want)
}
