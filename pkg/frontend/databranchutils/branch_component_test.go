// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databranchutils

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func branchComponentResult(t *testing.T, mp *mpool.MPool, rows []DataBranchMetadata) executor.Result {
	t.Helper()
	kinds := []types.T{types.T_uint64, types.T_uint64, types.T_int64, types.T_uint64, types.T_varchar, types.T_bool}
	b := batch.NewWithSize(len(kinds))
	for i, kind := range kinds {
		b.Vecs[i] = vector.NewVec(kind.ToType())
	}
	for _, row := range rows {
		require.NoError(t, vector.AppendFixed(b.Vecs[0], row.TableID, false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[1], row.PTableID, false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[2], row.CloneTS, false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[3], row.Creator, false, mp))
		require.NoError(t, vector.AppendBytes(b.Vecs[4], []byte(row.Level), false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[5], row.TableDeleted, false, mp))
	}
	b.SetRowCount(len(rows))
	return executor.Result{Mp: mp, Batches: []*batch.Batch{b}}
}

func TestLoadLockedBranchComponents(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	rows := []DataBranchMetadata{
		{TableID: 2, PTableID: 1, Creator: 7, Level: "table", TableDeleted: true},
		{TableID: 3, PTableID: 1, Creator: 8, Level: "table"},
		{TableID: 4, PTableID: 2, Creator: 9, Level: "alter:table"},
		{TableID: 10, PTableID: 9, Level: "table"}, // Unrelated component.
	}
	queries := 0
	query := func(ctx context.Context, sql string) (executor.Result, error) {
		require.NoError(t, ctx.Err())
		require.NotContains(t, sql, "for update")
		parts := strings.Split(sql, " where ")
		require.Len(t, parts, 2)
		filter := strings.Split(parts[1], " in (")
		require.Len(t, filter, 2)
		keys := strings.Split(strings.TrimSuffix(filter[1], ")"), ",")
		require.LessOrEqual(t, len(keys), 128)
		ids := make(map[uint64]bool)
		for _, key := range keys {
			id, err := strconv.ParseUint(key, 10, 64)
			require.NoError(t, err)
			ids[id] = true
		}
		var selected []DataBranchMetadata
		for _, row := range rows {
			key := row.TableID
			if filter[0] == "p_table_id" {
				key = row.PTableID
			} else {
				require.Equal(t, "table_id", filter[0])
			}
			if ids[key] {
				selected = append(selected, row)
			}
		}
		queries++
		return branchComponentResult(t, mp, selected), nil
	}
	admissions := 0
	dag, err := LoadLockedBranchComponents(context.Background(), []uint64{4, 3, 20, 4}, query, func(roots []uint64) error {
		admissions++
		require.Equal(t, []uint64{1, 20}, roots) // Includes absent, isolated key.
		require.Zero(t, mp.CurrNB())
		// Sibling commit and a new edge become visible only after admission.
		rows[1].TableDeleted = true
		rows = append(rows, DataBranchMetadata{TableID: 5, PTableID: 2, Level: "table"})
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, admissions)
	require.Len(t, dag.Info, 4)
	require.True(t, dag.Info[3].Deleted)
	require.Equal(t, uint64(2), dag.Info[5].ParentTableID)
	require.NotContains(t, dag.Info, uint64(10))
	require.Equal(t, []string{BranchSnapshotName(3)}, ComputeBranchReclaimDropList(dag, []uint64{3}))
	require.Zero(t, mp.CurrNB())

	// Crossing the IN boundary is six indexed queries, not 129 scalar probes.
	queries, rows = 0, nil
	ids := make([]uint64, 129)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	_, err = LoadLockedBranchComponents(context.Background(), ids, query, func(roots []uint64) error {
		require.Equal(t, ids, roots)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 6, queries)
}

func TestBranchComponentFailures(t *testing.T) {
	for _, name := range []string{"root changed", "cycle", "query error", "admission error", "cancel", "bad column", "contradictory row", "admission panic"} {
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			admitted := false
			failure := errors.New("injected failure")
			query := func(_ context.Context, sql string) (executor.Result, error) {
				var rows []DataBranchMetadata
				if strings.Contains(sql, "in (2)") {
					parent := uint64(1)
					if name == "root changed" && admitted {
						parent = 9
					}
					if name == "cycle" {
						parent = 2
					}
					rows = append(rows, DataBranchMetadata{TableID: 2, PTableID: parent, Level: "table"})
					if name == "contradictory row" {
						rows = append(rows, DataBranchMetadata{TableID: 2, PTableID: 8, Level: "table"})
					}
				}
				res := branchComponentResult(t, mp, rows)
				switch name {
				case "query error":
					return res, failure // Partial result must still be released.
				case "cancel":
					cancel()
				case "bad column":
					res.Batches[0].Vecs[4].SetType(types.T_text.ToType())
				}
				return res, nil
			}
			admit := func([]uint64) error {
				require.Zero(t, mp.CurrNB())
				admitted = true
				if name == "admission error" {
					return failure
				}
				if name == "admission panic" {
					panic(failure)
				}
				return nil
			}
			if name == "admission panic" {
				require.Panics(t, func() { _, _ = LoadLockedBranchComponents(ctx, []uint64{2}, query, admit) })
				return
			}
			dag, err := LoadLockedBranchComponents(ctx, []uint64{2}, query, admit)
			require.Error(t, err)
			require.Empty(t, dag.Info)
			if name == "root changed" {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
			}
			if name == "query error" || name == "admission error" {
				require.ErrorIs(t, err, failure)
			}
		})
	}
}
