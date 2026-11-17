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

package frontend

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/stretchr/testify/require"
)

func TestRebaseDeltaKeyDecision(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  rebaseDeltaKeyState
		policy rebaseConflictPolicy
		want   rebaseDeltaDecision
	}{
		{"unchanged", rebaseDeltaKeyState{headsEqual: true, featurePresent: true, upstreamPresent: true}, rebaseFail, rebaseDeltaDecision{}},
		{"upstream_only", rebaseDeltaKeyState{upstreamChanged: true, featurePresent: true, upstreamPresent: true}, rebaseFail, rebaseDeltaDecision{counts: [6]uint64{0, 1}}},
		{"same_insert", rebaseDeltaKeyState{true, true, true, true, true}, rebaseFail, rebaseDeltaDecision{counts: [6]uint64{1, 1}}},
		{"same_delete", rebaseDeltaKeyState{true, true, true, false, false}, rebaseKeepBranch, rebaseDeltaDecision{counts: [6]uint64{1, 1}}},
		{"insert", rebaseDeltaKeyState{featureChanged: true, featurePresent: true}, rebaseFail, rebaseDeltaDecision{counts: [6]uint64{1, 0, 0, 1}, insert: true}},
		{"update", rebaseDeltaKeyState{featureChanged: true, featurePresent: true, upstreamPresent: true}, rebaseKeepUpstream, rebaseDeltaDecision{counts: [6]uint64{1, 0, 0, 0, 1}, remove: true, insert: true}},
		{"delete", rebaseDeltaKeyState{featureChanged: true, upstreamPresent: true}, rebaseFail, rebaseDeltaDecision{counts: [6]uint64{1, 0, 0, 0, 0, 1}, remove: true}},
		{"fail_conflict", rebaseDeltaKeyState{true, true, false, true, true}, rebaseFail, rebaseDeltaDecision{counts: [6]uint64{1, 1, 1}}},
		{"keep_upstream", rebaseDeltaKeyState{true, true, false, true, true}, rebaseKeepUpstream, rebaseDeltaDecision{counts: [6]uint64{1, 1, 1}}},
		{"keep_branch_update", rebaseDeltaKeyState{true, true, false, true, true}, rebaseKeepBranch, rebaseDeltaDecision{counts: [6]uint64{1, 1, 1, 0, 1}, remove: true, insert: true}},
		{"keep_branch_restore", rebaseDeltaKeyState{true, true, false, true, false}, rebaseKeepBranch, rebaseDeltaDecision{counts: [6]uint64{1, 1, 1, 1}, insert: true}},
		{"keep_branch_delete", rebaseDeltaKeyState{true, true, false, false, true}, rebaseKeepBranch, rebaseDeltaDecision{counts: [6]uint64{1, 1, 1, 0, 0, 1}, remove: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, decideRebaseDeltaKey(tc.state, tc.policy))
		})
	}
}

func TestRebaseDeltaKeysUniqueAndBounded(t *testing.T) {
	mp := mpool.MustNewZero()
	allocator := &branchHashmapAllocator{upstream: malloc.GetDefault(nil),
		throttler: rscthrottler.NewMemThrottler("rebase-key-test", 1,
			rscthrottler.WithConstLimit(4<<20))}
	before := allocator.Available()
	keys, err := newRebaseDeltaKeys(allocator, mp, []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, keys.close())
		require.Equal(t, before, allocator.Available())
		require.Zero(t, mp.CurrNB())
	})
	vec := vector.NewVec(types.T_int64.ToType())
	defer vec.Free(mp)
	for i := 0; i < 140; i++ {
		require.NoError(t, vector.AppendFixed(vec, int64(i%70), false, mp))
	}
	require.NoError(t, keys.add(t.Context(), []*vector.Vector{vec}))
	require.NoError(t, keys.add(t.Context(), []*vector.Vector{vec}))
	require.EqualValues(t, 70, keys.store.ItemCount())
	seen := make(map[string]bool)
	require.NoError(t, keys.drain(t.Context(), nil, func(rows [][]string) error {
		require.LessOrEqual(t, len(rows), rebaseDeltaKeyBatch)
		for _, row := range rows {
			require.Len(t, row, 1)
			require.False(t, seen[row[0]])
			seen[row[0]] = true
		}
		return nil
	}))
	for i := 0; i < 70; i++ {
		require.True(t, seen[fmt.Sprint(i)])
	}
	require.Zero(t, keys.store.ItemCount())
	require.NoError(t, keys.close())
	require.Error(t, keys.add(t.Context(), []*vector.Vector{vec}))
	_, err = newRebaseDeltaKeys(allocator, mp, []types.Type{types.T_float64.ToType()})
	require.Error(t, err)
	limited := &branchHashmapAllocator{upstream: malloc.GetDefault(nil),
		throttler: rscthrottler.NewMemThrottler("rebase-key-limit", 1,
			rscthrottler.WithConstLimit(rebaseDeltaKeyScratchBytes-1))}
	_, err = newRebaseDeltaKeys(limited, mp, []types.Type{types.T_int64.ToType()})
	require.Error(t, err)
}

func TestRebaseDeltaKeysSpillLifecycle(t *testing.T) {
	for _, scenario := range []string{"drain", "cancel", "write_error"} {
		t.Run(scenario, func(t *testing.T) {
			mp := mpool.MustNewZero()
			allocator := &branchHashmapAllocator{upstream: malloc.GetDefault(nil),
				throttler: rscthrottler.NewMemThrottler("rebase-key-spill", 1,
					rscthrottler.WithConstLimit(rebaseDeltaKeyScratchBytes+128))}
			before := allocator.Available()
			keys, err := newRebaseDeltaKeys(allocator, mp, []types.Type{types.T_int64.ToType()})
			require.NoError(t, err)
			t.Cleanup(func() {
				closeErr := keys.close()
				if scenario != "write_error" {
					require.NoError(t, closeErr)
				}
				require.Equal(t, before, allocator.Available())
				require.Zero(t, mp.CurrNB())
			})
			root := t.TempDir()
			if scenario == "write_error" {
				root = filepath.Join(root, "not-a-directory")
				require.NoError(t, os.WriteFile(root, []byte("owned fault fixture"), 0600))
			}
			// The existing store seam selects an owned filesystem without adding
			// a production failpoint or changing the shared memory budget.
			require.NoError(t, keys.store.Close())
			keys.store, err = databranchutils.NewBranchHashmap(
				databranchutils.WithBranchHashmapAllocator(allocator),
				databranchutils.WithBranchHashmapStrictCapacity(),
				databranchutils.WithBranchHashmapSpillRoot(root))
			require.NoError(t, err)
			vec := vector.NewVec(types.T_int64.ToType())
			defer vec.Free(mp)
			for i := 0; i < 128; i++ {
				require.NoError(t, vector.AppendFixed(vec, int64(i), false, mp))
			}
			err = keys.add(t.Context(), []*vector.Vector{vec})
			if scenario == "write_error" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			files := 0
			require.NoError(t, filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
				if err == nil && !entry.IsDir() {
					files++
				}
				return err
			}))
			require.Positive(t, files, "test must force real spill files")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if scenario == "cancel" {
				cancel()
			}
			count := 0
			err = keys.drain(ctx, nil, func(rows [][]string) error {
				count += len(rows)
				return nil
			})
			if scenario == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, count)
			} else {
				require.NoError(t, err)
				require.Equal(t, 128, count)
			}
			require.NoError(t, keys.close())
			entries, err := os.ReadDir(root)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestRebaseDeltaBatchKeys(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(3)
	t.Cleanup(func() {
		bat.Clean(mp)
		require.Zero(t, mp.CurrNB())
	})
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[i], int64(i), false, mp))
	}
	bat.SetRowCount(1)
	wrapped := batchWithKind{batch: bat}
	keys, err := rebaseDeltaBatchKeys(t.Context(), wrapped, []int{2, 0})
	require.NoError(t, err)
	require.Len(t, keys, 2)
	require.Same(t, bat.Vecs[2], keys[0])
	require.Same(t, bat.Vecs[0], keys[1])
	for _, indexes := range [][]int{nil, {-1}, {3}} {
		_, err = rebaseDeltaBatchKeys(t.Context(), wrapped, indexes)
		require.Error(t, err)
	}
	_, err = rebaseDeltaBatchKeys(t.Context(), batchWithKind{}, []int{0})
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = rebaseDeltaBatchKeys(ctx, wrapped, []int{0})
	require.ErrorIs(t, err, context.Canceled)
	// Projection borrows vectors and never frees or mutates their contents.
	require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[2], 0))
}

func TestRebaseDeltaLayout(t *testing.T) {
	columns := []rebaseSQLColumn{{"id", types.T_int64.ToType(), true}, {"v", types.T_int64.ToType(), false}}
	keys, ok := rebaseDeltaLayout(columns)
	require.True(t, ok)
	require.Equal(t, []types.Type{types.T_int64.ToType()}, keys)
	floatColumns := append([]rebaseSQLColumn(nil), columns...)
	floatColumns[1].typ = types.T_float64.ToType()
	_, ok = rebaseDeltaLayout(floatColumns)
	require.True(t, ok, "candidate keys precede DIFF value equality")
	for _, typ := range []types.Type{types.T_text.ToType(), types.New(types.T_varchar, 4097, 0), types.T_timestamp.ToType(), types.New(types.T_timestamp, 0, 6)} {
		copy := append([]rebaseSQLColumn(nil), columns...)
		copy[1].typ = typ
		_, ok = rebaseDeltaLayout(copy)
		require.False(t, ok)
	}
	columns[0].typ = types.T_varchar.ToType()
	_, ok = rebaseDeltaLayout(columns)
	require.False(t, ok)
}

func TestRebaseDeltaReplayBudgetFailure(t *testing.T) {
	allocator := &branchHashmapAllocator{upstream: malloc.GetDefault(nil),
		throttler: rscthrottler.NewMemThrottler("rebase-replay-limit", 1,
			rscthrottler.WithConstLimit(rebaseDeltaReplayBufferBytes-1))}
	before := allocator.Available()
	// Fails before executor/session access; no candidate writes or retry can run.
	replay, err := prepareRebaseDeltaReplay(t.Context(), nil, nil, rebasePlannedTable{},
		[4]rebaseSQLRead{}, rebaseFail, allocator, []types.Type{types.T_int64.ToType()})
	require.Error(t, err)
	require.Nil(t, replay)
	require.Equal(t, before, allocator.Available())
}
