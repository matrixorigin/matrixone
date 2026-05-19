// Copyright 2021-2024 Matrix Origin
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

package multi_update

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestReplaceGroupIdDedup_FanOutCollapse covers the REPLACE INTO multi-UK
// scenario where the OR'd LEFT JOIN fan-out repeats the same logical new row
// across N pipeline rows. The MULTI_UPDATE operator should keep only the
// first occurrence of each group_id on the insert side; everything else is
// dropped before reaching insert sinkers / Write.
func TestReplaceGroupIdDedup_FanOutCollapse(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	// 3 fan-out rows from the same new logical row (group_id=42) plus a
	// genuinely distinct new row (group_id=99). Other columns are just
	// stand-ins; only the group_id column is consulted by the helper.
	groupIDs := []int64{42, 42, 42, 99}
	payload := []int64{1, 1, 1, 2}
	inBat := batch.New([]string{"payload", "group_id"})
	inBat.SetVector(0, testutil.MakeInt64Vector(payload, nil, proc.Mp()))
	inBat.SetVector(1, testutil.MakeInt64Vector(groupIDs, nil, proc.Mp()))
	inBat.SetRowCount(len(groupIDs))
	defer inBat.Clean(proc.Mp())

	op := &MultiUpdate{InsertGroupIdIdx: 1}
	op.ctr.insertedGroupIds = map[string]struct{}{}

	dedup, err := op.dedupBatchByGroupId(proc, inBat)
	require.NoError(t, err)
	require.NotNil(t, dedup, "fan-out should produce a shrunk batch")
	defer dedup.Clean(proc.Mp())

	require.Equal(t, 2, dedup.RowCount(), "should keep one row per group_id")
	gotGroup := vector.MustFixedColNoTypeCheck[int64](dedup.Vecs[1])
	require.Equal(t, []int64{42, 99}, gotGroup,
		"first occurrence of each group_id is kept; later duplicates are dropped")
}

// TestReplaceGroupIdDedup_NoShrinkWhenUnique returns the original batch (nil
// sentinel) when every row's group_id is unique. This is the fast path for
// REPLACE without fan-out (single-UK conflict, no conflict, etc.).
func TestReplaceGroupIdDedup_NoShrinkWhenUnique(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	groupIDs := []int64{1, 2, 3}
	inBat := batch.New([]string{"group_id"})
	inBat.SetVector(0, testutil.MakeInt64Vector(groupIDs, nil, proc.Mp()))
	inBat.SetRowCount(len(groupIDs))
	defer inBat.Clean(proc.Mp())

	op := &MultiUpdate{InsertGroupIdIdx: 0}
	op.ctr.insertedGroupIds = map[string]struct{}{}

	dedup, err := op.dedupBatchByGroupId(proc, inBat)
	require.NoError(t, err)
	require.Nil(t, dedup, "all-unique group ids should not allocate a shrunk batch")
}

// TestReplaceGroupIdDedup_AcrossBatches verifies the hashset persists across
// successive dedup calls so a logical new row inserted in batch N is not
// inserted again when its fan-out rows arrive in batch N+1.
func TestReplaceGroupIdDedup_AcrossBatches(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	op := &MultiUpdate{InsertGroupIdIdx: 0}
	op.ctr.insertedGroupIds = map[string]struct{}{}

	bat1 := batch.New([]string{"group_id"})
	bat1.SetVector(0, testutil.MakeInt64Vector([]int64{1, 2}, nil, proc.Mp()))
	bat1.SetRowCount(2)
	defer bat1.Clean(proc.Mp())

	dedup1, err := op.dedupBatchByGroupId(proc, bat1)
	require.NoError(t, err)
	require.Nil(t, dedup1, "no duplicates within batch 1")

	bat2 := batch.New([]string{"group_id"})
	bat2.SetVector(0, testutil.MakeInt64Vector([]int64{2, 3}, nil, proc.Mp()))
	bat2.SetRowCount(2)
	defer bat2.Clean(proc.Mp())

	dedup2, err := op.dedupBatchByGroupId(proc, bat2)
	require.NoError(t, err)
	require.NotNil(t, dedup2, "group_id=2 already seen in batch 1 should be dropped from batch 2")
	defer dedup2.Clean(proc.Mp())
	require.Equal(t, 1, dedup2.RowCount())
	require.Equal(t,
		[]int64{3},
		vector.MustFixedColNoTypeCheck[int64](dedup2.Vecs[0]),
		"only the unseen group_id=3 survives")
}

// TestReplaceGroupIdDedup_VarlenKey exercises the varlen branch of
// groupIdKey, used when the planner emits serial_full() (varchar) as the
// group-id expression for REPLACE INTO with multiple unique keys.
func TestReplaceGroupIdDedup_VarlenKey(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	groupIDs := []string{"a", "b", "a", "c"}
	inBat := batch.New([]string{"group_id"})
	inBat.SetVector(0, testutil.NewVector(len(groupIDs), types.T_varchar.ToType(), proc.Mp(), false, groupIDs))
	inBat.SetRowCount(len(groupIDs))
	defer inBat.Clean(proc.Mp())

	op := &MultiUpdate{InsertGroupIdIdx: 0}
	op.ctr.insertedGroupIds = map[string]struct{}{}

	dedup, err := op.dedupBatchByGroupId(proc, inBat)
	require.NoError(t, err)
	require.NotNil(t, dedup)
	defer dedup.Clean(proc.Mp())
	require.Equal(t, 3, dedup.RowCount(), "varlen dedup: a, b, c")
}

// TestReplaceGroupIdDedup_ConstVector regression-guards groupIdKey against a
// constant-vector group-id column. A naive `data[row*width : (row+1)*width]`
// implementation reads past the single-element backing storage of a const
// vector; this asserts that every row maps to the same key and no panic
// occurs.
func TestReplaceGroupIdDedup_ConstVector(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	constVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(42), 5, proc.Mp())
	require.NoError(t, err)
	inBat := batch.New([]string{"group_id"})
	inBat.SetVector(0, constVec)
	inBat.SetRowCount(5)
	defer inBat.Clean(proc.Mp())

	op := &MultiUpdate{InsertGroupIdIdx: 0}
	op.ctr.insertedGroupIds = map[string]struct{}{}

	dedup, err := op.dedupBatchByGroupId(proc, inBat)
	require.NoError(t, err)
	require.NotNil(t, dedup, "const vector with 5 rows of value 42 collapses to one row")
	defer dedup.Clean(proc.Mp())
	require.Equal(t, 1, dedup.RowCount())
}
