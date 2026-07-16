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

package hashtable

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

type testResizeReservation struct {
	commits   int
	rollbacks int
}

func (r *testResizeReservation) Commit(ResizePlan) { r.commits++ }
func (r *testResizeReservation) Rollback()         { r.rollbacks++ }

func TestInt64ResizeAllocationFailureRollsBackWithoutMutation(t *testing.T) {
	mp, err := mpool.NewMPool("int-resize-rollback", mpool.MB, mpool.NoLock)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	var ht Int64HashMap
	require.NoError(t, ht.Init(mp))
	defer ht.Free()
	ht.cells[0][0] = Int64HashMapCell{Key: 7, Mapped: 1}
	beforeBytes := mp.CurrNB()
	beforeCells := ht.cells
	reservation := new(testResizeReservation)
	ht.SetResizeAdmission(func(ResizePlan) (ResizeReservation, error) {
		return reservation, nil
	})

	err = ht.ResizeOnDemand(100_000)
	require.Error(t, err)
	require.Equal(t, 0, reservation.commits)
	require.Equal(t, 1, reservation.rollbacks)
	require.Equal(t, beforeBytes, mp.CurrNB())
	require.Equal(t, beforeCells[0][0], ht.cells[0][0])
	require.Equal(t, len(beforeCells), len(ht.cells))
}

func TestStringResizeAdmissionRejectsBeforeMutation(t *testing.T) {
	mp := mpool.MustNewNoLock("string-resize-admission")
	defer mpool.DeleteMPool(mp)

	var ht StringHashMap
	require.NoError(t, ht.Init(mp))
	defer ht.Free()
	beforeBytes := mp.CurrNB()
	beforeCells := ht.cellCnt
	sentinel := assertResizeAdmissionError{}
	ht.SetResizeAdmission(func(ResizePlan) (ResizeReservation, error) {
		return nil, sentinel
	})

	err := ht.ResizeOnDemand(100_000)
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, beforeBytes, mp.CurrNB())
	require.Equal(t, beforeCells, ht.cellCnt)
}

func TestResizePlanRejectsOverflow(t *testing.T) {
	mp := mpool.MustNewNoLock("resize-overflow")
	defer mpool.DeleteMPool(mp)
	var ht Int64HashMap
	require.NoError(t, ht.Init(mp))
	defer ht.Free()

	plan := ht.PlanResize(math.MaxUint64)
	require.True(t, plan.Invalid)
	require.ErrorIs(t, ht.ResizeWithPlan(plan), ErrInvalidResizePlan)
}

type assertResizeAdmissionError struct{}

func (assertResizeAdmissionError) Error() string { return "resize admission rejected" }
