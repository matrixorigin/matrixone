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

package materialized

import (
	"context"
	"math"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestWriterSourceCopiesIntoAttemptAccount(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unaccounted", true: "mixed producer accounts"}[mixed], func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			registry, err := mpool.NewAllocationAccountRegistry(2, 128)
			require.NoError(t, err)
			account, err := registry.Open(1 << 20)
			require.NoError(t, err)
			foreign, err := registry.Open(1 << 20)
			require.NoError(t, err)
			selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
			require.NoError(t, err)
			other, err := vector.NewAllocationAccountSelection(foreign, 1, 1, 2, 3, 4)
			require.NoError(t, err)
			input := batch.NewWithSchema(true, []string{"id", "payload"}, []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()})
			source := NewSource(1)
			budget := newTestSpillBudget(1<<20, math.MaxUint64, 1)
			config := budget.config(testSpillFactory(t.TempDir()))
			config.Allocation = selection
			t.Cleanup(func() {
				source.Close()
				input.Clean(mp)
				for _, a := range []*mpool.AllocationAccount{account, foreign} {
					require.Zero(t, a.Seal().Used)
					_, e := registry.Finalize(a)
					require.NoError(t, e)
				}
				memory, _, _ := budget.usage()
				require.Zero(t, memory)
				require.Zero(t, mp.CurrNB())
				mpool.DeleteMPool(mp)
			})
			if mixed {
				require.NoError(t, input.Vecs[0].SetAllocationAccount(other))
			}
			require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(7), false, mp))
			require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("writer payload outside the inline descriptor"), false, mp))
			require.NoError(t, input.Vecs[0].PreExtendGrouping(1, mp))
			input.Vecs[0].GetGrouping().Set(0)
			input.ExtraBuf = []byte{3, 4}
			input.SetRowCount(1)
			originalCharge := foreign.Snapshot().Used
			for range 2 {
				require.NoError(t, source.Begin(mp, config))
				require.NoError(t, source.Append(t.Context(), input))
				source.Finish(nil)
				retained := account.Snapshot().Used
				require.Positive(t, retained)
				require.Equal(t, originalCharge, foreign.Snapshot().Used, "the source account is never relabeled")
				out, charge, end, err := source.Next(t.Context(), 0, 0)
				require.NoError(t, err)
				require.False(t, end)
				require.NotNil(t, charge)
				// Register both owners before checking the copy's contents.
				func() {
					defer charge.Release()
					defer out.Clean(mp)
					require.Greater(t, account.Snapshot().Used, retained)
					for _, v := range out.Vecs {
						require.True(t, vector.AllocationAccountSelectionsEqual(selection, v.AllocationAccountSelection()))
					}
					source.ReleaseReader(0)
					require.Empty(t, source.batches)
					require.Positive(t, account.Snapshot().Used, "the reader owns its independent vector lease")
					memory, _, _ := budget.usage()
					require.Positive(t, memory, "metadata lives through reader ownership")
					require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](out.Vecs[0], 0))
					require.True(t, out.Vecs[0].GetGrouping().Contains(0))
					require.Equal(t, input.ExtraBuf, out.ExtraBuf)
					require.Equal(t, input.Attrs, out.Attrs)
					require.Equal(t, input.Vecs[1].GetBytesAt(0), out.Vecs[1].GetBytesAt(0))
				}()
				require.Zero(t, account.Snapshot().Used)
				memory, _, _ := budget.usage()
				require.Zero(t, memory)
			}
		})
	}
}

func TestWriterSourceCopyFailureOwnsNoPartialBatch(t *testing.T) {
	for _, failure := range []string{"metadata admission", "vector admission", "append cancellation", "reader admission", "reader cancellation"} {
		t.Run(failure, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			registry, err := mpool.NewAllocationAccountRegistry(1, 64)
			require.NoError(t, err)
			limit := uint64(1 << 20)
			if failure == "vector admission" {
				limit = 1
			}
			account, err := registry.Open(limit)
			require.NoError(t, err)
			selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
			require.NoError(t, err)
			source := NewSource(1)
			input := testInt64Batch(t, mp, 7)
			budget := newTestSpillBudget(1<<20, math.MaxUint64, 1)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			config := budget.config(testSpillFactory(t.TempDir()))
			config.Allocation = selection
			t.Cleanup(func() {
				source.Close()
				input.Clean(mp)
				require.Zero(t, account.Seal().Used)
				_, e := registry.Finalize(account)
				require.NoError(t, e)
				memory, _, _ := budget.usage()
				require.Zero(t, memory)
				require.Zero(t, mp.CurrNB())
				mpool.DeleteMPool(mp)
			})
			require.NoError(t, source.Begin(mp, config))
			switch failure {
			case "metadata admission":
				budget.setCap(testBudgetMemory, 1)
			case "append cancellation":
				cancel()
			}
			err = source.Append(ctx, input)
			if failure == "metadata admission" || failure == "vector admission" || failure == "append cancellation" {
				require.Error(t, err)
				if failure == "vector admission" {
					require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				}
				if failure == "append cancellation" {
					require.ErrorIs(t, err, context.Canceled)
				}
				require.Empty(t, source.batches)
				require.Zero(t, account.Snapshot().Used)
				memory, _, _ := budget.usage()
				require.Zero(t, memory)
				_, _, end, readErr := source.Next(t.Context(), 0, 0)
				require.True(t, end)
				require.Same(t, err, readErr)
			} else {
				require.NoError(t, err)
				source.Finish(nil)
				before := account.Snapshot().Used
				memory, _, _ := budget.usage()
				if failure == "reader admission" {
					budget.setCap(testBudgetMemory, memory)
				} else {
					cancel()
				}
				out, charge, _, readErr := source.Next(ctx, 0, 0)
				require.Error(t, readErr)
				require.Nil(t, out)
				require.Nil(t, charge)
				require.Equal(t, before, account.Snapshot().Used)
				after, _, _ := budget.usage()
				require.Equal(t, memory, after)
			}
		})
	}
}

func TestSourceSpillStopsAfterChildAdmissionCancellation(t *testing.T) {
	for _, stage := range []string{"memory", "disk", "fd", "file"} {
		t.Run(stage, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			input := testInt64Batch(t, mp, 7)
			source := newSource(1, 0)
			t.Cleanup(func() { source.Close(); input.Clean(mp); require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			child, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			stopped := context.Canceled
			budget := newTestSpillBudget(1<<20, 1<<20, 1)
			var created *os.File
			config := budget.config(func(fileCtx context.Context, _ string) (*os.File, error) {
				require.Same(t, child, fileCtx)
				f, err := os.CreateTemp(t.TempDir(), "cancelled-spill")
				created = f
				if err == nil && stage == "file" {
					cancel(stopped)
				}
				return f, err
			})
			memory, disk, fd := config.Budget.ReserveMemory, config.Budget.ReserveDisk, config.Budget.ReserveFD
			config.Budget.ReserveMemory = func(size uint64) (Reservation, error) {
				r, e := memory(size)
				if e == nil && stage == "memory" {
					cancel(stopped)
				}
				return r, e
			}
			config.Budget.ReserveDisk = func(size uint64) (GrowingReservation, error) {
				r, e := disk(size)
				if e == nil && stage == "disk" {
					cancel(stopped)
				}
				return r, e
			}
			config.Budget.ReserveFD = func(size uint64) (Reservation, error) {
				r, e := fd(size)
				if e == nil && stage == "fd" {
					cancel(stopped)
				}
				return r, e
			}
			require.NoError(t, source.Begin(mp, config))
			require.ErrorIs(t, source.Append(child, input), stopped)
			require.Zero(t, source.spillBatchCount)
			if stage == "file" {
				require.NotNil(t, created)
				info, err := created.Stat()
				require.NoError(t, err)
				require.Zero(t, info.Size(), "no header is written after cancellation")
			} else {
				require.Nil(t, created, "no file is created after an earlier admission stop")
			}
			out, charge, end, err := source.Next(t.Context(), 0, 0)
			require.True(t, end)
			require.Nil(t, out)
			require.Nil(t, charge)
			require.ErrorIs(t, err, stopped)
			source.Finish(err)
			source.ReleaseReader(0)
			memoryUsed, diskUsed, fdUsed := budget.usage()
			require.Zero(t, memoryUsed)
			require.Zero(t, diskUsed)
			require.Zero(t, fdUsed)
		})
	}
}
