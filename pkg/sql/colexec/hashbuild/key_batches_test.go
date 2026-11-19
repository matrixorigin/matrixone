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

package hashbuild

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestHashBuildPublishesCompleteChunkedMembership(t *testing.T) {
	typ := types.T_int64.ToType()
	tc := newTestCase(t, []types.Type{typ}, []*plan.Expr{newExpr(0, typ)})
	defer func() {
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.GetMessageBoard().Reset()
		tc.proc.Free()
		require.Zero(t, tc.proc.Mp().CurrNB())
	}()
	tc.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: 177, Expr: newExpr(0, typ), UseMembershipFilter: true, MustApply: true}
	require.NoError(t, tc.arg.Prepare(tc.proc))
	builder := &tc.arg.ctr.hashmapBuilder
	func() {
		input := batch.NewWithSize(1)
		input.Vecs[0] = vector.NewVec(typ)
		defer input.Clean(tc.proc.Mp())
		for row := 0; row < 8193; row++ {
			require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(row), false, tc.proc.Mp()))
		}
		input.SetRowCount(8193)
		require.NoError(t, builder.CopyBuildBatch(input, tc.proc))
	}()
	builder.InputBatchRowCount = 8193
	require.NoError(t, builder.BuildHashmap(true, false, true, tc.proc))
	require.NoError(t, tc.arg.handleRuntimeFilter(tc.proc))
	receiver := message.NewMessageReceiver([]int32{177}, message.AddrBroadCastOnCurrentCN(), tc.proc.GetMessageBoard())
	messages, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, messages, 1)
	msg := messages[0].(message.RuntimeFilterMessage)
	require.EqualValues(t, message.RuntimeFilter_UNIQUEJOINKEYS, msg.Typ)
	require.EqualValues(t, 8193, msg.Card)
	require.EqualValues(t, 8193, msg.Keys.Rows())
	require.Equal(t, 2, msg.Keys.ChunkCount())
	require.Empty(t, msg.Data, "membership has one authoritative chunked representation")
	require.Zero(t, builder.uniqueKeys.count())
	filter, err := docfilter.NewForCKeys(tc.proc.Ctx, msg.Keys, nil)
	require.NoError(t, err)
	defer filter.Free()
	tc.proc.GetMessageBoard().Reset()
	require.False(t, msg.Keys.Retain())
	require.True(t, filter.Test(types.EncodeFixed(int64(8192))))
	require.False(t, filter.Test(types.EncodeFixed(int64(8193))))
}

func TestHashBuildAllNullKeysPublishesDrop(t *testing.T) {
	typ := types.T_int64.ToType()
	tc := newTestCase(t, []types.Type{typ}, []*plan.Expr{newExpr(0, typ)})
	defer func() {
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.GetMessageBoard().Reset()
		tc.proc.Free()
		require.Zero(t, tc.proc.Mp().CurrNB())
	}()
	tc.arg.RuntimeFilterSpec = rawRuntimeFilterSpec(178, 100, typ)
	require.NoError(t, tc.arg.Prepare(tc.proc))
	builder := &tc.arg.ctr.hashmapBuilder
	func() {
		input := batch.NewWithSize(1)
		input.Vecs[0] = vector.NewConstNull(typ, 17, tc.proc.Mp())
		defer input.Clean(tc.proc.Mp())
		input.SetRowCount(17)
		require.NoError(t, builder.CopyBuildBatch(input, tc.proc))
	}()
	builder.InputBatchRowCount = 17
	require.NoError(t, builder.BuildHashmap(false, false, true, tc.proc))
	require.NoError(t, tc.arg.handleRuntimeFilter(tc.proc))
	receiver := message.NewMessageReceiver([]int32{178}, message.AddrBroadCastOnCurrentCN(), tc.proc.GetMessageBoard())
	messages, done, err := receiver.ReceiveMessage(false, tc.proc.Ctx)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, messages, 1)
	require.EqualValues(t, message.RuntimeFilter_DROP, messages[0].(message.RuntimeFilterMessage).Typ)
}

func TestHashmapUniqueKeyChunking(t *testing.T) {
	for _, pk := range []bool{false, true} {
		for _, rows := range []int{0, 1, 8191, 8192, 8193, 16385} {
			t.Run(fmt.Sprintf("pk=%v/rows=%d", pk, rows), func(t *testing.T) {
				proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
				defer proc.Free()
				hb := newTestHashmapBuilder(t)
				defer hb.Free(proc)
				typ := types.T_int32.ToType()
				require.NoError(t, hb.Prepare([]*plan.Expr{newExpr(0, typ), newExpr(1, typ), newExpr(2, typ)}, -1, -1, nil, proc))
				hb.uniqueKeySlots = []bool{true, false, true}
				distinct := rows
				if !pk && rows > 1 {
					distinct = (rows + 1) / 2
				}
				func() {
					input := batch.NewWithSize(3)
					defer input.Clean(proc.Mp())
					for col := range input.Vecs {
						input.Vecs[col] = vector.NewVec(typ)
					}
					for row := 0; row < rows; row++ {
						key := int32(row % distinct)
						require.NoError(t, vector.AppendFixed(input.Vecs[0], key, false, proc.Mp()))
						require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(0), false, proc.Mp()))
						require.NoError(t, vector.AppendFixed(input.Vecs[2], -key, false, proc.Mp()))
					}
					input.SetRowCount(rows)
					require.NoError(t, hb.CopyBuildBatch(input, proc))
				}()
				hb.InputBatchRowCount = rows
				require.NoError(t, hb.BuildHashmap(pk, false, true, proc))
				require.EqualValues(t, distinct, hb.GetGroupCount())
				require.Equal(t, distinct, hb.uniqueKeys.count())
				next := int32(0)
				require.NoError(t, hb.uniqueKeys.visit(func(vecs []*vector.Vector) error {
					require.Nil(t, vecs[1])
					require.LessOrEqual(t, vecs[0].Length(), 8192)
					for row, key := range vector.MustFixedColNoTypeCheck[int32](vecs[0]) {
						require.Equal(t, next, key)
						require.Equal(t, -next, vector.MustFixedColNoTypeCheck[int32](vecs[2])[row])
						next++
					}
					return nil
				}))
				require.EqualValues(t, distinct, next)
			})
		}
	}
}

type keyAppendCapacity struct {
	calls, failAt int
	used          uint64
	onFailure     func()
}

func (c *keyAppendCapacity) AcquireAllocationCapacity(bytes uint64) error {
	c.calls++
	if c.failAt > 0 && c.calls == c.failAt {
		if c.onFailure != nil {
			c.onFailure()
		}
		return mpool.ErrAllocationAccountCapacity
	}
	c.used += bytes
	return nil
}
func (c *keyAppendCapacity) ReleaseAllocationCapacity(bytes uint64) { c.used -= bytes }

func TestUniqueKeyAppendRollbackAcrossChunks(t *testing.T) {
	run := func(t *testing.T, failAt int) int {
		proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
		defer proc.Free()
		control := &keyAppendCapacity{}
		registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
		require.NoError(t, err)
		account, err := registry.OpenWithController(1<<30, control)
		require.NoError(t, err)
		hb := &HashmapBuilder{}
		require.NoError(t, hb.SetAllocationAccount(account))
		defer func() { hb.Free(proc); require.Zero(t, account.Snapshot().Used); require.Zero(t, control.used) }()
		left, right := vector.NewVec(types.T_int64.ToType()), vector.NewVec(types.T_varchar.ToType())
		defer left.Free(proc.Mp())
		defer right.Free(proc.Mp())
		for row := 0; row < 8196; row++ {
			require.NoError(t, vector.AppendFixed(left, int64(row), false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(right, []byte("a variable-width runtime key value"), row == 8191, proc.Mp()))
		}
		hb.curVecs = []*vector.Vector{left, right}
		require.NoError(t, hb.appendUniqueKeys(proc, 0, 8191, nil))
		control.calls, control.failAt = 0, failAt
		err = hb.appendUniqueKeys(proc, 8191, 5, nil)
		if failAt == 0 {
			require.NoError(t, err)
			require.Equal(t, 8196, hb.uniqueKeys.count())
		} else {
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Equal(t, 8191, hb.uniqueKeys.count())
			require.Empty(t, hb.uniqueKeys.rest)
			for _, v := range hb.uniqueKeys.first.vecs {
				require.Equal(t, 8191, v.Length())
				require.False(t, v.HasNull())
			}
		}
		hb.curVecs = nil
		return control.calls
	}
	calls := run(t, 0)
	for failAt := 1; failAt <= calls; failAt++ {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) { run(t, failAt) })
	}
}

func TestUniqueKeyRollbackInvariantDiscardsCollection(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	control := &keyAppendCapacity{}
	registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<30, control)
	require.NoError(t, err)
	hb := &HashmapBuilder{}
	require.NoError(t, hb.SetAllocationAccount(account))
	t.Cleanup(func() {
		hb.curVecs = nil
		hb.Free(proc)
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, control.used)
	})
	input := vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { input.Free(proc.Mp()) })
	require.NoError(t, vector.AppendFixedList(input, make([]int64, 8193), nil, proc.Mp()))
	hb.curVecs = []*vector.Vector{input, input}
	require.NoError(t, hb.appendUniqueKeys(proc, 0, 8191, nil))
	control.calls, control.failAt = 0, 1
	control.onFailure = func() {
		// Keep the backing owned but invalidate a captured logical checkpoint.
		// Use the real account rejection boundary, not a production test hook.
		hb.uniqueKeys.first.vecs[0].SetLength(0)
	}
	err = hb.appendUniqueKeys(proc, 8191, 2, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	require.False(t, mpool.IsRetryableAllocationCapacity(err))
	require.ErrorIs(t, hb.fallbackOptionalRuntimeFilterCollection(proc, err), mpool.ErrAllocationAccountInvariant)
	require.Empty(t, hb.uniqueKeys.first.vecs)
	require.Empty(t, hb.uniqueKeys.rest)
	require.Zero(t, hb.uniqueKeys.count())
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, control.used)
}

func TestUniqueKeyBatchesAligned(t *testing.T) {
	for _, selected := range []bool{false, true} {
		for _, rows := range []int{0, 1, 8191, 8192, 8193, 16385} {
			t.Run(fmt.Sprintf("selected=%v/rows=%d", selected, rows), func(t *testing.T) {
				proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
				defer proc.Free()
				hb := newTestHashmapBuilder(t)
				defer hb.Free(proc)
				left := vector.NewVec(types.T_int64.ToType())
				defer left.Free(proc.Mp())
				right := vector.NewVec(types.T_int64.ToType())
				defer right.Free(proc.Mp())
				var sels []int64
				for i := 0; i < rows; i++ {
					require.NoError(t, vector.AppendFixed(left, int64(i), false, proc.Mp()))
					require.NoError(t, vector.AppendFixed(right, int64(-i), false, proc.Mp()))
					if selected {
						sels = append(sels, int64(rows-i-1))
					}
				}
				hb.curVecs = []*vector.Vector{left, nil, right}
				hb.uniqueKeySlots = []bool{true, false, true}
				require.NoError(t, hb.appendUniqueKeys(proc, 0, rows, sels))
				require.Equal(t, rows, hb.uniqueKeys.count())
				next := 0
				require.NoError(t, hb.uniqueKeys.visit(func(vecs []*vector.Vector) error {
					require.Nil(t, vecs[1])
					require.LessOrEqual(t, vecs[0].Length(), 8192)
					require.Equal(t, vecs[0].Length(), vecs[2].Length())
					for i, value := range vector.MustFixedColNoTypeCheck[int64](vecs[0]) {
						want := next
						if selected {
							want = rows - next - 1
						}
						require.EqualValues(t, want, value)
						require.EqualValues(t, -want, vector.MustFixedColNoTypeCheck[int64](vecs[2])[i])
						next++
					}
					return nil
				}))
				require.Equal(t, rows, next)
				// curVecs are executor-owned in production, but borrowed here.
				hb.curVecs = nil
				hb.uniqueKeys.free(proc.Mp())
				require.Zero(t, hb.mapAllocationAccount.Snapshot().Used)
			})
		}
	}
}

func TestUniqueKeyBytePreflight(t *testing.T) {
	mp := mpool.MustNewZero()
	dst := vector.NewVec(types.T_varchar.ToType())
	defer dst.Free(mp)
	src := vector.NewVec(types.T_varchar.ToType())
	defer src.Free(mp)
	require.NoError(t, vector.AppendBytesList(src, [][]byte{make([]byte, 100), make([]byte, 100)}, nil, mp))
	n, err := keyAppendCount([]*vector.Vector{dst}, []*vector.Vector{src}, 0, 2, nil, 4096+24+100)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	_, err = keyAppendCount([]*vector.Vector{dst}, []*vector.Vector{src}, 0, 2, nil, 4096+24+99)
	require.ErrorIs(t, err, runtimefilter.ErrOptionalKeySize)
	require.Zero(t, dst.Length())
}

func TestUniqueKeyByteBoundedCollection(t *testing.T) {
	for _, selected := range []bool{false, true} {
		t.Run(fmt.Sprint(selected), func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			hb := newTestHashmapBuilder(t)
			left := vector.NewVec(types.T_varchar.ToType())
			right := vector.NewVec(types.T_int64.ToType())
			t.Cleanup(func() {
				hb.curVecs = nil // The test, rather than an executor, owns the sources.
				hb.Free(proc)
				left.Free(proc.Mp())
				right.Free(proc.Mp())
				require.Zero(t, hb.mapAllocationAccount.Snapshot().Used)
				require.Zero(t, proc.Mp().CurrNB())
			})
			for row := 0; row < 5; row++ {
				value := make([]byte, 100)
				value[0] = byte(row)
				require.NoError(t, vector.AppendBytes(left, value, false, proc.Mp()))
				require.NoError(t, vector.AppendFixed(right, int64(row), false, proc.Mp()))
			}
			hb.curVecs = []*vector.Vector{left, nil, right}
			hb.uniqueKeySlots = []bool{true, false, true}
			var sels []int64
			if selected {
				sels = []int64{4, 3, 2, 1, 0}
			}
			// Admit exactly two varlen descriptors and payloads per chunk.
			require.NoError(t, hb.appendUniqueKeysWithin(proc, 0, 5, sels, 4096+2*(24+100)))
			require.Len(t, hb.uniqueKeys.rest, 2)
			require.Equal(t, 5, hb.uniqueKeys.count())
			next := 0
			require.NoError(t, hb.uniqueKeys.visit(func(vecs []*vector.Vector) error {
				require.Nil(t, vecs[1])
				require.LessOrEqual(t, vecs[0].Length(), 2)
				require.Equal(t, vecs[0].Length(), vecs[2].Length())
				for row := 0; row < vecs[0].Length(); row++ {
					want := next
					if selected {
						want = 4 - next
					}
					require.Equal(t, left.GetRawBytesAt(want), vecs[0].GetRawBytesAt(row))
					require.EqualValues(t, want, vector.MustFixedColNoTypeCheck[int64](vecs[2])[row])
					next++
				}
				return nil
			}))
			require.Equal(t, 5, next)
		})
	}
}
