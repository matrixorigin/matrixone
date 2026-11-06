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

package message

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func membershipIntBatch(t *testing.T, mp *mpool.MPool, vals []int32, nils []bool) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	t.Cleanup(func() { bat.Clean(mp) })
	for i, v := range vals {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, len(nils) > i && nils[i], mp))
	}
	bat.SetRowCount(len(vals))
	return bat
}

func TestMembershipTruthTable(t *testing.T) {
	for _, tc := range []struct {
		name string
		vals []int32
		nils []bool
		want [3][2]bool // probe 1, 2, NULL; matched/unknown
	}{
		{"empty", nil, nil, [3][2]bool{}},
		{"match-and-miss", []int32{1, 1}, nil, [3][2]bool{{true, false}, {false, false}, {false, true}}},
		{"null-does-not-hide-match", []int32{1, 0}, []bool{false, true}, [3][2]bool{{true, false}, {false, true}, {false, true}}},
		{"all-null", []int32{0}, []bool{true}, [3][2]bool{{false, true}, {false, true}, {false, true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
			build := membershipIntBatch(t, mp, tc.vals, tc.nils)
			probe := membershipIntBatch(t, mp, []int32{1, 2, 0}, []bool{false, false, true})
			index, err := NewMembershipIndex(context.Background(), []*batch.Batch{build}, 0, types.T_int32.ToType(), mp)
			require.NoError(t, err)
			require.NotNil(t, index)
			t.Cleanup(index.Free)
			for row, want := range tc.want {
				match, unknown, err := index.Probe(context.Background(), probe.Vecs[0], row)
				require.NoError(t, err)
				require.Equal(t, want, [2]bool{match, unknown})
			}
		})
	}
}

func TestMembershipBorrowedStringsAndConstVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
	key := append(bytes.Repeat([]byte{'k'}, 4096), 0, 'x')
	for _, oid := range []types.T{types.T_char, types.T_varchar, types.T_text, types.T_blob, types.T_binary, types.T_varbinary, types.T_datalink} {
		t.Run(oid.String(), func(t *testing.T) {
			typ := oid.ToType()
			vec, err := vector.NewConstBytes(typ, key, 256, mp)
			require.NoError(t, err)
			t.Cleanup(func() { vec.Free(mp) })
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vec
			bat.SetRowCount(256)
			base := mp.AllocatedBytes()
			index, err := NewMembershipIndex(context.Background(), []*batch.Batch{bat}, 0, typ, mp)
			require.NoError(t, err)
			require.NotNil(t, index)
			t.Cleanup(index.Free)
			require.Equal(t, index.Size(), mp.AllocatedBytes()-base)
			require.Equal(t, int64(4096), index.Size())
			match, unknown, err := index.Probe(context.Background(), vec, 255)
			require.NoError(t, err)
			require.True(t, match)
			require.False(t, unknown)
			probe := vector.NewVec(typ)
			t.Cleanup(func() { probe.Free(mp) })
			require.NoError(t, vector.AppendBytes(probe, key[:len(key)-1], false, mp))
			match, unknown, err = index.Probe(context.Background(), probe, 0)
			require.NoError(t, err)
			require.False(t, match)
			require.False(t, unknown)
			require.Zero(t, testing.AllocsPerRun(100, func() {
				_, _, _ = index.Probe(context.Background(), vec, 255)
			}))
		})
	}
}

func TestMembershipFixedDomainsAndProbeCancellation(t *testing.T) {
	for _, tc := range []struct {
		typ   types.T
		value any
	}{
		{types.T_bool, true},
		{types.T_int8, int8(-128)}, {types.T_int16, int16(-32768)},
		{types.T_int32, int32(-2147483648)}, {types.T_int64, int64(-9223372036854775808)},
		{types.T_uint8, uint8(255)}, {types.T_uint16, uint16(65535)},
		{types.T_uint32, uint32(4294967295)}, {types.T_uint64, uint64(18446744073709551615)},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
			vec := vector.NewVec(tc.typ.ToType())
			t.Cleanup(func() { vec.Free(mp) })
			require.NoError(t, vector.AppendAny(vec, tc.value, false, mp))
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vec
			bat.SetRowCount(1)
			index, err := NewMembershipIndex(context.Background(), []*batch.Batch{bat}, 0, tc.typ.ToType(), mp)
			require.NoError(t, err)
			require.NotNil(t, index)
			t.Cleanup(index.Free)
			match, unknown, err := index.Probe(context.Background(), vec, 0)
			require.NoError(t, err)
			require.True(t, match)
			require.False(t, unknown)
			nilVec := vector.NewConstNull(tc.typ.ToType(), 3, mp)
			t.Cleanup(func() { nilVec.Free(mp) })
			match, unknown, err = index.Probe(context.Background(), nilVec, 2)
			require.NoError(t, err)
			require.False(t, match)
			require.True(t, unknown)
			for _, at := range []int{1, 2} {
				ctx := &membershipCancelContext{Context: context.Background(), at: at}
				_, _, err = index.Probe(ctx, vec, 0)
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

func TestMembershipCollisionsAndBatchReferences(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
	batches := make([]*batch.Batch, 0, 3)
	for b := range 3 {
		vals := make([]int32, 128)
		for row := range vals {
			vals[row] = int32(b*128 + row)
		}
		batches = append(batches, membershipIntBatch(t, mp, vals, nil))
	}
	index, err := NewMembershipIndex(context.Background(), batches, 0, types.T_int32.ToType(), mp)
	require.NoError(t, err)
	require.NotNil(t, index)
	t.Cleanup(index.Free)
	for _, bat := range batches {
		for row := range bat.RowCount() {
			match, unknown, err := index.Probe(context.Background(), bat.Vecs[0], row)
			require.NoError(t, err)
			require.True(t, match)
			require.False(t, unknown)
		}
	}
}

func TestMembershipEligibilityAndBudget(t *testing.T) {
	mp, err := mpool.NewMPool("membership", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
	bat := membershipIntBatch(t, mp, []int32{1}, nil)
	for _, typ := range []types.Type{types.T_float32.ToType(), types.T_decimal64.ToType(), types.T_json.ToType(), types.T_array_float32.ToType()} {
		index, err := NewMembershipIndex(context.Background(), []*batch.Batch{bat}, 0, typ, mp)
		require.NoError(t, err)
		require.Nil(t, index)
	}
	for _, col := range []int{-1, 1} {
		index, err := NewMembershipIndex(context.Background(), []*batch.Batch{bat}, col, types.T_int32.ToType(), mp)
		require.NoError(t, err)
		require.Nil(t, index)
	}
	bat.Vecs[0].GetGrouping().Add(0)
	index, err := NewMembershipIndex(context.Background(), []*batch.Batch{bat}, 0, types.T_int32.ToType(), mp)
	require.NoError(t, err)
	require.Nil(t, index)
	bat.Vecs[0].GetGrouping().Clear()
	vec, err := vector.NewConstFixed(types.T_int32.ToType(), int32(1), 8193, mp)
	require.NoError(t, err)
	t.Cleanup(func() { vec.Free(mp) })
	large := batch.NewWithSize(1)
	large.Vecs[0] = vec
	large.SetRowCount(8193) // rounds above the 128 KiB per-index allowance
	base := mp.AllocatedBytes()
	index, err = NewMembershipIndex(context.Background(), []*batch.Batch{large}, 0, types.T_int32.ToType(), mp)
	require.NoError(t, err)
	require.Nil(t, index)
	require.Equal(t, base, mp.AllocatedBytes())
	blocker, err := mp.Alloc(1<<20, true)
	require.NoError(t, err)
	t.Cleanup(func() { mp.Free(blocker) })
	index, err = NewMembershipIndex(context.Background(), []*batch.Batch{bat}, 0, types.T_int32.ToType(), mp)
	require.NoError(t, err)
	require.Nil(t, index)
	require.Equal(t, int64(1<<20), mp.CurrNB())
	require.Equal(t, int32(1), vector.GetFixedAtNoTypeCheck[int32](bat.Vecs[0], 0))
	require.True(t, membershipAllocationPressure(moerr.NewOOMNoCtx(), 16, 1<<20))
	require.False(t, membershipAllocationPressure(moerr.NewInternalErrorNoCtx("unrelated"), 16, 1<<20))
	require.False(t, membershipAllocationPressure(errors.New("out of memory"), 16, 1<<20))
}

type membershipCancelContext struct {
	context.Context
	calls, at int
}

func (ctx *membershipCancelContext) Err() error {
	ctx.calls++
	if ctx.calls >= ctx.at {
		return context.Canceled
	}
	return nil
}

func TestMembershipCancellationReleasesPartialBuild(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
	bat := membershipIntBatch(t, mp, []int32{1, 2}, nil)
	base := mp.AllocatedBytes()
	for _, at := range []int{1, 2, 3, 4, 5, 6} {
		ctx := &membershipCancelContext{Context: context.Background(), at: at}
		index, err := NewMembershipIndex(ctx, []*batch.Batch{bat}, 0, types.T_int32.ToType(), mp)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, index)
		require.Equal(t, base, mp.AllocatedBytes())
	}
}

func TestMembershipConcurrentBuildersShareCap(t *testing.T) {
	mp, err := mpool.NewMPool("membership-concurrent", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { require.Zero(t, mp.AllocatedBytes()) })
	vec, err := vector.NewConstBytes(types.T_varchar.ToType(), bytes.Repeat([]byte{'k'}, 4096), 256, mp)
	require.NoError(t, err)
	t.Cleanup(func() { vec.Free(mp) })
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vec
	bat.SetRowCount(256)
	blocker, err := mp.Alloc((1<<20)-8*4096, true)
	require.NoError(t, err)
	t.Cleanup(func() { mp.Free(blocker) })
	baseline := mp.AllocatedBytes()
	const workers = 44
	indexes := make([]*MembershipIndex, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			indexes[i], errs[i] = NewMembershipIndex(context.Background(), []*batch.Batch{bat}, 0, types.T_varchar.ToType(), mp)
		}()
	}
	close(start)
	wg.Wait() // every admitted index remains live until after the assertions
	t.Cleanup(func() {
		for _, index := range indexes {
			if index != nil {
				index.Free()
			}
		}
	})
	var size int64
	var admitted int
	for i, index := range indexes {
		require.NoError(t, errs[i])
		if index != nil {
			size += index.Size()
			admitted++
			match, unknown, err := index.Probe(context.Background(), vec, 255)
			require.NoError(t, err)
			require.True(t, match)
			require.False(t, unknown)
		}
	}
	require.Equal(t, 8, admitted)
	require.Equal(t, int64(1<<20), mp.CurrNB())
	require.Equal(t, baseline+size, mp.AllocatedBytes())
}
