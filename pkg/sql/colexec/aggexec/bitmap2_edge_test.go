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

package aggexec

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func bitmapPortableBytes(t *testing.T, bitmap *roaring.Bitmap) []byte {
	t.Helper()
	data, err := bitmap.ToBytes()
	require.NoError(t, err)
	return data
}

func TestAccountedBitmapWireRejectsTruncationAndMalformedContainers(t *testing.T) {
	arrayBitmap := roaring.BitmapOf(1, 3, 7, 1<<16|2, 1<<16|9)
	arrayWire := bitmapPortableBytes(t, arrayBitmap)

	denseBitmap := roaring.New()
	for value := uint32(0); value < 10000; value += 2 {
		denseBitmap.Add(value)
	}
	denseWire := bitmapPortableBytes(t, denseBitmap)

	runBitmap := roaring.New()
	runBitmap.AddRange(100, 400)
	runBitmap.RunOptimize()
	runWire := bitmapPortableBytes(t, runBitmap)

	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	for name, wire := range map[string][]byte{
		"array": arrayWire,
		"dense": denseWire,
		"run":   runWire,
	} {
		t.Run(name, func(t *testing.T) {
			count, err := scanAccountedBitmapWire(wire, nil)
			require.NoError(t, err)
			require.Positive(t, count)
			decoded := make([]uint32, count)
			written, err := scanAccountedBitmapWire(wire, decoded)
			require.NoError(t, err)
			require.Equal(t, count, written)
			require.Equal(t, bitmapPortableValues(t, wire), decoded)

			baseline := account.Snapshot().Used
			for cut := 0; cut < len(wire); cut++ {
				_, err = scanAccountedBitmapWire(wire[:cut], nil)
				require.Error(t, err, "scan cut=%d", cut)
				candidate, decodeErr := decodeAccountedBitmap(
					bytes.NewReader(wire[:cut]), mp, allocation)
				require.Error(t, decodeErr, "decode cut=%d", cut)
				require.Nil(t, candidate)
				require.Equal(t, baseline, account.Snapshot().Used, "cut=%d", cut)
			}
		})
	}

	mutations := []struct {
		name  string
		wire  []byte
		apply func([]byte)
	}{
		{name: "cookie", wire: arrayWire, apply: func(data []byte) {
			binary.LittleEndian.PutUint32(data, 99)
		}},
		{name: "container-count", wire: arrayWire, apply: func(data []byte) {
			binary.LittleEndian.PutUint32(data[4:], 1<<16+1)
		}},
		{name: "duplicate-container-key", wire: arrayWire, apply: func(data []byte) {
			copy(data[12:14], data[8:10])
		}},
		{name: "container-offset", wire: arrayWire, apply: func(data []byte) {
			binary.LittleEndian.PutUint32(data[16:], 1)
		}},
		{name: "array-order", wire: arrayWire, apply: func(data []byte) {
			container := int(binary.LittleEndian.Uint32(data[16:20]))
			copy(data[container+2:container+4], data[container:container+2])
		}},
		{name: "dense-cardinality", wire: denseWire, apply: func(data []byte) {
			binary.LittleEndian.PutUint16(data[10:12], 1)
		}},
		{name: "run-cardinality", wire: runWire, apply: func(data []byte) {
			header := 4 + (int(binary.LittleEndian.Uint32(data)>>16+1)+7)/8
			binary.LittleEndian.PutUint16(data[header+2:header+4], 1)
		}},
		{name: "run-count", wire: runWire, apply: func(data []byte) {
			count := int(binary.LittleEndian.Uint32(data)>>16) + 1
			container := 4 + (count+7)/8 + count*4
			if count >= bitmapNoOffsetThreshold {
				container += count * 4
			}
			binary.LittleEndian.PutUint16(data[container:container+2], ^uint16(0))
		}},
		{name: "trailing-data", wire: append(arrayWire, 0), apply: func([]byte) {}},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			data := append([]byte(nil), tc.wire...)
			tc.apply(data)
			_, err := scanAccountedBitmapWire(data, nil)
			require.Error(t, err)
			baseline := account.Snapshot().Used
			candidate, err := decodeAccountedBitmap(bytes.NewReader(data), mp, allocation)
			if err == nil {
				mpool.FreeSlice(mp, candidate)
			} else {
				require.Nil(t, candidate)
			}
			require.Equal(t, baseline, account.Snapshot().Used)
		})
	}

	require.ErrorIs(t, func() error {
		_, err := scanAccountedBitmapWire(arrayWire, make([]uint32, 1))
		return err
	}(), mpool.ErrAllocationAccountInvariant)
	_, err := decodeAccountedBitmap(nil, mp, allocation)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, err = decodeAccountedBitmap(bytes.NewReader(nil), nil, allocation)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, err = decodeAccountedBitmap(bytes.NewReader(nil), mp, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)

	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func bitmapPortableValues(t *testing.T, wire []byte) []uint32 {
	t.Helper()
	bitmap := roaring.New()
	require.NoError(t, bitmap.UnmarshalBinary(wire))
	return bitmap.ToArray()
}

func TestAccountedBitmapPrimitiveOwnershipAndMergePaths(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)

	require.ErrorIs(t, func() error {
		_, err := makeBmp(nil, allocation)
		return err
	}(), mpool.ErrAllocationAccountInvalid)
	accounted, err := makeBmp(mp, allocation)
	require.NoError(t, err)
	legacy, err := makeBmp(mp, nil)
	require.NoError(t, err)
	require.NoError(t, legacy.add(2))
	require.NoError(t, legacy.add(4))
	require.NoError(t, accounted.add(3))
	require.NoError(t, accounted.add(1))
	require.NoError(t, accounted.add(3))
	require.Equal(t, []uint32{1, 3}, accounted.values)
	require.NoError(t, accounted.union(legacy))
	require.Equal(t, []uint32{1, 2, 3, 4}, accounted.values)

	accountedOther, err := makeBmp(mp, allocation)
	require.NoError(t, err)
	for _, value := range []uint32{3, 5, 7} {
		require.NoError(t, accountedOther.add(value))
	}
	require.NoError(t, accounted.union(accountedOther))
	require.Equal(t, []uint32{1, 2, 3, 4, 5, 7}, accounted.values)
	require.NoError(t, legacy.union(accountedOther))
	require.True(t, legacy.legacy.Contains(7))
	require.ErrorIs(t, (*bmp)(nil).union(accounted), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, accounted.union(nil), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, (*bmp)(nil).ensureCapacity(1), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, accounted.ensureCapacity(-1), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, (*bmp)(nil).appendSorted(nil), mpool.ErrAllocationAccountInvariant)
	require.NoError(t, accounted.appendSorted(nil))
	require.ErrorIs(t, accounted.appendSorted([]uint32{7}), mpool.ErrAllocationAccountInvariant)

	data, err := accounted.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, accounted.MarshaledSize(), len(data))
	require.ErrorIs(t, mergeAccountedBitmapWire(data, nil), mpool.ErrAllocationAccountInvariant)
	tooSmall, err := makeBmp(mp, allocation)
	require.NoError(t, err)
	require.ErrorIs(t, mergeAccountedBitmapWire(data, tooSmall), mpool.ErrAllocationAccountInvariant)
	require.NoError(t, tooSmall.ensureCapacity(len(accounted.values)))
	require.NoError(t, mergeAccountedBitmapWire(data, tooSmall))
	require.Equal(t, accounted.values, tooSmall.values)
	require.Error(t, mergeAccountedBitmapWire(data[:len(data)-1], tooSmall))

	require.ErrorIs(t, writeBitmapFull(&medianFailAfterWriter{remaining: 1, short: true}, []byte{1, 2}), io.ErrShortWrite)
	require.ErrorIs(t, writeBitmapFull(&medianFailAfterWriter{}, []byte{1}), errMedianInjectedWrite)
	require.ErrorIs(t, accounted.MarshalTo(nil), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, (*bmp)(nil).MarshalTo(io.Discard), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, (*bmp)(nil).UnmarshalFromReader(bytes.NewReader(nil)), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, accounted.UnmarshalFromReader(nil), mpool.ErrAllocationAccountInvalid)

	legacy.Free()
	accounted.Free()
	accountedOther.Free()
	tooSmall.Free()
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}
