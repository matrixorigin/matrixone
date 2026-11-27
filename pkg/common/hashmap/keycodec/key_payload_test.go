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

package keycodec

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestKeyPayloadCompleteAndRetained(t *testing.T) {
	for _, rows := range []int{0, 1, 8191, 8192, 8193, 16385} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			mp := mpool.MustNewZero()
			var chunks [][]byte
			for base := 0; base < rows; base += KeyChunkRows {
				func() {
					v := vector.NewVec(types.T_int64.ToType(), mp)
					defer v.Free(mp)
					for i := base; i < min(rows, base+KeyChunkRows); i++ {
						require.NoError(t, vector.AppendFixed(v, int64(i), false, mp))
					}
					data, err := v.MarshalBinary()
					require.NoError(t, err)
					chunks = append(chunks, data)
				}()
			}
			released := 0
			p, err := NewKeyPayload(context.Background(), chunks, 0, func() { released++ })
			require.NoError(t, err)
			require.EqualValues(t, rows, p.Rows())
			require.True(t, p.Retain())
			p.Release()
			require.Zero(t, released)
			next := int64(0)
			require.NoError(t, p.Visit(context.Background(), func(v *vector.Vector) error {
				require.LessOrEqual(t, v.Length(), KeyChunkRows)
				for _, value := range vector.MustFixedColNoTypeCheck[int64](v) {
					require.Equal(t, next, value)
					next++
				}
				return nil
			}))
			require.EqualValues(t, rows, next)
			p.Release()
			require.Equal(t, 1, released)
			require.False(t, p.Retain())
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestKeyPayloadRejectsMalformedAndCancellation(t *testing.T) {
	mp := mpool.MustNewZero()
	v := vector.NewVec(types.T_int64.ToType(), mp)
	defer v.Free(mp)
	require.NoError(t, vector.AppendFixedList(v, make([]int64, KeyChunkRows+1), nil, mp))
	oversized, err := v.MarshalBinary()
	require.NoError(t, err)
	for _, chunks := range [][][]byte{{[]byte("invalid")}, {oversized}} {
		released := false
		_, err = NewKeyPayload(context.Background(), chunks, 0, func() { released = true })
		require.Error(t, err)
		require.False(t, released, "failed construction must not consume ownership")
	}
	v.SetLength(1)
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	p, err := NewKeyPayload(context.Background(), [][]byte{data}, 0, nil)
	require.NoError(t, err)
	defer p.Release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, p.Visit(ctx, func(*vector.Vector) error { t.Fatal("cancelled visitor ran"); return nil }), context.Canceled)
}

// A payload is shared by concurrent runtime-filter consumers. Every holder can
// read it while it holds a reference, the creator's cleanup runs exactly once
// after the last Release, and no Retain succeeds once cleanup has started.
func TestKeyPayloadConcurrentOwnership(t *testing.T) {
	newPayload := func(t *testing.T, cleaned *atomic.Int32) *KeyPayload {
		mp := mpool.MustNewZero()
		v := vector.NewVec(types.T_int64.ToType(), mp)
		require.NoError(t, vector.AppendFixedList(v, []int64{1, 2, 3}, nil, mp))
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		v.Free(mp)
		p, err := NewKeyPayload(context.Background(), [][]byte{data}, 0, func() { cleaned.Add(1) })
		require.NoError(t, err)
		return p
	}

	t.Run("last release cleans once", func(t *testing.T) {
		const holders = 128
		var cleaned, early atomic.Int32
		p := newPayload(t, &cleaned)
		for range holders {
			require.True(t, p.Retain())
		}
		var wait sync.WaitGroup
		wait.Add(holders)
		for range holders {
			go func() {
				defer wait.Done()
				// A holder may take and drop further references.
				if p.Retain() {
					p.Release()
				}
				if err := p.Visit(context.Background(), func(v *vector.Vector) error {
					if cleaned.Load() != 0 || v.Length() != 3 {
						early.Add(1)
					}
					return nil
				}); err != nil {
					early.Add(1)
				}
				p.Release()
			}()
		}
		wait.Wait()
		require.Zero(t, early.Load(), "a holder saw the payload cleaned")
		require.Zero(t, cleaned.Load(), "the creator still holds a reference")
		p.Release()
		require.Equal(t, int32(1), cleaned.Load())
		require.False(t, p.Retain())
	})

	t.Run("no retain after the last release", func(t *testing.T) {
		const contenders = 64
		var cleaned, resurrected atomic.Int32
		p := newPayload(t, &cleaned)
		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(contenders)
		for range contenders {
			go func() {
				defer wait.Done()
				<-start
				for cleaned.Load() == 0 {
					if p.Retain() {
						// A reference taken before cleanup keeps the payload
						// alive; its Release may be the last one.
						if cleaned.Load() != 0 {
							resurrected.Add(1)
						}
						p.Release()
					}
				}
				if p.Retain() {
					resurrected.Add(1)
					p.Release()
				}
			}()
		}
		close(start)
		p.Release() // the creator's reference
		wait.Wait()
		require.Zero(t, resurrected.Load(), "Retain succeeded after cleanup")
		require.Equal(t, int32(1), cleaned.Load())
	})
}
