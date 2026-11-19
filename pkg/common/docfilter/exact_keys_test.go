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

package docfilter

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestExactKeysGloballySortedChunks(t *testing.T) {
	mp := mpool.MustNewZero()
	var chunks [][]byte
	const count = 8192*2 + 1
	for start := 0; start < count; start += 8192 {
		func() {
			v := vector.NewVec(types.T_varchar.ToType())
			defer v.Free(mp)
			for row := start; row < min(start+8192, count); row++ {
				require.NoError(t, vector.AppendBytes(v, []byte(fmt.Sprintf("key-%06d", count-row-1)), false, mp))
			}
			data, err := v.MarshalBinary()
			require.NoError(t, err)
			chunks = append(chunks, data)
		}()
	}
	released := 0
	keys, err := keycodec.NewKeyPayload(context.Background(), chunks, 0, func() { released++ })
	require.NoError(t, err)
	admission := &recordingMemoryAdmission{grant: true}
	f, err := NewExactKeys(context.Background(), keys, types.T_varchar.ToType(), admission)
	require.NoError(t, err)
	defer f.Free()
	keys.Release() // the message-board owner can retire before readers finish
	require.Zero(t, released)
	require.True(t, f.Exact())
	for row := 0; row < count; row++ {
		require.True(t, f.Test([]byte(fmt.Sprintf("key-%06d", row))), "key %d", row)
	}
	require.False(t, f.Test([]byte("key-999999")))
	share := f.Share()
	require.NotNil(t, share)
	f.Free()
	require.True(t, share.Test([]byte("key-008192")))
	share.Free()
	require.Equal(t, 1, released)
	require.ElementsMatch(t, admission.acquired, admission.released)
	require.Zero(t, mp.CurrNB())
}

func TestExactKeysUsesPrimaryKeyDomain(t *testing.T) {
	mp := mpool.MustNewZero()
	v := vector.NewVec(types.T_varchar.ToType())
	defer v.Free(mp)
	require.NoError(t, vector.AppendBytes(v, []byte("char-key "), false, mp))
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	keys, err := keycodec.NewKeyPayload(context.Background(), [][]byte{data}, 0, nil)
	require.NoError(t, err)
	defer keys.Release()
	f, err := NewExactKeys(context.Background(), keys, types.T_char.ToType(), nil)
	require.NoError(t, err)
	defer f.Free()
	require.True(t, f.Test([]byte("char-key")))
	require.True(t, f.Test([]byte("char-key    ")))
	require.False(t, f.Test([]byte("different")))
}

func TestExactKeysFloatNullAndCancellation(t *testing.T) {
	mp := mpool.MustNewZero()
	v := vector.NewVec(types.T_float64.ToType())
	defer v.Free(mp)
	require.NoError(t, vector.AppendFixedList(v, []float64{math.Copysign(0, -1), math.NaN(), 10, 99}, []bool{false, false, false, true}, mp))
	data, err := v.MarshalBinary()
	require.NoError(t, err)
	keys, err := keycodec.NewKeyPayload(context.Background(), [][]byte{data}, 0, nil)
	require.NoError(t, err)
	defer keys.Release()
	f, err := NewExactKeys(context.Background(), keys, types.T_float64.ToType(), nil)
	require.NoError(t, err)
	defer f.Free()
	require.True(t, f.Test(types.EncodeFixed(float64(0))))
	require.True(t, f.Test(types.EncodeFixed(math.Copysign(0, -1))))
	require.False(t, f.Test(types.EncodeFixed(math.NaN())))
	require.False(t, f.Test(types.EncodeFixed(float64(99))))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	admission := &recordingMemoryAdmission{grant: true}
	_, err = NewExactKeys(ctx, keys, types.T_float64.ToType(), admission)
	require.ErrorIs(t, err, context.Canceled)
	require.ElementsMatch(t, admission.acquired, admission.released)
}
