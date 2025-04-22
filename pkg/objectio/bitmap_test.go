// Copyright 2021 Matrix Origin
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

package objectio

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitmap1(t *testing.T) {
	bm1 := GetReusableBitmap()
	require.True(t, bm1.Reusable())
	require.True(t, bm1.IsValid())
	require.True(t, bm1.IsEmpty())
	require.Equal(t, 0, bm1.Count())
	t.Log(bm1.Idx())
	bm1.Add(BitmapBitsInPool - 1)
	require.True(t, bm1.Contains(BitmapBitsInPool-1))
	require.Equal(t, 1, bm1.Count())
	require.False(t, bm1.IsEmpty())
	require.True(t, bm1.Reusable())
	require.Equal(t, 1, BitmapPool.InUseCount())

	bm1.Add(BitmapBitsInPool)
	require.Equal(t, 0, BitmapPool.InUseCount())
	require.True(t, bm1.Contains(BitmapBitsInPool))
	require.Equal(t, 2, bm1.Count())
	require.False(t, bm1.IsEmpty())
	require.False(t, bm1.Reusable())

	bm1.Release()
	require.False(t, bm1.Reusable())
	require.False(t, bm1.IsValid())

	bm2 := GetNoReuseBitmap()
	require.False(t, bm2.Reusable())
	require.True(t, bm2.IsValid())
	require.True(t, bm2.IsEmpty())
	require.Equal(t, 0, bm2.Count())

	bm1 = GetReusableBitmap()

	bm1.Add(3)
	bm1.Add(100)

	bm2.Add(5)
	bm2.Add(3)
	bm2.Add(200)

	bm1.Or(bm2)
	require.True(t, bm1.Contains(100))
	require.True(t, bm1.Contains(200))
	require.True(t, bm1.Contains(3))
	require.True(t, bm1.Contains(5))
	require.Equal(t, 4, bm1.Count())
	require.Equal(t, 1, BitmapPool.InUseCount())
	bm1.Release()
	require.Equal(t, 0, BitmapPool.InUseCount())

	bm1 = GetReusableBitmap()
	bm1.Add(2)
	bm1.Add(5)
	bm1.OrBitmap(bm2.bm)

	require.True(t, bm1.Contains(2))
	require.True(t, bm1.Contains(200))
	require.True(t, bm1.Contains(3))
	require.True(t, bm1.Contains(5))
	require.Equal(t, 4, bm1.Count())
	require.Equal(t, 1, BitmapPool.InUseCount())
	require.Equal(t, []uint64{2, 3, 5, 200}, bm1.ToArray())
	require.Equal(t, []int64{2, 3, 5, 200}, bm1.ToI64Array(nil))
	require.False(t, bm1.Contains(BitmapBitsInPool+1))

	bm1.Release()

	require.Equal(t, 0, len(bm1.ToI64Array(nil)))
	require.Equal(t, 0, len(bm1.ToArray()))

	var bm3 Bitmap
	require.Equal(t, 0, bm3.Count())
}
