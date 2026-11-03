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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestBorrowedDataShrinkPreservesRetainedView(t *testing.T) {
	mp := mpool.MustNewZero()
	data := types.EncodeSlice([]int64{10, 20, 30, 40})
	lease, err := NewRefCountedBufferLease(data, int64(cap(data)), nil)
	require.NoError(t, err)
	t.Cleanup(lease.Release)
	v, err := NewBorrowedFixedVector(types.T_int64.ToType(), 4, data, lease)
	require.NoError(t, err)
	t.Cleanup(func() { v.Free(mp) })
	view, err := v.Window(0, 4)
	require.NoError(t, err)
	t.Cleanup(func() { view.Free(mp) })

	v.Shrink([]int64{1, 3}, false)
	require.Equal(t, []int64{20, 40}, MustFixedColWithTypeCheck[int64](v))
	require.Equal(t, []int64{10, 20, 30, 40}, MustFixedColWithTypeCheck[int64](view))
}
