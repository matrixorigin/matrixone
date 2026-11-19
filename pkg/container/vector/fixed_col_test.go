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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestCheckedFixedColumnPreservesBackedEmptyView(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	v := NewVec(types.T_int32.ToType())
	t.Cleanup(func() { v.Free(mp) })
	require.NoError(t, AppendFixed(v, int32(7), false, mp))
	v.SetLength(0)
	checked := MustFixedColWithTypeCheck[int32](v)
	require.NotNil(t, checked)
	require.Empty(t, checked)
	require.Equal(t, v.Capacity(), cap(checked))
	// The unchecked logical accessor keeps its existing zero-row convention.
	require.Nil(t, MustFixedColNoTypeCheck[int32](v))
	v.SetLength(1)
	require.Equal(t, []int32{7}, MustFixedColWithTypeCheck[int32](v))
}
