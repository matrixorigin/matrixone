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

func TestCloneToFlatCompactFreesPartialAllocation(t *testing.T) {
	srcMP := mpool.MustNew("clone-compact-source")
	src := NewVec(types.T_varchar.ToType())
	require.NoError(t, AppendBytes(src, make([]byte, 2<<20), false, srcMP))

	dstMP := mpool.MustNew("clone-compact-destination")
	oldLimit := mpool.CapLimit
	mpool.CapLimit = 1 << 20
	t.Cleanup(func() { mpool.CapLimit = oldLimit })
	got, err := src.CloneToFlatCompact(dstMP)
	require.Error(t, err)
	require.Nil(t, got)
	require.Equal(t, int64(0), dstMP.CurrNB())

	src.Free(srcMP)
	require.Equal(t, int64(0), srcMP.CurrNB())
}
