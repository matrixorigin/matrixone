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

package message

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func testMp() *mpool.MPool {
	return mpool.MustNewZero()
}

func TestGroupSels_NilBeforeInit(t *testing.T) {
	var js GroupSels
	require.NoError(t, js.Finalize(3, 3, testMp()))
	require.Nil(t, js.offsets)
	require.Nil(t, js.Get(0))
}

func TestGroupSels_AllUnique(t *testing.T) {
	mp := testMp()
	var js GroupSels
	require.NoError(t, js.Init(3, mp))
	js.Insert(0, 0)
	js.Insert(1, 1)
	js.Insert(2, 2)
	require.NoError(t, js.Finalize(3, 3, mp))
	require.Nil(t, js.offsets)
	require.Nil(t, js.Get(0))
}

func TestGroupSels_Normal0Based(t *testing.T) {
	mp := testMp()
	var js GroupSels
	require.NoError(t, js.Init(4, mp))
	js.Insert(0, 10)
	js.Insert(1, 11)
	js.Insert(0, 12)
	js.Insert(1, 13)
	require.NoError(t, js.Finalize(2, 4, mp))
	require.NotNil(t, js.offsets)
	require.ElementsMatch(t, []int32{10, 12}, js.Get(0))
	require.ElementsMatch(t, []int32{11, 13}, js.Get(1))
	require.Empty(t, js.Get(2))
	js.Free(mp)
}

func TestGroupSels_Dedup1Based(t *testing.T) {
	mp := testMp()
	var js GroupSels
	require.NoError(t, js.Init(3, mp))
	js.Insert(1, 0)
	js.Insert(2, 1)
	js.Insert(1, 2)
	require.NoError(t, js.Finalize(2, 3, mp))
	require.NotNil(t, js.offsets)
	require.ElementsMatch(t, []int32{0, 2}, js.Get(1))
	require.ElementsMatch(t, []int32{1}, js.Get(2))
	require.Empty(t, js.Get(0))
	js.Free(mp)
}

func TestGroupSels_Free(t *testing.T) {
	mp := testMp()
	var js GroupSels
	require.NoError(t, js.Init(2, mp))
	js.Insert(0, 5)
	js.Free(mp)
	require.NoError(t, js.Finalize(1, 1, mp))
	require.Nil(t, js.offsets)
}

func TestGroupSels_AllNulls(t *testing.T) {
	// all rows are null — Init called but Insert never called
	mp := testMp()
	var js GroupSels
	require.NoError(t, js.Init(3, mp))
	// no Insert calls
	require.NoError(t, js.Finalize(0, 3, mp))
	require.Nil(t, js.offsets)
}

func TestGroupSels_NullsSkipped(t *testing.T) {
	// 4 input rows but only 3 inserted (1 null) — groupCount==n but inputRowCount!=groupCount
	// must NOT trigger all-unique path
	mp := testMp()
	var js GroupSels
	require.NoError(t, js.Init(4, mp))
	js.Insert(0, 0) // row 0 → group 0
	js.Insert(1, 1) // row 1 → group 1
	// row 2 is null, skipped
	js.Insert(2, 3) // row 3 → group 2
	require.NoError(t, js.Finalize(3, 4, mp))
	require.NotNil(t, js.offsets) // must NOT be nil
	require.ElementsMatch(t, []int32{0}, js.Get(0))
	require.ElementsMatch(t, []int32{1}, js.Get(1))
	require.ElementsMatch(t, []int32{3}, js.Get(2))
	js.Free(mp)
}
