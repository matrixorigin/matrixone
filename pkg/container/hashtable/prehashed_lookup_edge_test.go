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

package hashtable

import (
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestPrehashedLookupValidationAndRingRows(t *testing.T) {
	mp := mpool.MustNewZero()

	var ints Int64HashMap
	require.NoError(t, ints.Init(mp))
	keys := []uint64{7, 9}
	hashes := make([]uint64, len(keys))
	inserted := make([]uint64, len(keys))
	require.NoError(t, ints.InsertBatch(
		len(keys), hashes, unsafe.Pointer(&keys[0]), inserted))
	values := make([]uint64, len(keys))
	require.NoError(t, ints.FindPrehashedBatch(
		[]int64{1, 0}, hashes, values, true))
	require.Equal(t, []uint64{inserted[0], 0}, values)
	require.ErrorIs(t, ints.FindPrehashedBatch(nil, hashes, values, true),
		mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, ints.FindPrehashedBatch(nil, hashes, values[:1], false),
		mpool.ErrAllocationAccountInvalid)
	ints.Free()

	var strings StringHashMap
	require.NoError(t, strings.Init(mp))
	stringKeys := [][]byte{[]byte("seven"), []byte("nine")}
	states := make([][3]uint64, len(stringKeys))
	stringInserted := make([]uint64, len(stringKeys))
	require.NoError(t, strings.InsertStringBatch(states, stringKeys, stringInserted))
	values = make([]uint64, len(stringKeys))
	require.NoError(t, strings.FindPrehashedStringBatch(
		[]int64{1, 0}, states, values, true))
	require.Equal(t, []uint64{stringInserted[0], 0}, values)
	require.ErrorIs(t, strings.FindPrehashedStringBatch(nil, states, values, true),
		mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, strings.FindPrehashedStringBatch(nil, states, values[:1], false),
		mpool.ErrAllocationAccountInvalid)
	strings.Free()
	require.Zero(t, mp.CurrNB())
}
