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

package arenaskl

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInserterCachedSuccessorOverlap(t *testing.T) {
	list := NewSkiplist(NewArena(make([]byte, 4096)), bytes.Compare)
	for _, key := range []string{"a", "c", "e"} {
		require.NoError(t, list.Add([]byte(key), nil))
	}
	var ins Inserter
	require.False(t, list.findSplice([]byte("b"), &ins))
	require.True(t, list.findSplice([]byte("c"), &ins), "a cached successor must be recognized without a list-height change")
	for _, key := range []string{"b", "c", "d", "e"} {
		err := ins.Add(list, []byte(key), nil)
		if key == "c" || key == "e" {
			require.ErrorIs(t, err, ErrRecordExists)
		} else {
			require.NoError(t, err)
		}
	}
	it := list.NewIter(nil, nil)
	defer it.Close()
	var keys []string
	for ok, k, _ := it.First(); ok; ok, k, _ = it.Next() {
		keys = append(keys, string(k))
	}
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, keys)
}

func TestGrowArenaPreservesOrderedState(t *testing.T) {
	old := make([]byte, 4096)
	list := NewSkiplist(NewArena(old), bytes.Compare)
	for _, key := range []string{"a", "c", "e"} {
		require.NoError(t, list.Add([]byte(key), []byte(key+"!")))
	}
	require.Error(t, list.GrowArena(old))
	require.Error(t, list.GrowArena(old[:2048]))
	require.NoError(t, list.GrowArena(make([]byte, 8192)))
	clear(old) // a retained pointer into the old arena would now fail the oracle.
	require.NoError(t, list.Add([]byte("d"), []byte("d!")))
	require.ErrorIs(t, list.Add([]byte("c"), nil), ErrRecordExists)
	it := list.NewIter(nil, nil)
	defer it.Close()
	var keys []string
	for ok, k, v := it.First(); ok; ok, k, v = it.Next() {
		keys = append(keys, string(k))
		require.Equal(t, string(k)+"!", string(v))
	}
	require.Equal(t, []string{"a", "c", "d", "e"}, keys)
	keys = nil
	for ok, k, _ := it.Last(); ok; ok, k, _ = it.Prev() {
		keys = append(keys, string(k))
	}
	require.Equal(t, []string{"e", "d", "c", "a"}, keys)
}

func TestGrowArenaRejectsPoisonedAllocation(t *testing.T) {
	list := NewSkiplist(NewArena(make([]byte, 2048)), bytes.Compare)
	require.NoError(t, list.Add([]byte("kept"), nil))
	require.ErrorIs(t, list.Add(bytes.Repeat([]byte("z"), 4096), nil), ErrArenaFull)
	require.ErrorIs(t, list.GrowArena(make([]byte, 8192)), ErrArenaFull)
	it := list.NewIter(nil, nil)
	defer it.Close()
	ok, k, _ := it.First()
	require.True(t, ok)
	require.Equal(t, "kept", string(k))
}
