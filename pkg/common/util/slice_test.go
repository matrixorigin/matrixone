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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoveIfInPlace(t *testing.T) {
	var nilSlice []int
	require.Nil(t, RemoveIf(nilSlice, func(int) bool { t.Fatal("predicate called on nil"); return false }))
	empty := make([]int, 0, 2)
	require.NotNil(t, RemoveIf(empty, func(int) bool { t.Fatal("predicate called on empty"); return false }))
	input := []int{1, 2, 3, 4, 5}
	seen := make([]int, 0, len(input))
	got := RemoveIf(input, func(x int) bool { seen = append(seen, x); return x%2 == 0 })
	require.Equal(t, []int{1, 2, 3, 4, 5}, seen)
	require.Equal(t, []int{1, 3, 5}, got)
	require.Equal(t, []int{1, 3, 5, 4, 5}, input)
	require.Equal(t, &input[0], &got[0])
}
