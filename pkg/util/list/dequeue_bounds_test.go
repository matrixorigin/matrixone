// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package list

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDrainOutOfBounds(t *testing.T) {
	for _, bounds := range [][2]int{{3, 5}, {4, 5}} {
		q := New[int]()
		for _, value := range []int{1, 2, 3} {
			q.PushBack(value)
		}
		require.Nil(t, q.Drain(bounds[0], bounds[1]))
		require.Equal(t, 3, q.Len())
		var values []int
		q.Iter(0, func(value int) bool { values = append(values, value); return true })
		require.Equal(t, []int{1, 2, 3}, values)
	}
}

func TestDrainNegativeStart(t *testing.T) {
	q := New[int]()
	for _, value := range []int{1, 2, 3} {
		q.PushBack(value)
	}
	drained := q.Drain(-2, 2)
	require.Equal(t, 2, drained.Len())
	require.Equal(t, 1, drained.PopFront().Value)
	require.Equal(t, 2, drained.PopFront().Value)
	require.Equal(t, 1, q.Len())
	require.Equal(t, 3, q.PopFront().Value)
}

func TestTruncateNegativeKeepsNoElements(t *testing.T) {
	q := New[int]()
	q.PushBack(1)
	q.Truncate(-1)
	require.Zero(t, q.Len())
	require.Nil(t, q.PopFront())
	q.PushBack(2)
	require.Equal(t, 2, q.PopFront().Value)
}
