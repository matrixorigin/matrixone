// Copyright 2023 Matrix Origin
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

package interval

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntervalTree(t *testing.T) {
	interval1 := Interval{low: 15, high: 20}
	interval2 := Interval{low: 10, high: 30}
	interval3 := Interval{low: 17, high: 19}
	interval4 := Interval{low: 5, high: 20}
	interval5 := Interval{low: 12, high: 15}
	interval6 := Interval{low: 30, high: 40}

	tree := NewIntervalTree()

	// Insert intervals
	tree.Insert(interval1)
	tree.Insert(interval2)
	tree.Insert(interval3)
	tree.Insert(interval4)
	tree.Insert(interval5)
	tree.Insert(interval6)

	// Test Contains()
	assert.True(t, tree.Contains(interval1))
	assert.True(t, tree.Contains(interval2))
	assert.True(t, tree.Contains(interval3))
	assert.True(t, tree.Contains(interval4))
	assert.True(t, tree.Contains(interval5))
	assert.True(t, tree.Contains(interval6))

	// Test Remove()
	tree.Remove(interval3)
	assert.False(t, tree.Contains(interval3))
	tree.Remove(interval5)
	assert.False(t, tree.Contains(interval5))

	// Test additional cases
	assert.False(t, tree.Contains(Interval{low: 7, high: 9}))
	assert.False(t, tree.Contains(Interval{low: 25, high: 28}))
}
