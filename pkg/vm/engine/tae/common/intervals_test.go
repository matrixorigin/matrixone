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

package common

import (
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestMerge1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	i1 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 1, End: 3},
			{Start: 5, End: 5},
			{Start: 11, End: 12},
		},
	}
	i2 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i2bk := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i1.TryMerge(*i2)
	res := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 1, End: 6},
			{Start: 8, End: 13},
		},
	}
	assert.True(t, i1.Equal(res))
	assert.True(t, i2.Equal(i2bk)) //i2 should remain the same
}

func TestMerge2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	i1 := &ClosedIntervals{
		Intervals: []*ClosedInterval{},
	}
	i2 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i2bk := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i1.TryMerge(*i2)
	res := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	assert.True(t, i1.Equal(res))
	assert.True(t, i2.Equal(i2bk)) //i2 should remain the same
}

func TestMerge3(t *testing.T) {
	defer leaktest.AfterTest(t)()
	i1 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i2 := &ClosedIntervals{
		Intervals: []*ClosedInterval{},
	}
	i2bk := &ClosedIntervals{
		Intervals: []*ClosedInterval{},
	}
	i1.TryMerge(*i2)
	res := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	assert.True(t, i1.Equal(res))
	assert.True(t, i2.Equal(i2bk)) //i2 should remain the same
}
func TestContains1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	i1 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i2 := &ClosedIntervals{
		Intervals: []*ClosedInterval{},
	}
	i2bk := &ClosedIntervals{
		Intervals: []*ClosedInterval{},
	}
	assert.True(t, i1.Contains(*i2))
	assert.True(t, i2.Equal(i2bk)) //i2 should remain the same
}
func TestContains2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	i1 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 4, End: 4},
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i2 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 10, End: 13},
		},
	}
	i2bk := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 2, End: 2},
			{Start: 10, End: 13},
		},
	}
	assert.True(t, i1.Contains(*i2))
	assert.True(t, i2.Equal(i2bk)) //i2 should remain the same
}
func TestContains3(t *testing.T) {
	defer leaktest.AfterTest(t)()
	i1 := &ClosedIntervals{
		Intervals: []*ClosedInterval{},
	}
	i2 := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	i2bk := &ClosedIntervals{
		Intervals: []*ClosedInterval{
			{Start: 6, End: 6},
			{Start: 8, End: 13},
		},
	}
	assert.False(t, i1.Contains(*i2))
	assert.True(t, i2.Equal(i2bk)) //i2 should remain the same
}

func TestNewBySlice(t *testing.T) {
	defer leaktest.AfterTest(t)()
	i := NewClosedIntervalsBySlice([]uint64{})
	t.Log(i)
	assert.Equal(t, 0, len(i.Intervals))
	i = NewClosedIntervalsBySlice([]uint64{1})
	t.Log(i)
	assert.Equal(t, 1, len(i.Intervals))
	i = NewClosedIntervalsBySlice([]uint64{1, 2})
	t.Log(i)
	assert.Equal(t, 1, len(i.Intervals))
	i = NewClosedIntervalsBySlice([]uint64{2, 1})
	t.Log(i)
	assert.Equal(t, 1, len(i.Intervals))
	i = NewClosedIntervalsBySlice([]uint64{1, 2, 3})
	t.Log(i)
	assert.Equal(t, 1, len(i.Intervals))
	i = NewClosedIntervalsBySlice([]uint64{1, 3, 2})
	t.Log(i)
	assert.Equal(t, 1, len(i.Intervals))
	i = NewClosedIntervalsBySlice([]uint64{1, 3, 4})
	t.Log(i)
	assert.Equal(t, 2, len(i.Intervals))
	i = NewClosedIntervalsBySlice([]uint64{1, 4, 3})
	t.Log(i)
	assert.Equal(t, 2, len(i.Intervals))
	i = NewClosedIntervalsBySlice([]uint64{1, 4, 3, 3, 2, 5, 3, 4, 6, 10, 7, 4567, 654, 67, 78, 67, 68})
	t.Log(i)
	assert.Equal(t, 6, len(i.Intervals))
}
