// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package list

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPushBack(t *testing.T) {
	q := New[int]()
	q.PushBack(1)
	assert.Equal(t, 1, q.Len())
}

func TestPopFront(t *testing.T) {
	q := New[int]()
	q.PushBack(1)
	q.PushBack(2)
	v := q.PopFront()
	assert.Equal(t, 1, v.Value)
	assert.Equal(t, 2, q.MustFront().Value)
}

func TestPopBack(t *testing.T) {
	q := New[int]()
	q.PushBack(1)
	q.PushBack(2)
	v := q.PopBack()
	assert.Equal(t, 2, v.Value)
	assert.Equal(t, 1, q.MustFront().Value)
	assert.Equal(t, 1, q.MustBack().Value)
}

func TestRemove(t *testing.T) {
	q := New[int]()
	q.Remove(q.PushBack(1))
	assert.Equal(t, 0, q.Len())
}

func TestIter(t *testing.T) {
	q := New[int]()
	q.PushBack(0)
	q.PushBack(1)
	q.PushBack(2)

	var values []int
	q.Iter(0, func(i int) bool {
		values = append(values, i)
		return true
	})
	assert.Equal(t, []int{0, 1, 2}, values)

	values = values[:0]
	q.Iter(0, func(i int) bool {
		values = append(values, i)
		return false
	})
	assert.Equal(t, []int{0}, values)

	values = values[:0]
	q.Iter(1, func(i int) bool {
		values = append(values, i)
		return true
	})
	assert.Equal(t, []int{1, 2}, values)
}

func TestInsertBefore(t *testing.T) {
	q := New[int]()
	q.PushBack(0)
	e := q.PushBack(2)
	q.PushBack(3)
	q.InsertBefore(1, e)

	v := -1
	q.Iter(1, func(i int) bool {
		v = i
		return false
	})
	assert.Equal(t, 1, v)
}

func TestInsertAfter(t *testing.T) {
	q := New[int]()
	e := q.PushBack(0)
	q.PushBack(2)
	q.PushBack(3)
	q.InsertAfter(1, e)

	v := -1
	q.Iter(1, func(i int) bool {
		v = i
		return false
	})
	assert.Equal(t, 1, v)
}

func TestMoveToFront(t *testing.T) {
	q := New[int]()
	q.PushBack(0)
	q.PushBack(1)
	e := q.PushBack(2)
	q.MoveToFront(e)

	var values []int
	q.Iter(0, func(i int) bool {
		values = append(values, i)
		return true
	})
	assert.Equal(t, []int{2, 0, 1}, values)
}

func TestMoveToBack(t *testing.T) {
	q := New[int]()
	e := q.PushBack(0)
	q.PushBack(1)
	q.PushBack(2)
	q.MoveToBack(e)

	var values []int
	q.Iter(0, func(i int) bool {
		values = append(values, i)
		return true
	})
	assert.Equal(t, []int{1, 2, 0}, values)
}

func TestMoveBefore(t *testing.T) {
	q := New[int]()
	e0 := q.PushBack(0)
	e1 := q.PushBack(1)
	q.PushBack(2)

	q.MoveBefore(e1, e0)

	var values []int
	q.Iter(0, func(i int) bool {
		values = append(values, i)
		return true
	})
	assert.Equal(t, []int{1, 0, 2}, values)
}

func TestMoveAfter(t *testing.T) {
	q := New[int]()
	e0 := q.PushBack(0)
	e1 := q.PushBack(1)
	q.PushBack(2)

	q.MoveAfter(e0, e1)

	var values []int
	q.Iter(0, func(i int) bool {
		values = append(values, i)
		return true
	})
	assert.Equal(t, []int{1, 0, 2}, values)
}

func TestTruncate(t *testing.T) {
	q := New[int]()
	for i := 0; i < 10; i++ {
		q.PushBack(i)
	}

	q.Truncate(q.Len())
	assert.Equal(t, 10, q.Len())

	q.Truncate(q.Len() + 1)
	assert.Equal(t, 10, q.Len())

	q.Truncate(1)
	assert.Equal(t, 1, q.Len())
	v, ok := q.Front()
	assert.True(t, ok)
	i := 0
	for e := v; e != nil; e = e.Next() {
		assert.Equal(t, i, e.Value)
		i++
	}
	v, _ = q.Back()
	for e := v; e != nil; e = e.Prev() {
		i--
		assert.Equal(t, i, e.Value)
	}

	q.Truncate(0)
	assert.Equal(t, 0, q.Len())
	_, ok = q.Front()
	assert.False(t, ok)
	_, ok = q.Back()
	assert.False(t, ok)
}

func TestDrain(t *testing.T) {
	// case 1: drain 5,10 from 0-9
	q := New[int]()
	for i := 0; i < 10; i++ {
		q.PushBack(i)
	}
	drained := q.Drain(5, 10)

	assert.Equal(t, 5, q.Len())
	v, ok := q.Front()
	assert.True(t, ok)
	i := 0
	for e := v; e != nil; e = e.Next() {
		assert.Equal(t, i, e.Value)
		i++
	}
	v, _ = q.Back()
	for e := v; e != nil; e = e.Prev() {
		i--
		assert.Equal(t, i, e.Value)
	}

	v, ok = drained.Front()
	assert.Equal(t, 5, drained.Len())
	assert.True(t, ok)
	i = 5
	for e := v; e != nil; e = e.Next() {
		assert.Equal(t, i, e.Value)
		i++
	}
	v, _ = drained.Back()
	for e := v; e != nil; e = e.Prev() {
		i--
		assert.Equal(t, i, e.Value)
	}

	// case 2: drain 5,9 from 0-9
	q.Clear()
	for i := 0; i < 10; i++ {
		q.PushBack(i)
	}
	drained = q.Drain(5, 9)

	assert.Equal(t, 6, q.Len())

	i = 0
	values := []int{0, 1, 2, 3, 4, 9}
	v, ok = q.Front()
	assert.True(t, ok)
	for e := v; e != nil; e = e.Next() {
		assert.Equal(t, values[i], e.Value)
		i++
	}
	v, _ = q.Back()
	for e := v; e != nil; e = e.Prev() {
		i--
		assert.Equal(t, values[i], e.Value)
	}

	v, ok = drained.Front()
	assert.Equal(t, 4, drained.Len())
	assert.True(t, ok)
	i = 5
	for e := v; e != nil; e = e.Next() {
		assert.Equal(t, i, e.Value)
		i++
	}
	v, _ = drained.Back()
	for e := v; e != nil; e = e.Prev() {
		i--
		assert.Equal(t, i, e.Value)
	}

	// case 3: drain 0,5 from 0-9
	q.Clear()
	for i := 0; i < 10; i++ {
		q.PushBack(i)
	}
	drained = q.Drain(0, 5)

	assert.Equal(t, 5, q.Len())
	i = 5
	v, ok = q.Front()
	assert.True(t, ok)
	for e := v; e != nil; e = e.Next() {
		assert.Equal(t, i, e.Value)
		i++
	}
	v, _ = q.Back()
	for e := v; e != nil; e = e.Prev() {
		i--
		assert.Equal(t, i, e.Value)
	}

	v, ok = drained.Front()
	assert.Equal(t, 5, drained.Len())
	assert.True(t, ok)
	i = 0
	for e := v; e != nil; e = e.Next() {
		assert.Equal(t, i, e.Value)
		i++
	}
	v, _ = drained.Back()
	for e := v; e != nil; e = e.Prev() {
		i--
		assert.Equal(t, i, e.Value)
	}

	// case 3: drain 0,9 from 0-9
	q.Clear()
	for i := 0; i < 10; i++ {
		q.PushBack(i)
	}
	drained = q.Drain(0, 10)
	assert.Equal(t, 0, q.Len())
	_, ok = q.Front()
	assert.False(t, ok)
	_, ok = q.Back()
	assert.False(t, ok)

	assert.Equal(t, 10, drained.Len())
	v, ok = drained.Front()
	assert.True(t, ok)
	i = 0
	for e := v; e != nil; e = e.Next() {
		assert.Equal(t, i, e.Value)
		i++
	}
	v, _ = drained.Back()
	for e := v; e != nil; e = e.Prev() {
		i--
		assert.Equal(t, i, e.Value)
	}
}
