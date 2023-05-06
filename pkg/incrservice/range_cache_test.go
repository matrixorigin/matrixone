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

package incrservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRangeCount(t *testing.T) {
	r := &ranges{step: 1, values: []uint64{1, 2, 2, 3, 3, 4}}
	assert.Equal(t, 3, r.rangeCount())

	r = &ranges{step: 2, values: []uint64{1, 3, 3, 5, 5, 7}}
	assert.Equal(t, 3, r.rangeCount())
}

func TestRangeEmpty(t *testing.T) {
	r := &ranges{}
	assert.True(t, r.empty())

	r = &ranges{step: 2, values: []uint64{3, 3, 5, 5, 7, 7}}
	assert.True(t, r.empty())

	r = &ranges{step: 2, values: []uint64{1, 3}}
	assert.False(t, r.empty())

	r = &ranges{step: 2, values: []uint64{3, 3, 5, 5, 7, 9}}
	assert.False(t, r.empty())
}

func TestRangeNext(t *testing.T) {
	r := &ranges{}
	assert.Equal(t, uint64(0), r.next())

	r = &ranges{step: 1, values: []uint64{2, 2}}
	assert.Equal(t, uint64(0), r.next())

	r = &ranges{step: 1, values: []uint64{1, 2}}
	assert.Equal(t, uint64(1), r.next())
	assert.Equal(t, 0, len(r.values))

	r = &ranges{step: 1, values: []uint64{2, 2, 2, 3}}
	assert.Equal(t, uint64(2), r.next())
	assert.Equal(t, 0, len(r.values))
}

func TestRangeLeft(t *testing.T) {
	r := &ranges{}
	assert.Equal(t, 0, r.left())

	r = &ranges{step: 2, values: []uint64{3, 3, 5, 5, 7, 7}}
	assert.Equal(t, 0, r.left())

	r = &ranges{step: 2, values: []uint64{1, 3}}
	assert.Equal(t, 1, r.left())

	r = &ranges{step: 2, values: []uint64{3, 3, 5, 7, 7, 9}}
	assert.Equal(t, 2, r.left())
}

func TestRangeAdd(t *testing.T) {
	r := &ranges{step: 1}
	r.add(1, 2)
	assert.Equal(t, 1, r.left())
	assert.Equal(t, 2, len(r.values))

	r.add(2, 3)
	assert.Equal(t, 2, r.left())
	assert.Equal(t, 4, len(r.values))

	defer func() {
		if err := recover(); err != nil {
			assert.Equal(t, "invalid range", err)
			return
		}
		t.Fail()
	}()
	r.add(1, 2)
}
