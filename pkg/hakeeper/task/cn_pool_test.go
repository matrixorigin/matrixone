// Copyright 2022 Matrix Origin
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

package task

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderedMap(t *testing.T) {
	orderedMap := newCNPool()
	orderedMap.set(cnStore{uuid: "a"}, 100)
	orderedMap.set(cnStore{uuid: "b"}, 110)
	orderedMap.set(cnStore{uuid: "c"}, 90)
	orderedMap.set(cnStore{uuid: "d"}, 80)
	orderedMap.set(cnStore{uuid: "e"}, 120)
	orderedMap.set(cnStore{uuid: "f"}, 50)
	assert.Equal(t, 6, len(orderedMap.freq))
	assert.True(t, len(orderedMap.freq) == len(orderedMap.sortedCN))
	assert.Equal(t, "f", orderedMap.min().uuid)

	orderedMap.set(cnStore{uuid: "f"}, 150)
	assert.Equal(t, "d", orderedMap.min().uuid)

	orderedMap.set(cnStore{uuid: "b"}, 70)
	assert.Equal(t, "b", orderedMap.min().uuid)

	orderedMap.set(cnStore{uuid: "b"}, 100)
	for i := 0; i < 100; i++ {
		heap.Push(orderedMap, cnStore{uuid: "g"})
		assert.Equal(t, uint32(i+1), orderedMap.getFreq("g"))
	}
	assert.Equal(t, "d", orderedMap.min().uuid)
	assert.Equal(t, 7, len(orderedMap.freq))
	assert.True(t, len(orderedMap.freq) == len(orderedMap.sortedCN))
}
