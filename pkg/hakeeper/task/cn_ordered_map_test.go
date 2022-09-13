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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOrderedMap(t *testing.T) {
	orderedMap := NewOrderedMap()
	orderedMap.Set("a", 100)
	orderedMap.Set("b", 110)
	orderedMap.Set("c", 90)
	orderedMap.Set("d", 80)
	orderedMap.Set("e", 120)
	orderedMap.Set("f", 50)
	assert.Equal(t, 6, orderedMap.Len())
	assert.Equal(t, "f", orderedMap.Min())

	orderedMap.Set("f", 150)
	assert.Equal(t, "d", orderedMap.Min())

	orderedMap.Set("b", 70)
	assert.Equal(t, "b", orderedMap.Min())

	orderedMap.Set("b", 100)
	for i := 0; i < 100; i++ {
		orderedMap.Inc("g")
		assert.Equal(t, uint32(i+1), orderedMap.Get("g"))
	}
	assert.Equal(t, "d", orderedMap.Min())
}
