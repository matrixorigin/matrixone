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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderedMap(t *testing.T) {
	orderedMap := newOrderedMap(nil)
	orderedMap.set("a", 100)
	orderedMap.set("b", 110)
	orderedMap.set("c", 90)
	orderedMap.set("d", 80)
	orderedMap.set("e", 120)
	orderedMap.set("f", 50)
	assert.Equal(t, 6, len(orderedMap.m))
	assert.True(t, len(orderedMap.m) == len(orderedMap.orderedKeys))
	assert.Equal(t, "f", orderedMap.min())

	orderedMap.set("f", 150)
	assert.Equal(t, "d", orderedMap.min())

	orderedMap.set("b", 70)
	assert.Equal(t, "b", orderedMap.min())

	orderedMap.set("b", 100)
	for i := 0; i < 100; i++ {
		orderedMap.inc("g")
		assert.Equal(t, uint32(i+1), orderedMap.get("g"))
	}
	assert.Equal(t, "d", orderedMap.min())
	assert.Equal(t, 7, len(orderedMap.m))
	assert.True(t, len(orderedMap.m) == len(orderedMap.orderedKeys))
}
