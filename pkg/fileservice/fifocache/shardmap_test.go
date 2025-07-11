// Copyright 2024 Matrix Origin
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

package fifocache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardMap(t *testing.T) {

	m := NewShardMap[int, string](ShardInt[int])
	ok := m.Set(1, "1", func(v string) {
	})
	assert.Equal(t, ok, true)
	ok = m.Set(1, "1", func(v string) {
	})
	assert.Equal(t, ok, false)

	v, ok := m.Get(1, func(v string) {
	})
	assert.Equal(t, ok, true)
	assert.Equal(t, v, "1")

	_, ok = m.GetAndDelete(0, func(v string) {
	})
	assert.Equal(t, ok, false)

	_, ok = m.GetAndDelete(1, func(v string) {
	})
	assert.Equal(t, ok, true)
}
