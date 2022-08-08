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

package fileservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPin(t *testing.T) {
	l := NewLRU(1)

	p := Pin(42)
	l.Set(1, p, 1)
	_, ok := l.kv[1]
	assert.True(t, ok)

	l.Set(2, 42, 1)
	_, ok = l.kv[1]
	assert.True(t, ok)
	_, ok = l.kv[2]
	assert.False(t, ok)

	p.Unpin()
	l.Set(2, 42, 1)
	_, ok = l.kv[1]
	assert.False(t, ok)
	_, ok = l.kv[2]
	assert.True(t, ok)

	l.Set(3, Pin(42), 1)
	_, ok = l.kv[3]
	assert.True(t, ok)
	_, ok = l.kv[2]
	assert.False(t, ok)

}
