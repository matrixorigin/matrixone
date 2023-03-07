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

package morpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheAdd(t *testing.T) {
	c := newCache()
	defer c.Close()
	c.Add(newTestMessage(1))
	v, err := c.Len()
	assert.NoError(t, err)
	assert.Equal(t, 1, v)

	c.Close()
	v, err = c.Len()
	assert.Error(t, err)
	assert.Equal(t, v, 0)
}

func TestCachePop(t *testing.T) {
	c := newCache()
	defer c.Close()
	c.Add(newTestMessage(1))
	c.Add(newTestMessage(2))

	v, ok, err := c.Pop()
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, newTestMessage(1), v)

	v, ok, err = c.Pop()
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, newTestMessage(2), v)

	v, ok, err = c.Pop()
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, nil, v)

	c.Close()
	v, ok, err = c.Pop()
	assert.Error(t, err)
	assert.False(t, ok)
	assert.Equal(t, nil, v)
}
