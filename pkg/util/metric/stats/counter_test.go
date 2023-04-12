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

package stats

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
)

func TestCounterLoad(t *testing.T) {
	c := Counter{}

	c.Add(1)
	assert.Equal(t, int64(1), c.Load())

	c.Add(1)
	assert.Equal(t, int64(2), c.Load())

	c.Add(10)
	assert.Equal(t, int64(12), c.Load())
}

func TestCounterSwapW(t *testing.T) {
	c := Counter{}

	c.Add(1)
	assert.Equal(t, int64(1), c.Load())

	c.Add(1)
	assert.Equal(t, int64(2), c.Load())

	c.Add(2)
	assert.Equal(t, int64(4), c.SwapW(0))

	assert.Equal(t, int64(0), c.SwapW(0))

	c.Add(2)
	assert.Equal(t, int64(6), c.Load())
}

func TestCounterLoadW(t *testing.T) {
	c := Counter{}

	c.Add(1)
	assert.Equal(t, int64(1), c.Load())
	assert.Equal(t, int64(1), c.LoadW())

	c.Add(1)
	assert.Equal(t, int64(2), c.Load())
	assert.Equal(t, int64(2), c.LoadW())

	assert.Equal(t, int64(2), c.SwapW(0))

	assert.Equal(t, int64(2), c.Load())
	assert.Equal(t, int64(0), c.LoadW())

	c.Add(1)
	assert.Equal(t, int64(3), c.Load())
	assert.Equal(t, int64(1), c.LoadW())
}

func BenchmarkAdd(b *testing.B) {
	b.Run("atomic.Int64", func(b *testing.B) {
		c := atomic.Int64{}
		for i := 0; i < b.N; i++ {
			c.Add(int64(i))
		}
	})

	b.Run("Counter", func(b *testing.B) {
		c := Counter{}
		for i := 0; i < b.N; i++ {
			c.Add(int64(i))
		}
	})
}

func BenchmarkAddLoad(b *testing.B) {
	b.Run("atomic.Int64", func(b *testing.B) {
		c := atomic.Int64{}
		for i := 0; i < b.N; i++ {
			c.Add(1)
			if int64(i+1) != c.Load() {
				panic("Load() did not return global sum")
			}
		}
	})

	b.Run("Counter", func(b *testing.B) {
		c := Counter{}
		for i := 0; i < b.N; i++ {
			c.Add(1)
			if int64(i+1) != c.Load() {
				panic("Load() did not return global sum")
			}
		}
	})
}
