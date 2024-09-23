// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolAcquire(t *testing.T) {
	p := newDataPool(defaultDataSize)
	assert.NotNil(t, p)
	w := p.acquire(200)
	assert.Equal(t, 200, len(w.data))
	assert.Equal(t, defaultDataSize, cap(w.data))

	w = p.acquire(defaultDataSize + 200)
	assert.Equal(t, defaultDataSize+200, len(w.data))
	assert.Equal(t, defaultDataSize+200, cap(w.data))
}

func TestPoolRelease(t *testing.T) {
	originalMaxProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(originalMaxProcs)
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	t.Run("empty data", func(t *testing.T) {
		p := newDataPool(defaultDataSize)
		assert.NotNil(t, p)
		p.release(nil)
	})

	t.Run("small data", func(t *testing.T) {
		p := newDataPool(defaultDataSize)
		assert.NotNil(t, p)
		w := newWrappedData(make([]byte, 100), 0, nil)
		p.release(w)
		r := p.(*bytesPool).pool.Get().(*wrappedData)
		assert.NotNil(t, r)
		// assert.Equal(t, 100, len(r.data))
	})

	t.Run("large data", func(t *testing.T) {
		p := newDataPool(defaultDataSize)
		assert.NotNil(t, p)
		w := newWrappedData(make([]byte, defaultDataSize*2), 0, nil)
		p.release(w)
		r := p.(*bytesPool).pool.Get().(*wrappedData)
		assert.Equal(t, defaultDataSize, len(r.data))
	})
}
