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

package lockservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFixedSlicePool(t *testing.T) {
	assert.Equal(t, 1, len(newFixedSlicePool(1).slices))
	assert.Equal(t, 2, len(newFixedSlicePool(2).slices))
	assert.Equal(t, 3, len(newFixedSlicePool(3).slices))
	assert.Equal(t, 3, len(newFixedSlicePool(4).slices))
	assert.Equal(t, 4, len(newFixedSlicePool(5).slices))
	assert.Equal(t, 4, len(newFixedSlicePool(6).slices))
	assert.Equal(t, 4, len(newFixedSlicePool(7).slices))
	assert.Equal(t, 4, len(newFixedSlicePool(8).slices))
}

func TestAcquire(t *testing.T) {
	fsp := newFixedSlicePool(16)
	fs := fsp.acquire(1)
	assert.Equal(t, 1, fs.cap())
	fs.close()

	fs = fsp.acquire(3)
	assert.Equal(t, 4, fs.cap())
	fs.close()

	fs = fsp.acquire(5)
	assert.Equal(t, 8, fs.cap())
	fs.close()

	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	fsp.acquire(1024)
}

func TestRelease(t *testing.T) {
	fsp := newFixedSlicePool(16)
	fs := fsp.acquire(1)
	fsp.release(fs)
	assert.Equal(t, uint64(1), fsp.releaseV.Load())

	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	fs = fsp.acquire(1)
	fs.values = make([][]byte, 1024)
	fsp.release(fs)
}

func TestFixedSliceAppend(t *testing.T) {
	fsp := newFixedSlicePool(16)
	fs := fsp.acquire(4)
	defer fs.close()

	for i := byte(0); i < 4; i++ {
		fs.append([][]byte{{i}})
		assert.Equal(t, int(i+1), fs.len())
	}
}

func TestFixedSliceJoin(t *testing.T) {
	fsp := newFixedSlicePool(16)
	fs1 := fsp.acquire(4)
	defer fs1.close()

	fs2 := fsp.acquire(1)
	defer fs2.close()
	fs2.append([][]byte{{1}})

	fs1.join(fs2, [][]byte{{2}})
	assert.Equal(t, 2, fs1.len())
	assert.Equal(t, [][]byte{{1}, {2}}, fs1.values[:fs1.len()])
}

func TestFxiedSliceRefAndUnRef(t *testing.T) {
	fsp := newFixedSlicePool(16)
	fs := fsp.acquire(1)
	assert.Equal(t, int32(1), fs.atomic.ref.Load())
	fs.ref()
	assert.Equal(t, int32(2), fs.atomic.ref.Load())
	fs.append([][]byte{{1}})

	fs.unref()
	assert.Equal(t, int32(1), fs.atomic.ref.Load())
	assert.Equal(t, 1, fs.len())

	fs.unref()
	assert.Equal(t, int32(0), fs.atomic.ref.Load())
	assert.Equal(t, 0, fs.len())

	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()
	fs.unref()
}

func TestFxiedSliceIter(t *testing.T) {
	fsp := newFixedSlicePool(16)
	fs := fsp.acquire(4)
	defer fs.close()

	for i := byte(0); i < 4; i++ {
		fs.append([][]byte{{i}})
	}

	var values [][]byte
	fs.iter(func(b []byte) bool {
		values = append(values, b)
		return true
	})
	assert.Equal(t, fs.values[:fs.len()], values)

	values = values[:0]
	fs.iter(func(b []byte) bool {
		values = append(values, b)
		return false
	})
	assert.Equal(t, fs.values[:1], values)
}

func TestCowSliceAppend(t *testing.T) {
	fsp := newFixedSlicePool(16)
	cs := newCowSlice(fsp, [][]byte{{1}, {2}, {3}})
	assert.Equal(t, 4, cs.fs.Load().(*fixedSlice).cap())
	assert.Equal(t, uint64(1), fsp.acquireV.Load())

	cs.append([][]byte{{4}})
	assert.Equal(t, 4, cs.fs.Load().(*fixedSlice).cap())
	assert.Equal(t, uint64(1), fsp.acquireV.Load())

	assert.Equal(t, [][]byte{{1}, {2}, {3}, {4}},
		cs.fs.Load().(*fixedSlice).values[:cs.fs.Load().(*fixedSlice).len()])
}

func TestCowSliceAppendWithCow(t *testing.T) {
	fsp := newFixedSlicePool(16)
	cs := newCowSlice(fsp, [][]byte{{1}})
	assert.Equal(t, 1, cs.fs.Load().(*fixedSlice).cap())
	assert.Equal(t, uint64(1), fsp.acquireV.Load())

	cs.append([][]byte{{2}})
	assert.Equal(t, 2, cs.fs.Load().(*fixedSlice).cap())
	assert.Equal(t, uint64(2), fsp.acquireV.Load())

	assert.Equal(t, [][]byte{{1}, {2}},
		cs.fs.Load().(*fixedSlice).values[:cs.fs.Load().(*fixedSlice).len()])
}

func TestCowSliceRead(t *testing.T) {
	fsp := newFixedSlicePool(16)
	cs := newCowSlice(fsp, [][]byte{{1}})

	s := cs.slice()
	assert.Equal(t, [][]byte{{1}}, s.values[:s.len()])

	cs.append([][]byte{{2}})
	assert.Equal(t, uint64(0), fsp.releaseV.Load())

	assert.Equal(t, [][]byte{{1}}, s.values[:s.len()])
	s.unref()
	assert.Equal(t, uint64(1), fsp.releaseV.Load())

	cs.close()
	assert.Equal(t, uint64(2), fsp.releaseV.Load())
}

func TestCowSliceAppendConcurrentWithSliceGetNew(t *testing.T) {
	fsp := newFixedSlicePool(16)
	cs := newCowSlice(fsp, [][]byte{{1}})
	var s *fixedSlice
	n := 0
	cs.hack.replace = func() {
		if s == nil {
			s = cs.slice()
		}
		n++
	}
	cs.append([][]byte{{2}})
	assert.Equal(t, 2, n)
	assert.Equal(t, uint64(1), fsp.releaseV.Load())
}

func TestCowSliceAppendConcurrentWithSliceGetOld(t *testing.T) {
	fsp := newFixedSlicePool(16)
	cs := newCowSlice(fsp, [][]byte{{1}})
	old := cs.fs.Load().(*fixedSlice)
	n := 0
	cs.hack.replace = func() {
		if n == 0 {
			old.ref()
			cs.v.Add(1)
		}
		n++
	}
	cs.append([][]byte{{2}})
	assert.Equal(t, 2, n)
	assert.Equal(t, uint64(0), fsp.releaseV.Load())

	old.unref()
	assert.Equal(t, uint64(1), fsp.releaseV.Load())
}

func TestCowSliceSliceReadConcurrentWithAppend(t *testing.T) {
	fsp := newFixedSlicePool(16)
	cs := newCowSlice(fsp, [][]byte{{1}})

	n := 0
	cs.hack.slice = func() {
		if n == 0 {
			cs.append([][]byte{{2}})
		}
		n++
	}
	s := cs.slice()
	assert.Equal(t, [][]byte{{1}, {2}}, s.values[:s.len()])
	s.unref()
	assert.Equal(t, uint64(1), fsp.releaseV.Load())

	s.close()
	assert.Equal(t, uint64(2), fsp.releaseV.Load())
}
