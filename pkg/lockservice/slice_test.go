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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/stretchr/testify/assert"
)

func TestNewFixedSlicePool(t *testing.T) {
	reuse.RunReuseTests(func() {
		assert.Equal(t, 1, len(newFixedSlicePool(1).slices))
		assert.Equal(t, 2, len(newFixedSlicePool(2).slices))
		assert.Equal(t, 3, len(newFixedSlicePool(3).slices))
		assert.Equal(t, 3, len(newFixedSlicePool(4).slices))
		assert.Equal(t, 4, len(newFixedSlicePool(5).slices))
		assert.Equal(t, 4, len(newFixedSlicePool(6).slices))
		assert.Equal(t, 4, len(newFixedSlicePool(7).slices))
		assert.Equal(t, 4, len(newFixedSlicePool(8).slices))
	})
}

func TestAcquire(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		fs, err := fsp.acquire(1)
		assert.NoError(t, err)
		assert.Equal(t, 1, fs.cap())
		fs.close()

		fs, err = fsp.acquire(3)
		assert.NoError(t, err)
		assert.Equal(t, 4, fs.cap())
		fs.close()

		fs, err = fsp.acquire(5)
		assert.NoError(t, err)
		assert.Equal(t, 8, fs.cap())
		fs.close()

		fs, err = fsp.acquire(1024)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade))
		assert.Nil(t, fs)
	})
}

func TestRelease(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		fs, err := fsp.acquire(1)
		assert.NoError(t, err)
		err = fsp.release(fs)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), fsp.releaseV.Load())
		fs, err = fsp.acquire(1)
		assert.NoError(t, err)
		fs.values = make([][]byte, 1024)
		err = fsp.release(fs)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade))
	})
}

func TestFixedSliceAppend(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		fs, err := fsp.acquire(4)
		assert.NoError(t, err)
		defer fs.close()

		for i := byte(0); i < 4; i++ {
			fs.append([][]byte{{i}})
			assert.Equal(t, int(i+1), fs.len())
		}
	})
}

func TestFixedSliceJoin(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		fs1, err := fsp.acquire(4)
		assert.NoError(t, err)
		defer fs1.close()

		fs2, err := fsp.acquire(1)
		assert.NoError(t, err)
		defer fs2.close()
		fs2.append([][]byte{{1}})

		fs1.join(fs2, [][]byte{{2}})
		assert.Equal(t, 2, fs1.len())
		assert.Equal(t, [][]byte{{1}, {2}}, fs1.values[:fs1.len()])
	})
}

func TestFixedSliceRefAndUnRef(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		fs, err := fsp.acquire(1)
		assert.NoError(t, err)
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
	})
}

func TestFixedSliceIter(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		fs, err := fsp.acquire(4)
		assert.NoError(t, err)
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
	})
}

func TestCowSliceAppend(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		cs, err := newCowSlice(fsp, [][]byte{{1}, {2}, {3}})
		assert.NoError(t, err)
		defer cs.close()

		assert.Equal(t, 4, cs.fs.Load().(*fixedSlice).cap())
		assert.Equal(t, uint64(1), fsp.acquireV.Load())

		err = cs.append([][]byte{{4}})
		assert.NoError(t, err)
		assert.Equal(t, 4, cs.fs.Load().(*fixedSlice).cap())
		assert.Equal(t, uint64(1), fsp.acquireV.Load())

		assert.Equal(t, [][]byte{{1}, {2}, {3}, {4}},
			cs.fs.Load().(*fixedSlice).values[:cs.fs.Load().(*fixedSlice).len()])
	})
}

func TestCowSliceAppendWithCow(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		cs, err := newCowSlice(fsp, [][]byte{{1}})
		assert.NoError(t, err)
		defer cs.close()

		assert.Equal(t, 1, cs.fs.Load().(*fixedSlice).cap())
		assert.Equal(t, uint64(1), fsp.acquireV.Load())

		err = cs.append([][]byte{{2}})
		assert.NoError(t, err)
		assert.Equal(t, 2, cs.fs.Load().(*fixedSlice).cap())
		assert.Equal(t, uint64(2), fsp.acquireV.Load())

		assert.Equal(t, [][]byte{{1}, {2}},
			cs.fs.Load().(*fixedSlice).values[:cs.fs.Load().(*fixedSlice).len()])
	})
}

func TestCowSliceRead(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		cs, err := newCowSlice(fsp, [][]byte{{1}})
		assert.NoError(t, err)

		s := cs.slice()
		assert.Equal(t, [][]byte{{1}}, s.values[:s.len()])

		err = cs.append([][]byte{{2}})
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), fsp.releaseV.Load())

		assert.Equal(t, [][]byte{{1}}, s.values[:s.len()])
		s.unref()
		assert.Equal(t, uint64(1), fsp.releaseV.Load())

		cs.close()
		assert.Equal(t, uint64(2), fsp.releaseV.Load())
	})
}

func TestCowSliceAppendConcurrentWithSliceGetNew(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		cs, err := newCowSlice(fsp, [][]byte{{1}})
		assert.NoError(t, err)
		defer cs.close()

		var s *fixedSlice
		n := 0
		cs.hack.replace = func() {
			if s == nil {
				s = cs.slice()
			}
			n++
		}
		err = cs.append([][]byte{{2}})
		assert.NoError(t, err)
		assert.Equal(t, 2, n)
		assert.Equal(t, uint64(1), fsp.releaseV.Load())
	})
}

func TestCowSliceAppendConcurrentWithSliceGetOld(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		cs, err := newCowSlice(fsp, [][]byte{{1}})
		assert.NoError(t, err)
		defer cs.close()

		old := cs.fs.Load().(*fixedSlice)
		n := 0
		cs.hack.replace = func() {
			if n == 0 {
				old.ref()
				cs.v.Add(1)
			}
			n++
		}
		err = cs.append([][]byte{{2}})
		assert.NoError(t, err)
		assert.Equal(t, 2, n)
		assert.Equal(t, uint64(0), fsp.releaseV.Load())

		old.unref()
		assert.Equal(t, uint64(1), fsp.releaseV.Load())
	})
}

func TestCowSliceSliceReadConcurrentWithAppend(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		cs, err := newCowSlice(fsp, [][]byte{{1}})
		assert.NoError(t, err)
		defer cs.close()

		n := 0
		cs.hack.slice = func() {
			if n == 0 {
				err = cs.append([][]byte{{2}})
				assert.NoError(t, err)
			}
			n++
		}
		s := cs.slice()
		assert.Equal(t, [][]byte{{1}, {2}}, s.values[:s.len()])
		s.unref()
		assert.Equal(t, uint64(1), fsp.releaseV.Load())

		s.close()
		assert.Equal(t, uint64(2), fsp.releaseV.Load())
	})
}
