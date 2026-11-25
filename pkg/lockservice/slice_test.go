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
	"sync"
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
		fs.unref()

		fs, err = fsp.acquire(3)
		assert.NoError(t, err)
		assert.Equal(t, 4, fs.cap())
		fs.unref()

		fs, err = fsp.acquire(5)
		assert.NoError(t, err)
		assert.Equal(t, 8, fs.cap())
		fs.unref()

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
		fs.unref()
		assert.Equal(t, uint64(1), fsp.releaseV.Load())

		retired := &fixedSlice{values: make([][]byte, 1024), sp: fsp}
		err = fsp.release(retired)
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrLockNeedUpgrade))
	})
}

func TestFixedSliceAppend(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		fs, err := fsp.acquire(4)
		assert.NoError(t, err)
		defer fs.unref()

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
		defer fs1.unref()

		fs2, err := fsp.acquire(1)
		assert.NoError(t, err)
		defer fs2.unref()
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
		assert.True(t, fs.tryRef())
		assert.Equal(t, int32(2), fs.atomic.ref.Load())
		fs.append([][]byte{{1}})

		fs.unref()
		assert.Equal(t, int32(1), fs.atomic.ref.Load())
		assert.Equal(t, 1, fs.len())

		fs.unref()
		assert.Equal(t, int32(0), fs.atomic.ref.Load())
		assert.Equal(t, 0, fs.len())
		assert.False(t, fs.tryRef())

		defer func() {
			if err := recover(); err != nil {
				return
			}
			assert.Fail(t, "must panic")
		}()
		fs.unref()
	})
}

func TestFixedSliceInitRefRequiresRetiredSlice(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		fs, err := fsp.acquire(1)
		assert.NoError(t, err)
		assert.Panics(t, fs.initRef)
		fs.unref()
	})
}

func TestFixedSlicePoolRejectsLiveSlice(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		fs := &fixedSlice{values: make([][]byte, 1), sp: fsp}
		fs.atomic.ref.Store(1)
		defer fs.unref()
		fsp.slices[0] = sync.Pool{New: func() any { return fs }}
		assert.Panics(t, func() {
			_, _ = fsp.acquire(1)
		})
	})
}

func TestFixedSliceIter(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		fs, err := fsp.acquire(4)
		assert.NoError(t, err)
		defer fs.unref()

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
		defer func() {
			cs.hack.replace = nil
			cs.close()
		}()

		var s *fixedSlice
		defer func() {
			if s != nil {
				s.unref()
			}
		}()
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
		defer func() {
			cs.hack.replace = nil
			cs.close()
		}()

		old := cs.fs.Load().(*fixedSlice)
		n := 0
		cs.hack.replace = func() {
			if n == 0 {
				assert.True(t, old.tryRef())
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

		cs.hack.slice = nil
		cs.close()
		assert.Equal(t, uint64(2), fsp.releaseV.Load())
	})
}

func TestCowSliceSliceRetriesAfterVersionChange(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		cs, err := newCowSlice(fsp, [][]byte{{1}})
		assert.NoError(t, err)

		old := cs.mustGet()
		var retained *fixedSlice
		n := 0
		cs.hack.slice = func() {
			if n == 0 {
				assert.True(t, old.tryRef())
				retained = old
				err = cs.append([][]byte{{2}})
				assert.NoError(t, err)
			}
			n++
		}

		s := cs.slice()
		assert.Equal(t, [][]byte{{1}, {2}}, s.all())
		s.unref()
		assert.Equal(t, uint64(0), fsp.releaseV.Load())

		retained.unref()
		assert.Equal(t, uint64(1), fsp.releaseV.Load())
		cs.hack.slice = nil
		cs.close()
		assert.Equal(t, uint64(2), fsp.releaseV.Load())
	})
}

func TestCowSliceStaleReaderDoesNotAffectReusedOwner(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		cs, err := newCowSlice(fsp, [][]byte{{1}})
		assert.NoError(t, err)

		old := cs.mustGet()
		n := 0
		cs.hack.slice = func() {
			if n == 0 {
				err = cs.append([][]byte{{2}})
				assert.NoError(t, err)
				// Remove the retired entry if sync.Pool retained it. If the pool
				// dropped it, Get creates an unrelated zero-ref object instead.
				_ = fsp.slices[0].Get()
				old.initRef()
				old.append([][]byte{{9}})
			}
			n++
		}

		s := cs.slice()
		assert.Equal(t, [][]byte{{1}, {2}}, s.all())
		s.unref()
		assert.Equal(t, int32(1), old.atomic.ref.Load())
		assert.Equal(t, [][]byte{{9}}, old.all())

		old.unref()
		cs.hack.slice = nil
		cs.close()
		assert.Equal(t, uint64(3), fsp.releaseV.Load())
	})
}

func TestCowSliceMultipleReadersKeepRetiredSliceAlive(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		cs, err := newCowSlice(fsp, [][]byte{{1}})
		assert.NoError(t, err)

		const readers = 8
		ready := make(chan struct{}, readers)
		release := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(readers)
		for i := 0; i < readers; i++ {
			go func() {
				defer wg.Done()
				s := cs.slice()
				ready <- struct{}{}
				<-release
				s.unref()
			}()
		}
		for i := 0; i < readers; i++ {
			<-ready
		}

		err = cs.append([][]byte{{2}})
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), fsp.releaseV.Load())
		close(release)
		wg.Wait()
		assert.Equal(t, uint64(1), fsp.releaseV.Load())

		cs.close()
		assert.Equal(t, uint64(2), fsp.releaseV.Load())
	})
}

func TestCowSliceCloseKeepsActiveReaderAlive(t *testing.T) {
	reuse.RunReuseTests(func() {
		fsp := newFixedSlicePool(16)
		cs, err := newCowSlice(fsp, [][]byte{{1}})
		assert.NoError(t, err)

		s := cs.slice()
		cs.close()
		assert.Equal(t, uint64(0), fsp.releaseV.Load())
		assert.Equal(t, [][]byte{{1}}, s.all())

		s.unref()
		assert.Equal(t, uint64(1), fsp.releaseV.Load())
	})
}

func BenchmarkCowSliceRead(b *testing.B) {
	fsp := newFixedSlicePool(16)
	cs, err := newCowSlice(fsp, [][]byte{{1}})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(cs.close)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := cs.slice()
		s.unref()
	}
}
