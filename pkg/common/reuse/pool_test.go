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

package reuse

import (
	"reflect"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	providers = map[string]SPI{
		"sync-pool": SyncBased,
		"mpool":     MpoolBased,
	}
)

func TestGetAndFree(t *testing.T) {
	for name, spi := range providers {
		t.Run(name, func(t *testing.T) {
			for key := range pools {
				delete(pools, key)
			}
			use(spi)
			CreatePool[person](
				func() *person { return &person{} },
				func(p *person) {
					names := p.names[:0]
					*p = person{}
					p.names = names
				},
				DefaultOptions[person](),
			)

			p := Alloc[person](nil)
			assert.Empty(t, p.names)
			assert.Equal(t, 0, p.age)
			p.names = append(p.names, "hello")
			p.age = 10
			Free(p, nil)

			p2 := Alloc[person](nil)
			assert.Empty(t, p2.names)
			assert.Equal(t, 0, p2.age)
			Free(p2, nil)
		})
	}
}

func TestCheckDoubleFree(t *testing.T) {
	RunReuseTests(func() {
		for name, spi := range providers {
			t.Run(name, func(t *testing.T) {
				for key := range pools {
					delete(pools, key)
				}
				use(spi)
				CreatePool[person](
					func() *person { return &person{} },
					func(p *person) {
						names := p.names[:0]
						*p = person{}
						p.names = names
					},
					DefaultOptions[person]().WithEnableChecker(),
				)

				p := Alloc[person](nil)
				assert.Empty(t, p.names)
				assert.Equal(t, 0, p.age)
				p.names = append(p.names, "hello")
				p.age = 10
				Free(p, nil)

				defer func() {
					assert.NotNil(t, recover())
				}()
				Free(p, nil)
			})
		}
	})
}

func TestCheckLeakFree(t *testing.T) {
	RunReuseTests(func() {
		for name, spi := range providers {
			// mpool not support leak free check
			if spi == MpoolBased {
				continue
			}

			t.Run(name, func(t *testing.T) {
				for key := range pools {
					delete(pools, key)
				}
				use(spi)
				released := make(chan struct{})
				CreatePool[person](
					func() *person { return &person{} },
					func(p *person) {
						names := p.names[:0]
						*p = person{}
						p.names = names
					},
					DefaultOptions[person]().
						WithEnableChecker().
						WithReleaseFunc(func(*person) {
							close(released)
						}).
						withGCRecover(func() {
							assert.NotNil(t, recover())
						}),
				)

				p := Alloc[person](nil)
				assert.Empty(t, p.names)
				assert.Equal(t, 0, p.age)
				p = nil
				debug.FreeOSMemory()
				select {
				case <-released:
				case <-time.After(time.Second):
					assert.Fail(t, "release callback did not run")
				}
			})
		}
	})
}

func newCheckedPersonSyncPool() *syncPoolBased[person, *person] {
	return newSyncPoolBased(
		func() *person { return &person{} },
		func(p *person) { *p = person{} },
		DefaultOptions[person]().WithEnableChecker(),
	).(*syncPoolBased[person, *person])
}

func assertOnlyPersonCheckerStatus(
	t *testing.T,
	c *checker[person, *person],
	want step,
) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	assert.Len(t, c.mu.m, 1)
	for _, status := range c.mu.m {
		assert.Equal(t, want, status.step)
		assert.Equal(t, checkerActive.current(), status.epoch)
	}
}

func TestSyncPoolCheckerAdoptsObjectCreatedBeforeScope(t *testing.T) {
	pool := newCheckedPersonSyncPool()
	pool.c.enable = false
	p := pool.Alloc()
	pool.Free(p)
	assert.Empty(t, pool.c.mu.m)
	pool.c.enable = true

	RunReuseTests(func() {
		got := pool.Alloc()
		assertOnlyPersonCheckerStatus(t, pool.c, inUse)
		pool.Free(got)
	})
}

func TestSyncPoolCheckerAdoptsObjectFreedInsideScope(t *testing.T) {
	pool := newCheckedPersonSyncPool()
	pool.c.enable = false
	p := pool.Alloc()
	pool.c.enable = true

	RunReuseTests(func() {
		pool.Free(p)
		assertOnlyPersonCheckerStatus(t, pool.c, idle)
		runtime.KeepAlive(p)
		got := pool.Alloc()
		pool.Free(got)
	})
}

func TestSyncPoolCheckerStillDetectsDoubleFreeAfterAdoption(t *testing.T) {
	pool := newCheckedPersonSyncPool()
	pool.c.enable = false
	p := pool.Alloc()
	pool.Free(p)
	pool.c.enable = true

	RunReuseTests(func() {
		got := pool.Alloc()
		pool.Free(got)
		assert.Panics(t, func() {
			pool.Free(got)
		})
	})
}

func BenchmarkGet(b *testing.B) {
	fn := func(spi SPI, b *testing.B) {
		for key := range pools {
			delete(pools, key)
		}
		use(SyncBased)
		CreatePool[person](
			func() *person { return &person{} },
			func(p *person) {
				names := p.names[:0]
				*p = person{}
				p.names = names
			},
			DefaultOptions[person](),
		)
		b.ResetTimer()

		sum := uint64(0)
		for i := 0; i < b.N; i++ {
			p := Alloc[person](nil)
			Free(p, nil)
			sum++
		}
		_ = sum
	}

	b.Run("sync-pool", func(b *testing.B) {
		fn(SyncBased, b)
	})
	b.Run("mpool", func(b *testing.B) {
		fn(MpoolBased, b)
	})
}

func BenchmarkGetParallel(b *testing.B) {
	fn := func(spi SPI, b *testing.B) {
		for key := range pools {
			delete(pools, key)
		}
		use(SyncBased)
		CreatePool[person](
			func() *person { return &person{} },
			func(p *person) {
				names := p.names[:0]
				*p = person{}
				p.names = names
			},
			DefaultOptions[person](),
		)
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			sum := uint64(0)
			for {
				if pb.Next() {
					p := Alloc[person](nil)
					Free(p, nil)
					sum++
				} else {
					break
				}
			}
			_ = sum
		})
	}

	b.Run("sync-pool", func(b *testing.B) {
		fn(SyncBased, b)
	})
	b.Run("mpool", func(b *testing.B) {
		fn(MpoolBased, b)
	})
}

type person struct {
	names []string
	age   int
}

func (p *person) TypeName() string {
	return "person"
}

func TestTypeOf(t *testing.T) {
	rt := reflect.TypeOf((*person)(nil))
	want := reflect.ValueOf(rt).Pointer()
	got := uintptr(typeOf[person]())
	assert.Equal(t, want, got)
}
