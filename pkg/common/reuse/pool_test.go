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
	"runtime/debug"
	"testing"

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
				CreatePool[person](
					func() *person { return &person{} },
					func(p *person) {
						names := p.names[:0]
						*p = person{}
						p.names = names
					},
					DefaultOptions[person]().
						WithEnableChecker().
						withGCRecover(func() {
							assert.NotNil(t, recover())
						}),
				)

				p := Alloc[person](nil)
				assert.Empty(t, p.names)
				assert.Equal(t, 0, p.age)
				p = nil
				debug.FreeOSMemory()
			})
		}
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

func (p person) TypeName() string {
	return "person"
}

func TestTypeOf(t *testing.T) {
	rt := reflect.TypeOf((*person)(nil))
	want := reflect.ValueOf(rt).Pointer()
	got := uintptr(typeOf[person]())
	assert.Equal(t, want, got)
}
