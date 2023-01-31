// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"math/rand"
	"testing"
)

func TestMap(t *testing.T) {
	ints := rand.Perm(1024)
	m := new(Map[int, int])

	// Set
	for i, n := range ints {
		n := n
		m.Set(i, &n)
	}

	// Get
	for i, n := range ints {
		p, ok := m.Get(i)
		if !ok {
			t.Fatal()
		}
		if *p != n {
			t.Fatal()
		}
	}

	// Update
	for i := range ints {
		m.Update(i, func(p *int) *int {
			newInt := *p + 1
			return &newInt
		})
	}
	for i, n := range ints {
		p, ok := m.Get(i)
		if !ok {
			t.Fatal()
		}
		if *p != n+1 {
			t.Fatal()
		}
	}

	// Delete
	for i := range ints {
		m.Delete(i)
	}
	for i := range ints {
		_, ok := m.Get(i)
		if ok {
			t.Fatal()
		}
	}

	// Update
	for i, n := range ints {
		m.Update(i, func(p *int) *int {
			if p != nil {
				t.Fatalf("got %v", *p)
			}
			n := n
			return &n
		})
	}
	for i, n := range ints {
		p, ok := m.Get(i)
		if !ok {
			t.Fatal()
		}
		if *p != n {
			t.Fatal()
		}
	}

}

func TestMapConcurrentUpdate(t *testing.T) {
	m := new(Map[int, int])
	sem := make(chan bool, 128)
	for i := 0; i < 4096; i++ {
		sem <- true
		go func() {
			defer func() {
				<-sem
			}()
			m.Update(1, func(p *int) *int {
				if p == nil {
					n := 1
					return &n
				}
				newN := *p
				newN++
				return &newN
			})
		}()
	}
	for i := 0; i < cap(sem); i++ {
		sem <- true
	}
	p, ok := m.Get(1)
	if !ok {
		t.Fatal()
	}
	if *p != 4096 {
		t.Fatalf("got %v", *p)
	}
}

func BenchmarkMapSet(b *testing.B) {
	m := new(Map[int, int])
	for i := 0; i < b.N; i++ {
		i := i
		m.Set(i, &i)
	}
}

func BenchmarkMapSetSingleKey(b *testing.B) {
	m := new(Map[int, int])
	for i := 0; i < b.N; i++ {
		i := i
		m.Set(1, &i)
	}
}

func BenchmarkMapSetAndGet(b *testing.B) {
	m := new(Map[int, int])
	for i := 0; i < b.N; i++ {
		i := i
		m.Set(i, &i)
		p, ok := m.Get(i)
		if !ok {
			b.Fatal()
		}
		if *p != i {
			b.Fatal()
		}
	}
}

func BenchmarkMapGetSingleKey(b *testing.B) {
	m := new(Map[int, int])
	n := 42
	m.Set(42, &n)
	for i := 0; i < b.N; i++ {
		p, ok := m.Get(42)
		if !ok {
			b.Fatal()
		}
		if *p != 42 {
			b.Fatal()
		}
	}
}

func BenchmarkMapUpdate(b *testing.B) {
	m := new(Map[int, int])
	for i := 0; i < b.N; i++ {
		m.Update(i/2, func(p *int) *int {
			if p == nil {
				i := 1
				return &i
			}
			newI := *p
			newI++
			return &newI
		})
	}
}

func BenchmarkMapConcurrentUpdate(b *testing.B) {
	m := new(Map[int, int])
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Update(1, func(p *int) *int {
				if p == nil {
					i := 1
					return &i
				}
				newI := *p
				newI++
				return &newI
			})
		}
	})
}
