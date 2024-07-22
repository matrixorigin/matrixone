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

package fileservice

import (
	"sync"
	"testing"
)

func TestIOMerger(t *testing.T) {
	merger := NewIOMerger()
	n := 1024
	key := IOMergeKey{
		Path: "foo",
	}

	wg := new(sync.WaitGroup)
	wg.Add(n)
	var c int
	var cs []int
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for {
				done, wait := merger.Merge(key)
				if done != nil {
					cs = append(cs, c)
					c++
					done()
					return
				} else {
					wait()
				}
			}
		}()
	}
	wg.Wait()

	if c != 1024 {
		t.Fatal()
	}

	// should be no race and sequential
	for i, c := range cs {
		if c != i {
			t.Fatalf("got %v", cs)
		}
	}
}

func BenchmarkIOMergerNoContention(b *testing.B) {
	merger := NewIOMerger()
	key := IOMergeKey{
		Path: "foo",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		done, wait := merger.Merge(key)
		if done != nil {
			done()
		} else {
			wait()
		}
	}
}

func BenchmarkIOMergerParallel(b *testing.B) {
	merger := NewIOMerger()
	key := IOMergeKey{
		Path: "foo",
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			done, wait := merger.Merge(key)
			if done != nil {
				done()
			} else {
				wait()
			}
		}
	})
}
