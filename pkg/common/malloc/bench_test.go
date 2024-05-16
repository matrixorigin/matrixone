// Copyright 2024 Matrix Origin
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

package malloc

import (
	"testing"
)

func BenchmarkAllocFree(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, handle := Alloc(4096, true)
		handle.Free()
	}
}

func BenchmarkAllocFreeNoClear(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, handle := Alloc(4096, false)
		handle.Free()
	}
}

func BenchmarkParallelAllocFree(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for size := 1; pb.Next(); size++ {
			_, handle := Alloc(size%65536, true)
			handle.Free()
		}
	})
}

func BenchmarkParallelAllocFreeNoClear(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for size := 1; pb.Next(); size++ {
			_, handle := Alloc(size%65536, false)
			handle.Free()
		}
	})
}

func BenchmarkMax(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, handle := Alloc(maxClassSize, true)
		handle.Free()
	}
}

func BenchmarkMaxNoClear(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, handle := Alloc(maxClassSize, false)
		handle.Free()
	}
}

func BenchmarkMin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, handle := Alloc(minClassSize, true)
		handle.Free()
	}
}

func BenchmarkMinNoClear(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, handle := Alloc(minClassSize, false)
		handle.Free()
	}
}
