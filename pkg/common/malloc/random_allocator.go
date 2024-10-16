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

// RandomAllocator calls upstream1 or upstream2 in certain probability
type RandomAllocator[A, B Allocator] struct {
	upstream1         A
	upstream2         B
	upstream2Fraction uint32
}

func NewRandomAllocator[A, B Allocator](
	upstream1 A,
	upstream2 B,
	upstream2Fraction uint32,
) *RandomAllocator[A, B] {
	return &RandomAllocator[A, B]{
		upstream1:         upstream1,
		upstream2:         upstream2,
		upstream2Fraction: upstream2Fraction,
	}
}

var _ Allocator = new(RandomAllocator[Allocator, Allocator])

func (r *RandomAllocator[A, B]) Allocate(size uint64, hint Hints) ([]byte, Deallocator, error) {
	if fastrand()%r.upstream2Fraction > 0 {
		return r.upstream1.Allocate(size, hint)
	}
	return r.upstream2.Allocate(size, hint)
}
