// Copyright 2021 Matrix Origin
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

package neg

import (
	"fmt"
	"testing"
)

func makeIbuffer(l int) []int64 {
	buf := make([]int64, l)
	for i := range buf {
		buf[i] = int64(i)
	}
	return buf
}

func makeFbuffer(l int) []float32 {
	buf := make([]float32, l)
	for i := range buf {
		buf[i] = float32(i)
	}
	return buf
}

func TestF64Sum(t *testing.T) {
	xs := makeFbuffer(100)
	rs := make([]float32, 100)
	fmt.Printf("float neg: %v\n", Float32Neg(xs, rs))
	fmt.Printf("pure float neg: %v\n", negGeneric(xs, rs))
}

func TestI64Sum(t *testing.T) {
	xs := makeIbuffer(100)
	rs := make([]int64, 100)
	fmt.Printf("int neg: %v\n", Int64Neg(xs, rs))
	fmt.Printf("pure int neg: %v\n", negGeneric(xs, rs))
}
