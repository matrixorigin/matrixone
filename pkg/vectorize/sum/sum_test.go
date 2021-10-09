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

package sum

import (
	"fmt"
	"testing"
)

func makeIbuffer(l int) []int32 {
	buf := make([]int32, l)
	for i := range buf {
		buf[i] = int32(i + 1)
	}
	return buf
}

func makeFbuffer(l int) []float64 {
	buf := make([]float64, l)
	for i := range buf {
		buf[i] = float64(i)
	}
	return buf
}

func TestF32Sum(t *testing.T) {
	xs := makeFbuffer(1000000)
	fmt.Printf("sum: %v\n", Float64Sum(xs))
	fmt.Printf("pure sum: %v\n", sumFloatGeneric(xs))
}

func TestI64Sum(t *testing.T) {
	xs := makeIbuffer(1000000)
	avx2Sum := Int32Sum(xs)
	goSum := sumSignedGeneric(xs)
	if avx2Sum != goSum {
		t.Errorf("avx2 sum = %v, go sum = %v", avx2Sum, goSum)
	}
	fmt.Printf("sum: %v\n", Int32Sum(xs))
	fmt.Printf("pure sum: %v\n", sumSignedGeneric(xs))
}
