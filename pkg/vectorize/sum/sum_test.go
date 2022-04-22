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

func makeIbuffer(l int) []int64 {
	buf := make([]int64, l)
	for i := range buf {
		buf[i] = int64(i)
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

func TestF64Sum(t *testing.T) {
	xs := makeFbuffer(10000)
	fmt.Printf("sum: %v\n", Float64Sum(xs))
	fmt.Printf("pure sum: %v\n", floatSum(xs))
}

func TestI64Sum(t *testing.T) {
	xs := makeIbuffer(10000)
	fmt.Printf("sum: %v\n", Int64Sum(xs))
	fmt.Printf("pure sum: %v\n", signedSum(xs))
}
