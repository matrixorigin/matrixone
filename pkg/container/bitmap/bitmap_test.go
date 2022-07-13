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

package bitmap

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10
	BenchmarkRows = 8192
)

func TestNulls(t *testing.T) {
	np := New(Rows)
	ok := np.IsEmpty()
	require.Equal(t, true, ok)
	np.Add(0)
	ok = np.Contains(0)
	require.Equal(t, true, ok)
	np.Remove(0)
	ok = np.IsEmpty()
	require.Equal(t, true, ok)
	np.Filter([]int64{0})
	np.ToArray()
	fmt.Printf("%v\n", np.String())
	fmt.Printf("size: %v\n", np.Numbers())
	fmt.Printf("numbers: %v\n", np.Numbers())
	np.Clear()
}

func BenchmarkAdd(b *testing.B) {
	np := New(BenchmarkRows)
	for i := 0; i < b.N; i++ {
		for j := 0; j < BenchmarkRows; j++ {
			np.Add(uint64(j))
		}
		for j := 0; j < BenchmarkRows; j++ {
			np.Contains(uint64(j))
		}
		for j := 0; j < BenchmarkRows; j++ {
			np.Del(uint64(j))
		}
	}
}
