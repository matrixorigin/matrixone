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
	np.AddRange(0, 0)
	np.AddRange(1, 10)
	require.Equal(t, 9, np.Count())
	np.Clear()

	ok := np.IsEmpty()
	require.Equal(t, true, ok)
	np.Add(0)
	ok = np.Contains(0)
	require.Equal(t, true, ok)
	require.Equal(t, 1, np.Count())

	itr := np.Iterator()
	require.Equal(t, uint64(0), itr.PeekNext())
	require.Equal(t, true, itr.HasNext())
	require.Equal(t, uint64(0), itr.Next())

	np.Remove(0)
	ok = np.IsEmpty()
	require.Equal(t, true, ok)

	np.AddMany([]uint64{1, 2, 3})
	require.Equal(t, 3, np.Count())
	np.RemoveRange(1, 3)
	require.Equal(t, 0, np.Count())

	np.AddMany([]uint64{1, 2, 3})
	np.Filter([]int64{0})
	fmt.Printf("%v\n", np.String())
	fmt.Printf("size: %v\n", np.Size())
	fmt.Printf("numbers: %v\n", np.Count())

	nq := New(Rows)
	nq.Unmarshal(np.Marshal())

	require.Equal(t, np.ToArray(), nq.ToArray())

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
			np.Remove(uint64(j))
		}
	}
}

func TestC(t *testing.T) {
	bm3 := New(10000)
	bm5 := New(10000)

	require.True(t, bm3.IsEmpty())
	require.True(t, bm3.C_IsEmpty())
	require.True(t, bm3.Count() == 0)
	require.True(t, bm3.C_Count() == 0)

	for i := 0; i < 10000; i += 3 {
		bm3.Add(uint64(i))
	}
	for i := 0; i < 10000; i += 5 {
		bm3.Add(uint64(i))
	}

	require.False(t, bm3.IsEmpty())
	require.False(t, bm3.C_IsEmpty())

	bm3Copy := bm3.Clone()
	require.True(t, bm3.IsSame(bm3Copy))
	require.True(t, bm3.C_Count() == bm3Copy.C_Count())
	bm5Copy := bm5.Clone()
	require.True(t, bm5.IsSame(bm5Copy))
	require.True(t, bm5.C_Count() == bm5Copy.C_Count())

	bm3CopyAgain := bm3.Clone()

	bm3.Or(bm5)
	bm3Copy.C_Or(bm5)
	require.True(t, bm3.IsSame(bm3Copy))
	require.True(t, bm3.Count() == int(bm3Copy.C_Count()))

	bm5.And(bm3CopyAgain)
	bm5Copy.C_And(bm3CopyAgain)
	require.True(t, bm5.IsSame(bm5Copy))
	require.True(t, bm5.Count() == int(bm5Copy.C_Count()))
}
