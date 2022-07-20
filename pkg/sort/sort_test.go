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

package sort

import (
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 1000
	BenchmarkRows = 100000
)

type testCase struct {
	desc bool
	vec  *vector.Vector
	proc *process.Process
}

var (
	hm  *host.Mmu
	tcs []testCase
)

func init() {
	hm = host.New(1 << 30)
	tcs = []testCase{
		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_bool, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_bool, 0, 0, 0)),

		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_int8, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_int8, 0, 0, 0)),
		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_int16, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_int16, 0, 0, 0)),
		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_int32, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_int32, 0, 0, 0)),
		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_int64, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_int64, 0, 0, 0)),

		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_uint8, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_uint8, 0, 0, 0)),
		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_uint16, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_uint16, 0, 0, 0)),
		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_uint32, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_uint32, 0, 0, 0)),
		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_uint64, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_uint64, 0, 0, 0)),

		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_float32, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_float32, 0, 0, 0)),

		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_float64, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_float64, 0, 0, 0)),

		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_date, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_date, 0, 0, 0)),

		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_datetime, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_datetime, 0, 0, 0)),

		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_timestamp, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_timestamp, 0, 0, 0)),

		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_decimal64, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_decimal64, 0, 0, 0)),

		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_decimal128, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_decimal128, 0, 0, 0)),

		newTestCase(true, mheap.New(guest.New(1<<30, hm)), types.New(types.T_varchar, 0, 0, 0)),
		newTestCase(false, mheap.New(guest.New(1<<30, hm)), types.New(types.T_varchar, 0, 0, 0)),
	}
}

func TestSort(t *testing.T) {
	for _, tc := range tcs {
		os := make([]int64, vector.Length(tc.vec))
		for i := range os {
			os[i] = int64(i)
		}
		Sort(tc.desc, os, tc.vec)
		checkResult(t, tc.desc, tc.vec, os)
		tc.vec.Free(tc.proc.Mp)
		require.Equal(t, int64(0), mheap.Size(tc.proc.Mp))
	}
}

func BenchmarkSortInt(b *testing.B) {
	vs := make([]int, BenchmarkRows)
	for i := range vs {
		vs[i] = i
	}
	for i := 0; i < b.N; i++ {
		sort.Ints(vs)
	}
}

func BenchmarkSortIntVector(b *testing.B) {
	m := mheap.New(guest.New(1<<30, hm))
	vec := testutil.NewInt32Vector(BenchmarkRows, types.New(types.T_int32, 0, 0, 0), m, true, nil)
	os := make([]int64, vector.Length(vec))
	for i := range os {
		os[i] = int64(i)
	}
	for i := 0; i < b.N; i++ {
		Sort(false, os, vec)
	}
}

func checkResult(t *testing.T, desc bool, vec *vector.Vector, os []int64) {
	switch vec.Typ.Oid {
	case types.T_int32:
		vs := make([]int, len(os))
		col := vector.GenericVectorValues[int32](vec)
		for i := range vs {
			vs[i] = int(col[i])
		}
		sort.Ints(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, int(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, int(col[os[i]]))
			}
		}
	case types.T_int64:
		vs := make([]int, len(os))
		col := vector.GenericVectorValues[int64](vec)
		for i := range vs {
			vs[i] = int(col[i])
		}
		sort.Ints(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, int(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, int(col[os[i]]))
			}
		}
	case types.T_float32:
		vs := make([]float64, len(os))
		col := vector.GenericVectorValues[float32](vec)
		for i := range vs {
			vs[i] = float64(col[i])
		}
		sort.Float64s(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, float64(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, float64(col[os[i]]))
			}
		}
	case types.T_float64:
		vs := make([]float64, len(os))
		col := vector.GenericVectorValues[float64](vec)
		for i := range vs {
			vs[i] = float64(col[i])
		}
		sort.Float64s(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, float64(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, float64(col[os[i]]))
			}
		}
	}
}

func newTestCase(desc bool, m *mheap.Mheap, typ types.Type) testCase {
	return testCase{
		desc: desc,
		proc: process.New(m),
		vec:  testutil.NewVector(Rows, typ, m, true, nil),
	}
}
