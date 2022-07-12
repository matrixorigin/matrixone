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

package count

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10
	BenchmarkRows = 100000
)

type testCase struct {
	typ  types.Type
	af   agg.Agg[any]
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
		newTestCase(mheap.New(guest.New(1<<30, hm)), types.New(types.T_int32, 0, 0, 0)),
		newTestCase(mheap.New(guest.New(1<<30, hm)), types.New(types.T_int64, 0, 0, 0)),
		newTestCase(mheap.New(guest.New(1<<30, hm)), types.New(types.T_float32, 0, 0, 0)),
		newTestCase(mheap.New(guest.New(1<<30, hm)), types.New(types.T_float64, 0, 0, 0)),
	}
}

func TestCount(t *testing.T) {
	for _, tc := range tcs {
		af := tc.af
		err := af.Grows(Rows, tc.proc.Mp)
		require.NoError(t, err)
		af.Fill(0, 0, 1, tc.vec)
		af.BatchFill(0, []uint8{0, 1}, []uint64{1, 2}, []int64{1, 1}, tc.vec)
		af0 := af.Dup()
		af0.Grows(1, tc.proc.Mp)
		af0.Fill(0, 0, 1, tc.vec)
		af.Merge(af0, 0, 0)
		af.BatchMerge(af0, 0, []uint8{0}, []uint64{1})
		af.BulkFill(0, newZs(), tc.vec)
		rv := af.Eval(newZs(), nil)
		fmt.Printf("rv: %v\n", rv)
		rv.Free(tc.proc.Mp)
		fmt.Printf("r0: %v\n", af0.Type())
		fmt.Printf("r0: %v\n", af0.InputType())
		fmt.Printf("r0: %v\n", af0)
		af0.Free(tc.proc.Mp)
		tc.vec.Free(tc.proc.Mp)
		require.Equal(t, int64(0), mheap.Size(tc.proc.Mp))
	}
}

func BenchmarkCount(b *testing.B) {
	var r agg.Agg[any]

	r = New(types.New(types.T_int32, 0, 0, 0), types.New(types.T_int64, 0, 0, 0))
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	r.Grows(1, m)
	vec := testutil.NewInt32Vector(BenchmarkRows, types.New(types.T_int32, 0, 0, 0), m, false)
	for i := 0; i < b.N; i++ {
		for j := 0; j < BenchmarkRows; j++ {
			r.Fill(0, int64(j), 1, vec)
		}
	}
	r.Free(m)
	vec.Free(m)
}

func newZs() []int64 {
	zs := make([]int64, Rows)
	for i := range zs {
		zs[i] = 1
	}
	return zs
}

func newTestCase(m *mheap.Mheap, typ types.Type) testCase {
	return testCase{
		typ:  typ,
		proc: process.New(m),
		af:   New(typ, ReturnType(typ)),
		vec:  testutil.NewVector(Rows, typ, m, false),
	}
}
