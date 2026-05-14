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

package mergerecursive

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type mergeRecTestCase struct {
	arg  *MergeRecursive
	proc *process.Process
}

func makeTestCases(t *testing.T) []mergeRecTestCase {
	return []mergeRecTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg:  &MergeRecursive{},
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestMergeRecursive(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		resetChildren(tc.arg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)

		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		tc.arg.Reset(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func resetChildren(arg *MergeRecursive, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func TestAccountForRetainedBatchSurfacesQuotaError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(name string, isSystem, isGlobal bool) (interface{}, error) {
		if name == "cte_max_memory_bytes" {
			return int64(1), nil
		}
		return int64(0), nil
	})

	arg := &MergeRecursive{}
	defer arg.Free(proc, false, nil)

	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())
	err := arg.ctr.memAcct.AccountSlot(proc, arg.ctr.freeBats, arg.ctr.i, bat)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCteMemoryQuotaExceeded),
		"want ErrCteMemoryQuotaExceeded, got %v", err)
	require.Equal(t, int64(0), arg.ctr.memAcct.TotalBytes())
}

func TestResetClearsAccountant(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	arg := &MergeRecursive{}
	defer arg.Free(proc, false, nil)

	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())
	require.NoError(t, arg.ctr.memAcct.AccountSlot(proc, arg.ctr.freeBats, arg.ctr.i, bat))
	require.Greater(t, arg.ctr.memAcct.TotalBytes(), int64(0))

	arg.Reset(proc, false, nil)
	require.Equal(t, int64(0), arg.ctr.memAcct.TotalBytes())
	require.False(t, arg.ctr.memAcct.Resolved())
}

func TestResetCleansMpoolConsistently(t *testing.T) {
	// Without the bat.Clean loop in Reset, the mpool would leak across
	// operator reuse cycles even though the accountant reports zero.
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	arg := &MergeRecursive{}

	// MakeMockBatchs allocates from Go heap, which CurrNB does not
	// track. Allocate off-heap data through the mpool explicitly so
	// we can observe the effect of Reset's bat.Clean loop on
	// proc.Mp().CurrNB().
	const vecLen = 256
	const dataSz = vecLen * 4 // int32
	data1, err := proc.Mp().Alloc(dataSz, true)
	require.NoError(t, err)
	vec1 := newOffHeapVec(data1, vecLen)
	bat1 := batch.NewWithSchema(true, []string{"a"}, []types.Type{types.T_int32.ToType()})
	bat1.Vecs[0] = vec1
	bat1.SetRowCount(vecLen)

	data2, err := proc.Mp().Alloc(dataSz, true)
	require.NoError(t, err)
	vec2 := newOffHeapVec(data2, vecLen)
	bat2 := batch.NewWithSchema(true, []string{"a"}, []types.Type{types.T_int32.ToType()})
	bat2.Vecs[0] = vec2
	bat2.SetRowCount(vecLen)

	// Account the first batch, store both in freeBats
	require.NoError(t, arg.ctr.memAcct.AccountSlot(proc, arg.ctr.freeBats, arg.ctr.i, bat1))
	arg.ctr.freeBats = append(arg.ctr.freeBats, bat1, bat2)

	before := proc.Mp().CurrNB()
	require.Greater(t, before, int64(0),
		"storing batches must allocate memory in the mpool")
	require.Greater(t, arg.ctr.memAcct.TotalBytes(), int64(0),
		"accountant must track batch memory")

	// Reset must clean batches AND zero the accountant
	arg.Reset(proc, false, nil)
	require.Equal(t, int64(0), arg.ctr.memAcct.TotalBytes())
	require.Nil(t, arg.ctr.freeBats)
	require.Nil(t, arg.ctr.bats)

	// After Reset the mpool should have released the batches' data
	after := proc.Mp().CurrNB()
	require.Less(t, after, before,
		"mpool usage must decrease after Reset cleans the batches")

	// Free and verify mpool is fully released
	arg.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB(),
		"mpool must be clean after Reset + Free: leaked bytes would accumulate across pool reuse")
}

// newOffHeapVec creates a FLAT int32 vector backed by off-heap data.
func newOffHeapVec(data []byte, length int) *vector.Vector {
	vec := vector.NewVecWithData(types.T_int32.ToType(), length, data, nil)
	return vec
}
