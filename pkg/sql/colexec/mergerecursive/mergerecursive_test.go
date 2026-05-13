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
