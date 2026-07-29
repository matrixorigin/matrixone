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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// add unit tests for cases
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

func TestMergeRecursiveFreeWithoutReset(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := NewArgument()
	defer arg.Release()

	first := colexec.MakeMockBatchs(proc.Mp())
	last := colexec.MakeMockBatchs(proc.Mp())
	last.SetLast()
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, last})
	arg.AppendChild(child)

	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Len(t, arg.ctr.freeBats, 2)
	require.Len(t, arg.ctr.bats, 1)
	require.Same(t, result.Batch, arg.ctr.buf)
	require.True(t, arg.ctr.last)
	cached := append([]*batch.Batch(nil), arg.ctr.freeBats...)
	for _, bat := range cached {
		require.NotNil(t, bat)
		require.NotEmpty(t, bat.Vecs)
	}

	// Free is a terminal lifecycle operation and must release operator-owned
	// batches even when an interrupted execution did not reach Reset first.
	arg.Free(proc, true, context.Canceled)
	for _, bat := range cached {
		require.Nil(t, bat.Vecs)
	}
	require.Nil(t, arg.ctr.freeBats)
	require.Nil(t, arg.ctr.bats)
	require.Nil(t, arg.ctr.buf)
	require.False(t, arg.ctr.last)
	require.Zero(t, arg.ctr.i)

	// Cleanup must remain safe if callers defensively repeat it.
	arg.Free(proc, true, context.Canceled)

	child.Free(proc, true, context.Canceled)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func resetChildren(arg *MergeRecursive, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
