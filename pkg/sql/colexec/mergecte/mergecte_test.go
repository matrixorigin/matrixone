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

package mergecte

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

type mergeCTETestCase struct {
	arg  *MergeCTE
	proc *process.Process
}

func makeTestCases(t *testing.T) []mergeCTETestCase {
	return []mergeCTETestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg:  &MergeCTE{},
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

func TestMergeCTE(t *testing.T) {
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
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func resetChildren(arg *MergeCTE, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func TestResetSeedsBaselineFromFreeBats(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	arg := &MergeCTE{}
	defer arg.Free(proc, false, nil)

	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())
	require.NoError(t, arg.ctr.memAcct.AccountSlot(proc, arg.ctr.freeBats, arg.ctr.i, bat))
	require.Greater(t, arg.ctr.memAcct.TotalBytes(), int64(0))

	dup, err := bat.Dup(proc.Mp())
	require.NoError(t, err)
	arg.ctr.freeBats = append(arg.ctr.freeBats, dup)

	arg.Reset(proc, false, nil)
	require.Equal(t, int64(dup.Size()), arg.ctr.memAcct.TotalBytes(),
		"Reset must seed totalBytes with retained freeBats size")
	require.False(t, arg.ctr.memAcct.Resolved())
}

func TestResetSkipsSentinelMarker(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	arg := &MergeCTE{}
	defer arg.Free(proc, false, nil)

	sentinel := makeRecursiveBatch(proc)
	arg.ctr.freeBats = append(arg.ctr.freeBats, sentinel)

	arg.Reset(proc, false, nil)
	require.Equal(t, int64(0), arg.ctr.memAcct.TotalBytes(),
		"Reset must skip sentinel last-marker batches when seeding baseline")
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

	arg := &MergeCTE{}
	defer arg.Free(proc, false, nil)

	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())
	err := arg.ctr.memAcct.AccountSlot(proc, arg.ctr.freeBats, arg.ctr.i, bat)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCteMemoryQuotaExceeded),
		"want ErrCteMemoryQuotaExceeded, got %v", err)
	require.Equal(t, int64(0), arg.ctr.memAcct.TotalBytes())
}

func TestSentinelPromotionReleasesAccountedBytes(t *testing.T) {
	// Reproduces the totalBytes drift when a data batch slot is promoted to
	// a sentinel and then Reset is called. With the fix (Release +
	// CleanOnlyData before SetLast), Reset must seed baseline correctly
	// without counting the sentinel's released bytes.
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
		return int64(0), nil
	})

	arg := &MergeCTE{}
	defer arg.Free(proc, false, nil)

	// Simulate sendInitial: account and store a data batch in freeBats
	bat := colexec.MakeMockBatchs(proc.Mp())
	defer bat.Clean(proc.Mp())
	require.NoError(t, arg.ctr.memAcct.AccountSlot(proc, arg.ctr.freeBats, arg.ctr.i, bat))
	dataSize := int64(bat.Size())
	require.Greater(t, dataSize, int64(0))
	require.Equal(t, dataSize, arg.ctr.memAcct.TotalBytes())

	dup, err := bat.Dup(proc.Mp())
	require.NoError(t, err)
	arg.ctr.freeBats = append(arg.ctr.freeBats, dup)

	// Simulate sendLastTag: promote the data slot to a sentinel
	// This is the exact sendLastTag path from mergecte.go
	arg.ctr.memAcct.Release(arg.ctr.freeBats[arg.ctr.i])
	arg.ctr.freeBats[arg.ctr.i].CleanOnlyData()
	arg.ctr.freeBats[arg.ctr.i].SetLast()
	arg.ctr.i++

	// After Release, totalBytes must drop by the data batch size
	require.Equal(t, int64(0), arg.ctr.memAcct.TotalBytes(),
		"Release must zero out accounted bytes of promoted batch")

	// Verify the sentinel is in freeBats and can be distinguished
	promotedIdx := arg.ctr.i - 1
	require.True(t, arg.ctr.freeBats[promotedIdx].Last(),
		"promoted slot must be a sentinel (Last() == true)")

	// Reset must exclude the sentinel and keep baseline at 0
	arg.Reset(proc, false, nil)
	require.Equal(t, int64(0), arg.ctr.memAcct.TotalBytes(),
		"Reset must not re-add sentinel bytes to baseline")
}
