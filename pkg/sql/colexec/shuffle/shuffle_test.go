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

package shuffle

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 8192 // default rows
)

// add unit tests for cases
type shuffleTestCase struct {
	arg   *Shuffle
	types []types.Type
	proc  *process.Process
}

var (
	tcs []shuffleTestCase
)

func init() {
	tcs = []shuffleTestCase{
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int32.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     4,
				ShuffleColMin: 1,
				ShuffleColMax: 1000000,
			},
		},
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int64.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Range),
				BucketNum:     8,
				ShuffleColMin: 1,
				ShuffleColMax: 10000,
			},
		},
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			types: []types.Type{
				types.T_int64.ToType(),
			},
			arg: &Shuffle{
				ctr:           container{},
				ShuffleColIdx: 0,
				ShuffleType:   int32(plan.ShuffleType_Hash),
				BucketNum:     3,
			},
		},
	}
}

func runShuffleCase(t *testing.T, tc shuffleTestCase) {
	var result vm.CallResult
	var count int
	err := tc.arg.Prepare(tc.proc)
	require.NoError(t, err)
	resetChildren(tc.arg, getInputBats(tc))
	for {
		result, err = tc.arg.Call(tc.proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Batch.RowCount() == 0 || result.Status == vm.ExecStop {
			break
		}
		count += result.Batch.RowCount()
	}
	require.Equal(t, count, 16*Rows)
	tc.arg.GetChildren(0).Free(tc.proc, false, nil)
	tc.arg.Reset(tc.proc, false, nil)
	require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
}

func TestShuffle(t *testing.T) {

	for _, tc := range tcs {
		runShuffleCase(t, tc)
		// second run
		runShuffleCase(t, tc)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestFree(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.arg.Free(tc.proc, false, nil)

	}
}

func TestReset(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		tc.arg.Reset(tc.proc, false, nil)
	}
}

func getInputBats(tc shuffleTestCase) []*batch.Batch {
	return []*batch.Batch{
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		newBatch(tc.types, tc.proc, Rows),
		batch.EmptyBatch,
	}
}

// create a new block based on the type information
func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, true, int(rows), proc.Mp())
}

func resetChildren(arg *Shuffle, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
