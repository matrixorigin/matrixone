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

package value_scan

import (
	"bytes"
	"testing"

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

// add unit tests for cases
type valueScanTestCase struct {
	arg  *ValueScan
	proc *process.Process
}

var (
	tcs []valueScanTestCase
)

func init() {
	tcs = []valueScanTestCase{
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			arg: &ValueScan{
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestValueScan(t *testing.T) {
	for _, tc := range tcs {
		resetBatchs(tc.arg)
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)

		tc.arg.Reset(tc.proc, false, nil)

		resetBatchs(tc.arg)
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func resetBatchs(arg *ValueScan) {
	bat := colexec.MakeMockBatchs()
	arg.Batchs = append(arg.Batchs, bat)
}

func TestGenSubBatchFromOriginBatch(t *testing.T) {
	testCases := []struct {
		name      string
		types     []types.Type
		batchSize int
		expected  int
	}{
		{"BatchSize8191", []types.Type{types.T_int32.ToType()}, 8191, 8191},
		{"BatchSize8192", []types.Type{types.T_int32.ToType()}, 8192, 8192},
		{"BatchSize8193", []types.Type{types.T_int32.ToType()}, 8193, 8193},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())
			vs := &ValueScan{
				Batchs: make([]*batch.Batch, 2),
			}

			// Create a mock batch with the specified size
			bat := testutil.NewBatch(tc.types, false, tc.batchSize, proc.Mp())
			vs.Batchs[0] = bat

			// Generate sub-batch
			rowCnt := 0
			curValue := int32(0)
			for {
				subBatch, err := vs.genSubBatchFromOriginBatch(proc)
				require.NoError(t, err)
				if subBatch == nil {
					break
				}
				require.LessOrEqual(t, subBatch.RowCount(), oneBatchMaxRow)
				rowCnt += subBatch.RowCount()
				for i := 0; i < subBatch.RowCount(); i++ {
					vec := subBatch.GetVector(0)
					v := vector.GetFixedAtNoTypeCheck[int32](vec, i)
					require.Equal(t, curValue, v)
					curValue++
				}
			}
			require.Equal(t, rowCnt, tc.expected)
		})
	}
}
