// Copyright 2024 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package unionall

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	arg   *UnionAll
	types []types.Type
	proc  *process.Process
}

func newTestCase(m *mpool.MPool, ts []types.Type) testCase {
	return testCase{
		types: ts,
		proc:  testutil.NewProcessWithMPool("", m),
		arg:   &UnionAll{},
	}
}

func genTestCases() []testCase {
	return []testCase{
		newTestCase(mpool.MustNewZero(), []types.Type{types.T_int8.ToType()}),
		newTestCase(mpool.MustNewZero(), []types.Type{types.T_int8.ToType()}),
		newTestCase(mpool.MustNewZero(), []types.Type{types.T_int8.ToType(), types.T_int64.ToType()}),
	}
}

func newBatch(ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	return testutil.NewBatch(ts, false, int(rows), proc.Mp())
}
func TestUnionall(t *testing.T) {
	for _, tc := range genTestCases() {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats := []*batch.Batch{
			newBatch(tc.types, tc.proc, 10),
			newBatch(tc.types, tc.proc, 10),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.arg.Reset(tc.proc, false, nil)

		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		bats = []*batch.Batch{
			newBatch(tc.types, tc.proc, 10),
			newBatch(tc.types, tc.proc, 10),
			batch.EmptyBatch,
		}
		resetChildren(tc.arg, bats)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		tc.arg.GetChildren(0).Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func resetChildren(arg *UnionAll, bats []*batch.Batch) {
	op := colexec.NewMockOperator().WithBatchs(bats)
	arg.Children = nil
	arg.AppendChild(op)
}
