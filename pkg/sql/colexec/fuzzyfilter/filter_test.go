// Copyright 2023 Matrix Origin
//
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
package fuzzyfilter

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// can be used to check hash collision rate
// FIXME: current implemention will meet collision in rows 1000

const (
	Rows1 = 100   // default rows
	Rows2 = 1000  // default rows
	Rows3 = 10000 // default rows

	BenchmarkRows = 1000000 // default rows for benchmark
)

// add unit tests for cases
type fuzzyTestCase struct {
	arg   *Argument
	types []types.Type
	proc  *process.Process
}

var (
	tcs []fuzzyTestCase
)

func init() {
	tcs = []fuzzyTestCase{
		{
			proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: []types.Type{
				types.T_int32.ToType(),
			},
			arg: &Argument{
				TblName: "tblName",
				DbName:  "dbName",
			},
		},
	}
}

func TestString(t *testing.T) {
	for _, tc := range tcs {
		buf := new(bytes.Buffer)
		String(tc.arg, buf)
		require.Equal(t, " fuzzy check duplicate constraint", buf.String())
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
	}
}

func TestReturnBatchAttr(t *testing.T) {
	tc := tcs[0]
	err := Prepare(tc.proc, tc.arg)
	require.NoError(t, err)
	testBatch := newBatch(t, tc.types, tc.proc, Rows1)
	attrCnt := int32(len(testBatch.Attrs))
	tc.proc.Reg.InputBatch = testBatch

	_, err = Call(0, tc.proc, tc.arg, false, false)
	require.Equal(t, "dbName", tc.arg.ctr.rbat.GetVector(attrCnt).GetStringAt(0), "wrong format for batch that fuzzy filter that returns")
	require.Equal(t, "tblName", tc.arg.ctr.rbat.GetVector(attrCnt+1).GetStringAt(0), "wrong format for batch that fuzzy filter that returns")
}

func TestFuzzyFilter(t *testing.T) {
	for _, tc := range tcs {
		err := Prepare(tc.proc, tc.arg)
		require.NoError(t, err)
		tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.proc, Rows3)
		_, err = Call(0, tc.proc, tc.arg, false, false)
		require.NoError(t, err)
	}
}

// create a new block based on the type information
func newBatch(t *testing.T, ts []types.Type, proc *process.Process, rows int64) *batch.Batch {
	// not random
	bat := testutil.NewBatch(ts, false, int(rows), proc.Mp())
	pkAttr := make([]string, 1)
	pkAttr[0] = "pkCol"
	bat.SetAttributes(pkAttr)
	return bat
}
