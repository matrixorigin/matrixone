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

type fuzzyTestCase struct {
	arg   *Argument
	types []types.Type
	proc  *process.Process
}

var (
	rowCnts []float64

	referM []float64

	tcs []fuzzyTestCase
)

func init() {
	rowCnts = []float64{100000, 500000, 1000000, 5000000, 10000000}

	// https://hur.st/bloomfilter/?n=100000&p=0.00001&m=&k=3
	referM = []float64{
		13774223,
		68871111,
		137742221,
		688711101,
		1377422201,
	}

	tcs = []fuzzyTestCase{
		{
			arg:  new(Argument),
			proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: []types.Type{
				types.T_int32.ToType(),
			},
		},
		{
			arg:  new(Argument),
			proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: []types.Type{
				types.T_date.ToType(),
			},
		},
		{
			arg:  new(Argument),
			proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: []types.Type{
				types.T_float32.ToType(),
			},
		},
		{
			arg:  new(Argument),
			proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: []types.Type{
				types.T_varchar.ToType(),
			},
		},
		{
			arg:  new(Argument),
			proc: testutil.NewProcessWithMPool(mpool.MustNewZero()),
			types: []types.Type{
				types.T_binary.ToType(),
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

func TestEstimate(t *testing.T) {
	for i, r := range rowCnts {
		m := EstimateBitsNeed(r, k, p)
		require.LessOrEqual(t, referM[i], m, "The estimated number of bits required is too small")
	}
}

func TestFuzzyFilter(t *testing.T) {
	for _, tc := range tcs {
		for _, r := range rowCnts {
			tc.arg.N = r
			err := Prepare(tc.proc, tc.arg)
			require.NoError(t, err)
			tc.proc.Reg.InputBatch = newBatch(t, tc.types, tc.proc, int64(r))
			_, err = Call(0, tc.proc, tc.arg, false, false)
			require.NoError(t, err)
			t.Logf("Estimated row count is %f, collisionCnt is %d, fp is %f", tc.arg.N, tc.arg.collisionCnt, float64(tc.arg.collisionCnt)/float64(tc.arg.N))
			require.LessOrEqual(t, float64(tc.arg.collisionCnt)/float64(tc.arg.N), 1.1*p, "collision cnt is too high, sth must went wrong")
		}
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
