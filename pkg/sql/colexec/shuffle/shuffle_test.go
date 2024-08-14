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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 10     // default rows
	BenchmarkRows = 100000 // default rows for benchmark
)

// add unit tests for cases
type shuffleTestCase struct {
	arg  *Shuffle
	proc *process.Process
}

var (
	tcs []shuffleTestCase
)

func init() {
	tcs = []shuffleTestCase{
		newTestCase(mpool.MustNewZero()),
	}
}

func newTestCase(m *mpool.MPool) shuffleTestCase {
	return shuffleTestCase{
		proc: testutil.NewProcessWithMPool("", m),
		arg: &Shuffle{
			ctr:                container{},
			ShuffleColIdx:      0,
			ShuffleType:        int32(plan.ShuffleType_Range),
			AliveRegCnt:        4,
			ShuffleColMin:      1,
			ShuffleColMax:      1000000,
			ShuffleRangeUint64: nil,
			ShuffleRangeInt64:  nil,
			RuntimeFilterSpec:  nil,
			msgReceiver:        nil,
			OperatorBase:       vm.OperatorBase{},
		},
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
