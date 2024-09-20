// Copyright 2024 Matrix Origin
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

package apply

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type applyTestCase struct {
	arg  *Apply
	proc *process.Process
}

var (
	tcs []applyTestCase
)

func init() {
	tcs = []applyTestCase{
		newTestCase(CROSS),
		newTestCase(OUTER),
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestApply(t *testing.T) {
	/*for _, tc := range tcs {
		_ = tc.arg.Prepare(tc.proc)
		_, _ = tc.arg.Call(tc.proc)
		tc.arg.Reset(tc.proc, false, nil)
		tc.arg.Free(tc.proc, false, nil)
	}*/
}

func newTestCase(applyType int) applyTestCase {
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())
	arg := NewArgument()
	arg.ApplyType = applyType
	return applyTestCase{
		arg:  arg,
		proc: proc,
	}
}
