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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
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
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestApply(t *testing.T) {
}

func newTestCase(applyType int) applyTestCase {
	proc := testutil.NewProcessWithMPool("", mpool.MustNewZero())
	arg := NewArgument()
	arg.ApplyType = applyType
	arg.TableFunction = table_function.NewArgument()
	return applyTestCase{
		arg:  arg,
		proc: proc,
	}
}

/*
func resetChildren(arg *Apply) {
	bat := colexec.MakeMockBatchs()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
*/
