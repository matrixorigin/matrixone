// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func newSQLFileAccessTestProc(t *testing.T) *process.Process {
	t.Helper()
	proc := testutil.NewProc(t)
	proc.Base.SessionInfo.Account = "sys"
	proc.Base.SessionInfo.Role = "moadmin"
	proc.Ctx = process.SQLFileAccessContext(proc.Ctx, proc)
	return proc
}

// Adapt the existing 4.2 function fixture without changing its production API.
func (fc FunctionTestCase) WithSelectList(selection *FunctionSelectList) FunctionTestCase {
	original := fc.fn
	fc.fn = func(v []*vector.Vector, r vector.FunctionResultWrapper, p *process.Process, n int, _ *FunctionSelectList) error {
		return original(v, r, p, n, selection)
	}
	return fc
}
func (fc *FunctionTestCase) Free() {
	for _, v := range fc.parameters {
		v.Free(fc.proc.Mp())
	}
	fc.parameters = nil
	fc.result.Free()
}
func (fc *FunctionTestCase) RunAndFree() (bool, string) { defer fc.Free(); return fc.Run() }
