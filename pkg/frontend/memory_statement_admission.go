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

package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (execCtx *ExecCtx) admitStatementMemory() error {
	if execCtx.statementMemoryRelease != nil || execCtx.proc == nil {
		return nil
	}
	previous := execCtx.reqCtx
	ctx := previous
	stmt := execCtx.effectiveTxnStatement
	if stmt == nil {
		stmt = execCtx.stmt
	}
	// Transaction finalization must be able to release existing resources.
	// This is still bounded and physically accounted, not an internal-SQL bypass.
	switch stmt.(type) {
	case *tree.CommitTransaction, *tree.RollbackTransaction:
		ctx = process.WithControlMemoryAdmission(ctx, true)
	}
	ctx, lease, err := execCtx.proc.EnterStatementMemoryAdmission(ctx)
	if err != nil {
		return err
	}
	proc := execCtx.proc
	previousProcCtx := proc.GetTopContext()
	execCtx.reqCtx = ctx
	proc.ReplaceTopCtx(ctx)
	execCtx.statementMemoryRelease = func() {
		lease.Close()
		execCtx.reqCtx = previous
		proc.ReplaceTopCtx(previousProcCtx)
	}
	return nil
}

func (execCtx *ExecCtx) releaseStatementMemoryAdmission() {
	if release := execCtx.statementMemoryRelease; release != nil {
		execCtx.statementMemoryRelease = nil
		release()
	}
}
