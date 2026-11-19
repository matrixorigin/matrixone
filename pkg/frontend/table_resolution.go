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
	"context"

	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ plan.CompilerContext = (*TxnCompilerContext)(nil)

// planningCompiler accepts only the actual frontend compiler, not a wrapper
// whose Resolve may enforce a different tenant or record dependencies.
func planningCompiler(compiler plan.CompilerContext) plan.CompilerContext {
	tcc, ok := compiler.(*TxnCompilerContext)
	if !ok || tcc == nil {
		return nil
	}
	return tcc
}

func (tcc *TxnCompilerContext) statementExecution() (*ExecCtx, client.TxnOperator) {
	if tcc == nil {
		return nil, nil
	}
	tcc.mu.Lock()
	exec := tcc.execCtx
	tcc.mu.Unlock()
	if exec == nil || exec.ses == nil {
		return nil, nil
	}
	handler := exec.ses.GetTxnHandler()
	if handler == nil {
		return exec, nil
	}
	return exec, handler.GetTxn()
}

func (tcc *TxnCompilerContext) BorrowedTableStillCurrent(proc *process.Process, borrow *plan.BorrowedTable) bool {
	if tcc == nil || proc == nil || borrow == nil || borrow.Relation == nil || borrow.Read.Op == nil {
		return false
	}
	owner, ok := borrow.Owner.(*ExecCtx)
	if !ok || owner == nil || owner.proc != proc || owner.reqCtx == nil || context.Cause(owner.reqCtx) != nil {
		return false
	}
	current, op := tcc.statementExecution()
	return current == owner && op == borrow.Read.Op && proc.GetTxnOperator() == op
}

// tableReadCapture is lexical to one ordinary resolver call. The engine and
// transaction keep ownership of the relation; no frame or cleanup is installed.
type tableReadCapture struct {
	owner *ExecCtx
	read  client.CatalogReadStamp
	valid bool
}

func (capture *tableReadCapture) begin(tcc *TxnCompilerContext, op client.TxnOperator) {
	if capture == nil || op == nil {
		return
	}
	owner, current := tcc.statementExecution()
	if owner == nil || owner.proc == nil || owner.reqCtx == nil || context.Cause(owner.reqCtx) != nil || current != op || owner.proc.GetTxnOperator() != op {
		return
	}
	if op.IsSnapOp() {
		return
	}
	stamp, ok := client.TryCatalogReadStamp(op)
	if ok {
		capture.owner = owner
		capture.read = stamp
		capture.valid = true
	}
}

func (capture *tableReadCapture) finish(tcc *TxnCompilerContext, ctx context.Context, relation engine.Relation, ref *plan.ObjectRef, full *plan.TableDef) *plan.BorrowedTable {
	if capture == nil || !capture.valid || ctx == nil || context.Cause(ctx) != nil || ref == nil || full == nil ||
		ref.PubInfo != nil || ref.SubscriptionName != "" || full.IsTemporary {
		return nil
	}
	if _, ok := relation.(engine.IndexPlanningRelation); !ok {
		return nil
	}
	borrow := &plan.BorrowedTable{Relation: relation, Read: capture.read, Owner: capture.owner}
	if !tcc.BorrowedTableStillCurrent(capture.owner.proc, borrow) {
		return nil
	}
	if capture.read.Op.IsSnapOp() {
		return nil
	}
	stamp, ok := client.TryCatalogReadStamp(capture.read.Op)
	if !ok || !capture.read.MatchesRead(stamp, client.CatalogSnapshotSemantic) || context.Cause(ctx) != nil {
		return nil
	}
	return borrow
}

func borrowSnapshotOverride(snapshot *pbplan.Snapshot) bool {
	return snapshot != nil && (plan.IsSnapshotValid(snapshot) || snapshot.Tenant != nil || snapshot.ExtraInfo != nil)
}
