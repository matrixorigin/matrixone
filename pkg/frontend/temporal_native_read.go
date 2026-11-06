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
	"reflect"

	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ plan.BorrowingTableResolver = (*TxnCompilerContext)(nil)

// planningResolver accepts only the actual frontend compiler, not a wrapper
// whose Resolve may enforce a different tenant or record dependencies.
func planningResolver(compiler plan.CompilerContext) plan.BorrowingTableResolver {
	tcc, ok := compiler.(*TxnCompilerContext)
	if !ok || tcc == nil {
		return nil
	}
	return tcc
}

func (tcc *TxnCompilerContext) IsSameCompiler(compiler plan.CompilerContext) bool {
	actual, ok := compiler.(*TxnCompilerContext)
	return ok && actual != nil && actual == tcc
}

func (tcc *TxnCompilerContext) temporalExecution() (*ExecCtx, client.TxnOperator) {
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

func (tcc *TxnCompilerContext) BorrowStillCurrent(compiler plan.CompilerContext, proc *process.Process, borrow *plan.BorrowedTable) bool {
	if !tcc.IsSameCompiler(compiler) || proc == nil || borrow == nil || borrow.Relation == nil || borrow.Read.Op == nil {
		return false
	}
	owner, ok := borrow.Owner.(*ExecCtx)
	if !ok || owner == nil || owner.proc != proc || owner.reqCtx == nil || context.Cause(owner.reqCtx) != nil {
		return false
	}
	current, op := tcc.temporalExecution()
	return current == owner && op == borrow.Read.Op && proc.GetTxnOperator() == op
}

func readTemporalNativeStamp(op client.TxnOperator) (client.CatalogReadStamp, uint64, bool) {
	reader, ok := op.(interface {
		TryCatalogReadStamp() (client.CatalogReadStamp, bool)
	})
	if !ok || op.IsSnapOp() {
		return client.CatalogReadStamp{}, 0, false
	}
	stamp, ok := reader.TryCatalogReadStamp()
	if !ok {
		return client.CatalogReadStamp{}, 0, false
	}
	workspace, ok := stamp.Workspace.(interface{ CatalogVisibility() (uint64, bool) })
	if !ok || reflect.TypeOf(stamp.Workspace).Kind() != reflect.Pointer {
		return client.CatalogReadStamp{}, 0, false
	}
	visibility, stable := workspace.CatalogVisibility()
	return stamp, visibility, stable
}

// temporalReadCapture is lexical to one ordinary resolver call. The engine and
// transaction keep ownership of the relation; no frame or cleanup is installed.
type temporalReadCapture struct {
	owner *ExecCtx
	read  engine.TemporalRead
	valid bool
}

func (capture *temporalReadCapture) begin(tcc *TxnCompilerContext, op client.TxnOperator) {
	if capture == nil || op == nil {
		return
	}
	owner, current := tcc.temporalExecution()
	if owner == nil || owner.proc == nil || owner.reqCtx == nil || context.Cause(owner.reqCtx) != nil || current != op || owner.proc.GetTxnOperator() != op {
		return
	}
	stamp, visibility, ok := readTemporalNativeStamp(op)
	if ok {
		capture.owner = owner
		capture.read = engine.TemporalRead{Op: op, Stamp: stamp, Visibility: visibility}
		capture.valid = true
	}
}

func (capture *temporalReadCapture) finish(tcc *TxnCompilerContext, ctx context.Context, relation engine.Relation, ref *plan.ObjectRef, full *plan.TableDef) *plan.BorrowedTable {
	if capture == nil || !capture.valid || ctx == nil || context.Cause(ctx) != nil || ref == nil || full == nil ||
		ref.PubInfo != nil || ref.SubscriptionName != "" || full.IsTemporary {
		return nil
	}
	if _, ok := relation.(engine.TemporalIndexRelation); !ok {
		return nil
	}
	borrow := &plan.BorrowedTable{Relation: relation, Read: capture.read, Owner: capture.owner}
	if !tcc.BorrowStillCurrent(tcc, capture.owner.proc, borrow) {
		return nil
	}
	stamp, visibility, ok := readTemporalNativeStamp(capture.read.Op)
	if !ok || visibility != capture.read.Visibility || stamp.TransactionID != capture.read.Stamp.TransactionID ||
		!stamp.Snapshot.Equal(capture.read.Stamp.Snapshot) || stamp.Revision != capture.read.Stamp.Revision || stamp.Workspace != capture.read.Stamp.Workspace || context.Cause(ctx) != nil {
		return nil
	}
	return borrow
}

func temporalNativeSnapshotOverride(snapshot *pbplan.Snapshot) bool {
	return snapshot != nil && (plan.IsSnapshotValid(snapshot) || snapshot.Tenant != nil || snapshot.ExtraInfo != nil)
}
