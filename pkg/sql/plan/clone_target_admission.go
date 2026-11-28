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

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type cloneTargetAdmissionKey struct{}

type cloneTargetAdmission struct {
	txn          client.TxnOperator
	database     string
	account      uint32
	tables       map[string]struct{}
	noReferences bool
}

// WithCloneTargetAdmission carries the database and exact table-name locks
// held by the database clone's transaction. noReferences describes the FK
// catalog read after acquiring those locks.
func WithCloneTargetAdmission(ctx context.Context, txn client.TxnOperator, account uint32,
	database string, tables map[string]struct{}, noReferences bool,
) context.Context {
	return context.WithValue(ctx, cloneTargetAdmissionKey{}, &cloneTargetAdmission{
		txn: txn, account: account, database: database, tables: tables, noReferences: noReferences,
	})
}

func cloneTargetAdmissionFor(ctx context.Context, txn client.TxnOperator, account uint32, database, table string) *cloneTargetAdmission {
	admission, _ := ctx.Value(cloneTargetAdmissionKey{}).(*cloneTargetAdmission)
	if admission == nil || admission.txn != txn || admission.database != database || admission.account != account {
		return nil
	}
	if _, ok := admission.tables[table]; !ok {
		return nil
	}
	return admission
}

// OwnsCloneTargetName verifies that the database clone already holds this
// transaction's exact D and T locks. The executor still owns other FK endpoints.
func OwnsCloneTargetName(ctx context.Context, txn client.TxnOperator, account uint32, database, table string) bool {
	return cloneTargetAdmissionFor(ctx, txn, account, database, table) != nil
}

func cloneTargetAdmissionForPlanning(ctx CompilerContext, database, table string) *cloneTargetAdmission {
	if ctx.GetContext().Value(cloneTargetAdmissionKey{}) == nil {
		return nil
	}
	account, err := ctx.GetAccountId()
	if err != nil {
		return nil
	}
	return cloneTargetAdmissionFor(ctx.GetContext(), ctx.GetProcess().GetTxnOperator(), account, database, table)
}

// IndependentCloneTarget requires both the exact admitted name and the frozen
// absence of incoming foreign keys. Source dependencies are checked by clone.
func IndependentCloneTarget(ctx context.Context, txn client.TxnOperator, account uint32, database, table string) bool {
	a := cloneTargetAdmissionFor(ctx, txn, account, database, table)
	return a != nil && a.noReferences
}
