// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
package defines

import (
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type materializedViewWriteKey struct{}
type materializedViewWrite struct {
	account uint32
	table   uint64
	txn     string
}

// WithMaterializedViewWrite is an internal, transaction-bound capability. There
// is deliberately no SQL/session variable that can construct this context value.
func WithMaterializedViewWrite(ctx context.Context, account uint32, table uint64, txnID []byte) context.Context {
	return context.WithValue(ctx, materializedViewWriteKey{}, materializedViewWrite{account, table, string(txnID)})
}

func CanWriteMaterializedView(ctx context.Context, account uint32, table uint64, txnID []byte) bool {
	v, ok := ctx.Value(materializedViewWriteKey{}).(materializedViewWrite)
	return ok && table != 0 && len(txnID) != 0 && v.account == account && v.table == table && v.txn == string(txnID)
}

// HasMaterializedViewWrite permits definition-time DML binding. Execution must
// additionally verify the transaction identity through CanWriteMaterializedView.
func HasMaterializedViewWrite(ctx context.Context, account uint32, table uint64) bool {
	v, ok := ctx.Value(materializedViewWriteKey{}).(materializedViewWrite)
	return ok && v.account == account && v.table == table && table != 0
}

// MaterializedViewTask is supplied only by the typed background task executor.
// Object identity and epoch prevent stale tasks from resolving a recreated name.
type MaterializedViewTask struct {
	Account      uint32
	Table, Epoch uint64
	RunID        string
}
type materializedViewTaskKey struct{}

func WithMaterializedViewTask(ctx context.Context, task MaterializedViewTask) context.Context {
	return context.WithValue(ctx, materializedViewTaskKey{}, task)
}
func GetMaterializedViewTask(ctx context.Context) (MaterializedViewTask, bool) {
	task, ok := ctx.Value(materializedViewTaskKey{}).(MaterializedViewTask)
	return task, ok
}

func MaterializedViewWriteActive(ctx context.Context) bool {
	v, ok := ctx.Value(materializedViewWriteKey{}).(materializedViewWrite)
	return ok && v.table != 0 && v.txn != ""
}

type materializedViewBudgetKey struct{}
type materializedViewBudget struct {
	table uint64
	used  atomic.Int64
	limit int64
}

func WithMaterializedViewBudget(ctx context.Context, table uint64, limit int64) context.Context {
	return context.WithValue(ctx, materializedViewBudgetKey{}, &materializedViewBudget{table: table, limit: limit})
}

// ChargeMaterializedViewOutput shares a counter across local parallel writers.
// Ordinary writers have no counter, and DROP/DELETE never charge inserted bytes.
func ChargeMaterializedViewOutput(ctx context.Context, table uint64, bytes int) error {
	budget, ok := ctx.Value(materializedViewBudgetKey{}).(*materializedViewBudget)
	if !ok || budget.table != table {
		return nil
	}
	if bytes < 0 || budget.used.Add(int64(bytes)) > budget.limit {
		return moerr.NewInvalidInput(ctx, "materialized view output budget exceeded")
	}
	return nil
}
