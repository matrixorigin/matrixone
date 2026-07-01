// Copyright 2026 Matrix Origin
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

package iceberg

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	internalexecutor "github.com/matrixorigin/matrixone/pkg/util/executor"
)

func TestInternalSQLExecutorAdapterScansRows(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = vector.NewVec(types.T_uint32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_text.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_datetime.ToType())
	requireNoErr(t, vector.AppendFixed(bat.Vecs[0], uint32(7), false, mp))
	requireNoErr(t, vector.AppendBytes(bat.Vecs[1], []byte("ksa_gold"), false, mp))
	requireNoErr(t, vector.AppendFixed(bat.Vecs[2], types.DatetimeFromUnix(time.UTC, 1767225600), false, mp))
	bat.SetRowCount(1)

	exec := &fakeInternalSQLExecutor{
		result: internalexecutor.Result{Batches: []*batch.Batch{bat}, Mp: mp},
	}
	adapter := InternalSQLExecutorAdapter{Executor: exec}
	var accountID uint32
	var name string
	var ts time.Time
	requireNoErr(t, adapter.QueryRow(context.Background(), "select account_id, name, created_at from mo_catalog.t").Scan(&accountID, &name, &ts))
	if accountID != 7 || name != "ksa_gold" || !ts.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected scanned row account=%d name=%s ts=%s", accountID, name, ts)
	}
	if len(exec.sqls) != 1 || !strings.Contains(exec.sqls[0], "select account_id") {
		t.Fatalf("unexpected executed SQLs: %v", exec.sqls)
	}
	if !exec.options[0].StatementOption().DisableLog() {
		t.Fatalf("adapter should disable SQL logging")
	}
}

func TestInternalSQLExecutorAdapterExec(t *testing.T) {
	exec := &fakeInternalSQLExecutor{}
	err := (InternalSQLExecutorAdapter{Executor: exec}).Exec(context.Background(), "insert into mo_catalog.t values (1)")
	requireNoErr(t, err)
	if len(exec.sqls) != 1 || !strings.HasPrefix(exec.sqls[0], "insert into") {
		t.Fatalf("unexpected executed SQLs: %v", exec.sqls)
	}
	if !exec.options[0].StatementOption().DisableLog() {
		t.Fatalf("adapter should disable SQL logging")
	}
}

type fakeInternalSQLExecutor struct {
	sqls    []string
	options []internalexecutor.Options
	result  internalexecutor.Result
	err     error
}

func (e *fakeInternalSQLExecutor) Exec(ctx context.Context, sql string, opts internalexecutor.Options) (internalexecutor.Result, error) {
	e.sqls = append(e.sqls, sql)
	e.options = append(e.options, opts)
	if e.err != nil {
		return internalexecutor.Result{}, e.err
	}
	return e.result, nil
}

func (e *fakeInternalSQLExecutor) ExecTxn(ctx context.Context, execFunc func(txn internalexecutor.TxnExecutor) error, opts internalexecutor.Options) error {
	return nil
}

func requireNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
