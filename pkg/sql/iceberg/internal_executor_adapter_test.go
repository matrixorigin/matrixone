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
	"math"
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

func TestInternalSQLExecutorAdapterErrorBranches(t *testing.T) {
	ctx := context.Background()
	if err := (InternalSQLExecutorAdapter{}).Exec(ctx, "select 1"); err == nil {
		t.Fatalf("expected nil executor exec error")
	}
	if _, err := (InternalSQLExecutorAdapter{}).Query(ctx, "select 1"); err == nil {
		t.Fatalf("expected nil executor query error")
	}
	if err := (&internalSQLRow{}).Scan(new(string)); err == nil {
		t.Fatalf("expected no rows error")
	}

	rows := &internalSQLRows{}
	if rows.Next() {
		t.Fatalf("empty rows should not advance")
	}
	if err := rows.Scan(new(string)); err == nil {
		t.Fatalf("expected scan before next error")
	}
	if err := scanVectorValue(new(string), nil, 0); err == nil {
		t.Fatalf("expected nil vector error")
	}
	vec := vector.NewVec(types.T_varchar.ToType())
	mp := mpool.MustNewZero()
	defer vec.Free(mp)
	requireNoErr(t, vector.AppendBytes(vec, []byte("x"), false, mp))
	var unsupported struct{}
	if err := scanVectorValue(&unsupported, vec, 0); err == nil {
		t.Fatalf("expected unsupported destination error")
	}
}

func TestInternalSQLExecutorAdapterScanVectorTypeBranches(t *testing.T) {
	mp := mpool.MustNewZero()

	uint8Vec := vector.NewVec(types.T_uint8.ToType())
	defer uint8Vec.Free(mp)
	requireNoErr(t, vector.AppendFixed[uint8](uint8Vec, 8, false, mp))
	uint16Vec := vector.NewVec(types.T_uint16.ToType())
	defer uint16Vec.Free(mp)
	requireNoErr(t, vector.AppendFixed[uint16](uint16Vec, 16, false, mp))
	uint64Vec := vector.NewVec(types.T_uint64.ToType())
	defer uint64Vec.Free(mp)
	requireNoErr(t, vector.AppendFixed[uint64](uint64Vec, 64, false, mp))
	int8Vec := vector.NewVec(types.T_int8.ToType())
	defer int8Vec.Free(mp)
	requireNoErr(t, vector.AppendFixed[int8](int8Vec, 8, false, mp))
	int16Vec := vector.NewVec(types.T_int16.ToType())
	defer int16Vec.Free(mp)
	requireNoErr(t, vector.AppendFixed[int16](int16Vec, 16, false, mp))
	int32Vec := vector.NewVec(types.T_int32.ToType())
	defer int32Vec.Free(mp)
	requireNoErr(t, vector.AppendFixed[int32](int32Vec, 32, false, mp))
	int64Vec := vector.NewVec(types.T_int64.ToType())
	defer int64Vec.Free(mp)
	requireNoErr(t, vector.AppendFixed[int64](int64Vec, 64, false, mp))
	for _, tc := range []struct {
		vec  *vector.Vector
		want uint64
	}{
		{uint8Vec, 8}, {uint16Vec, 16}, {uint64Vec, 64},
		{int8Vec, 8}, {int16Vec, 16}, {int32Vec, 32}, {int64Vec, 64},
	} {
		got, err := scanVectorUint64(tc.vec, 0)
		requireNoErr(t, err)
		if got != tc.want {
			t.Fatalf("unexpected integer scan value got=%d want=%d", got, tc.want)
		}
	}

	negVec := vector.NewVec(types.T_int64.ToType())
	defer negVec.Free(mp)
	requireNoErr(t, vector.AppendFixed[int64](negVec, -1, false, mp))
	if _, err := scanVectorUint64(negVec, 0); err == nil {
		t.Fatalf("expected negative integer error")
	}
	overflowVec := vector.NewVec(types.T_uint64.ToType())
	defer overflowVec.Free(mp)
	requireNoErr(t, vector.AppendFixed[uint64](overflowVec, math.MaxUint32+1, false, mp))
	var small uint32
	if err := scanVectorValue(&small, overflowVec, 0); err == nil {
		t.Fatalf("expected uint32 overflow")
	}
	if _, err := scanVectorString(int64Vec, 0); err == nil {
		t.Fatalf("expected string type error")
	}
	if _, err := scanVectorUint64(vector.NewVec(types.T_varchar.ToType()), 0); err == nil {
		t.Fatalf("expected integer type error")
	}
}

func TestInternalSQLExecutorAdapterScanVectorTimeBranches(t *testing.T) {
	mp := mpool.MustNewZero()
	tsVec := vector.NewVec(types.T_timestamp.ToType())
	defer tsVec.Free(mp)
	requireNoErr(t, vector.AppendFixed[types.Timestamp](tsVec, types.UnixMicroToTimestamp(1767225600000000), false, mp))
	ts, err := scanVectorTime(tsVec, 0)
	requireNoErr(t, err)
	if ts.Year() != 2026 {
		t.Fatalf("unexpected timestamp: %s", ts)
	}

	for _, raw := range []string{"2026-01-01T00:00:00Z", "2026-01-01 00:00:00.123456", "2026-01-01 00:00:00", "2026-01-01", "1767225600000"} {
		vec := vector.NewVec(types.T_varchar.ToType())
		requireNoErr(t, vector.AppendBytes(vec, []byte(raw), false, mp))
		_, err := scanVectorTime(vec, 0)
		vec.Free(mp)
		requireNoErr(t, err)
	}
	bad := vector.NewVec(types.T_varchar.ToType())
	defer bad.Free(mp)
	requireNoErr(t, vector.AppendBytes(bad, []byte("not-a-time"), false, mp))
	if _, err := scanVectorTime(bad, 0); err == nil {
		t.Fatalf("expected invalid timestamp string")
	}
	if _, err := scanVectorTime(vector.NewVec(types.T_bool.ToType()), 0); err == nil {
		t.Fatalf("expected unsupported time vector")
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
