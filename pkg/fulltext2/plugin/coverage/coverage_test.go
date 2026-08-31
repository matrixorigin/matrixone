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

package coverage

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/indexplugin/coverage"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func ts(physical int64) types.TS { return types.BuildTS(physical, 0) }

// logRow is one mo_iscp_log row as the query projects it. dropped is modelled
// by a NULL drop_at, which is what makes the row live.
type logRow struct {
	watermark string
	state     int8
	dropped   bool
}

// mockLog installs an execWithResult that answers with the given rows, and
// restores the real one when the test ends.
func mockLog(t *testing.T, rows []logRow) *string {
	t.Helper()
	mp := mpool.MustNewZero()
	var lastSQL string
	prev := execWithResult
	t.Cleanup(func() { execWithResult = prev })
	execWithResult = func(_ context.Context, sql, _ string, _ client.TxnOperator) (executor.Result, error) {
		lastSQL = sql
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int8.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_timestamp.ToType())
		for _, r := range rows {
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(r.watermark), false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], r.state, false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[2], types.Timestamp(1), !r.dropped, mp))
		}
		bat.SetRowCount(len(rows))
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	return &lastSQL
}

// fakeTxn is a non-nil TxnOperator; the hook only passes it through to the
// (mocked) executor, so no behavior is needed.
type fakeTxn struct{ client.TxnOperator }

func sysCtx() context.Context {
	return context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(7))
}

// A live, running job whose watermark has reached the snapshot is the only
// shape that grants the probe.
func TestCoversSnapshotWatermarkReached(t *testing.T) {
	sql := mockLog(t, []logRow{{watermark: ts(200).ToString(), state: iscpJobStateRunning}})
	r := coverage.Request{CNUUID: "cn0", Txn: fakeTxn{}, TableID: 100,
		IndexDef: &plan.IndexDef{IndexName: "ftj"}, Snapshot: ts(100)}
	covered, err := Hooks{}.CoversSnapshot(sysCtx(), r)
	require.NoError(t, err)
	require.True(t, covered)

	// the tenant is a predicate, not inherited from the context, and the job
	// name is the ISCP identity for the index
	require.Contains(t, *sql, "account_id = 7")
	require.Contains(t, *sql, "table_id = 100")
	require.Contains(t, *sql, "'index_ftj'")
	require.False(t, strings.Contains(*sql, "mo_tables"),
		"the table id comes from the planner; no cross-tenant name resolution")
}

// Equality is coverage: the watermark need only reach the snapshot.
func TestCoversSnapshotWatermarkExactlyAtSnapshot(t *testing.T) {
	mockLog(t, []logRow{{watermark: ts(100).ToString(), state: iscpJobStateCompleted}})
	covered, err := Hooks{}.CoversSnapshot(sysCtx(), coverage.Request{
		CNUUID: "cn0", Txn: fakeTxn{}, TableID: 100,
		IndexDef: &plan.IndexDef{IndexName: "ftj"}, Snapshot: ts(100)})
	require.NoError(t, err)
	require.True(t, covered)
}

// Every way the log can fail to prove freshness must decline. Returning true
// here would silently drop rows from a strongly consistent query.
func TestCoversSnapshotFailsClosed(t *testing.T) {
	cases := []struct {
		name string
		rows []logRow
	}{
		{"no job at all", nil},
		{"watermark behind the snapshot", []logRow{{watermark: ts(50).ToString(), state: iscpJobStateRunning}}},
		{"job pending", []logRow{{watermark: ts(200).ToString(), state: 0}}},
		{"job errored", []logRow{{watermark: ts(200).ToString(), state: 4}}},
		{"job canceled", []logRow{{watermark: ts(200).ToString(), state: 5}}},
		{"unparsable watermark", []logRow{{watermark: "not-a-ts", state: iscpJobStateRunning}}},
		{"empty watermark", []logRow{{watermark: "", state: iscpJobStateRunning}}},
		{"only a dropped job", []logRow{{watermark: ts(200).ToString(), state: iscpJobStateRunning, dropped: true}}},
		{"one of two live jobs behind", []logRow{
			{watermark: ts(200).ToString(), state: iscpJobStateRunning},
			{watermark: ts(50).ToString(), state: iscpJobStateRunning},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mockLog(t, c.rows)
			covered, err := Hooks{}.CoversSnapshot(sysCtx(), coverage.Request{
				CNUUID: "cn0", Txn: fakeTxn{}, TableID: 100,
				IndexDef: &plan.IndexDef{IndexName: "ftj"}, Snapshot: ts(100)})
			require.NoError(t, err)
			require.False(t, covered)
		})
	}
}

// A dropped row alongside a live, current one is ignored rather than poisoning
// the answer: it says nothing about what the index holds now.
func TestCoversSnapshotIgnoresDroppedRows(t *testing.T) {
	mockLog(t, []logRow{
		{watermark: ts(10).ToString(), state: iscpJobStateRunning, dropped: true},
		{watermark: ts(200).ToString(), state: iscpJobStateRunning},
	})
	covered, err := Hooks{}.CoversSnapshot(sysCtx(), coverage.Request{
		CNUUID: "cn0", Txn: fakeTxn{}, TableID: 100,
		IndexDef: &plan.IndexDef{IndexName: "ftj"}, Snapshot: ts(100)})
	require.NoError(t, err)
	require.True(t, covered)
}

// Missing inputs are answered without touching the catalog at all.
func TestCoversSnapshotRejectsIncompleteRequests(t *testing.T) {
	called := false
	prev := execWithResult
	t.Cleanup(func() { execWithResult = prev })
	execWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		called = true
		return executor.Result{}, nil
	}

	full := coverage.Request{CNUUID: "cn0", Txn: fakeTxn{}, TableID: 100,
		IndexDef: &plan.IndexDef{IndexName: "ftj"}, Snapshot: ts(100)}

	noIdx := full
	noIdx.IndexDef = nil
	noTxn := full
	noTxn.Txn = nil
	noTable := full
	noTable.TableID = 0
	noSnap := full
	noSnap.Snapshot = types.TS{}

	for _, r := range []coverage.Request{noIdx, noTxn, noTable, noSnap} {
		covered, err := Hooks{}.CoversSnapshot(sysCtx(), r)
		require.NoError(t, err)
		require.False(t, covered)
	}
	require.False(t, called, "an incomplete request must not query the catalog")
}

// A lookup error is reported, and the caller still sees "not covered".
func TestCoversSnapshotLookupError(t *testing.T) {
	prev := execWithResult
	t.Cleanup(func() { execWithResult = prev })
	execWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("boom")
	}
	covered, err := Hooks{}.CoversSnapshot(sysCtx(), coverage.Request{
		CNUUID: "cn0", Txn: fakeTxn{}, TableID: 100,
		IndexDef: &plan.IndexDef{IndexName: "ftj"}, Snapshot: ts(100)})
	require.Error(t, err)
	require.False(t, covered)
}

// A context with no tenant cannot name the account in the predicate.
func TestCoversSnapshotNoAccount(t *testing.T) {
	mockLog(t, []logRow{{watermark: ts(200).ToString(), state: iscpJobStateRunning}})
	covered, err := Hooks{}.CoversSnapshot(context.Background(), coverage.Request{
		CNUUID: "cn0", Txn: fakeTxn{}, TableID: 100,
		IndexDef: &plan.IndexDef{IndexName: "ftj"}, Snapshot: ts(100)})
	require.Error(t, err)
	require.False(t, covered)
}

func TestJobNameForIndex(t *testing.T) {
	// must match compile.genCdcTaskJobID
	require.Equal(t, "index_ftj", jobNameForIndex("ftj"))
}

// parseWatermark must never panic, whatever the catalog holds: types.StringToTS
// does, and this runs inside the planner.
func TestParseWatermark(t *testing.T) {
	got, ok := parseWatermark("123-4")
	require.True(t, ok)
	require.Equal(t, types.BuildTS(123, 4), got)

	for _, bad := range []string{
		"", "-", "abc", "abc-1", "1-abc", "123", "1-2-3",
		"1-99999999999", // logical overflows uint32
		"99999999999999999999-0",
		"0-0", // the zero TS is no evidence of anything
	} {
		_, ok := parseWatermark(bad)
		require.False(t, ok, bad)
	}
}
