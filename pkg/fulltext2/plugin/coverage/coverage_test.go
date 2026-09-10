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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func ts(physical int64) types.TS { return types.BuildTS(physical, 0) }

// logRow models one mo_iscp_log liveness row. A NULL drop_at (dropped=false) is what
// makes the row live.
type logRow struct {
	state   int8
	dropped bool
}

// mockGate installs an execWithResult routing the gate's two reads: the mo_iscp_log
// liveness query (job_state, drop_at) and the metadata MAX(build_ts) query (int64).
// It returns a pointer to the last mo_iscp_log SQL seen. durableBuildTS is what a cold
// cache reads from the metadata table.
func mockGate(t *testing.T, jobs []logRow, durableBuildTS int64) *string {
	t.Helper()
	mp := mpool.MustNewZero()
	var iscpSQL string
	prev := execWithResult
	t.Cleanup(func() { execWithResult = prev })
	execWithResult = func(_ context.Context, sql, _ string, _ client.TxnOperator) (executor.Result, error) {
		if strings.Contains(sql, "mo_iscp_log") {
			iscpSQL = sql
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_int8.ToType())      // job_state
			bat.Vecs[1] = vector.NewVec(types.T_timestamp.ToType()) // drop_at
			for _, r := range jobs {
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], r.state, false, mp))
				require.NoError(t, vector.AppendFixed(bat.Vecs[1], types.Timestamp(1), !r.dropped, mp))
			}
			bat.SetRowCount(len(jobs))
			return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
		}
		// metadata MAX(build_ts)
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], durableBuildTS, false, mp))
		bat.SetRowCount(1)
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
	}
	return &iscpSQL
}

// fakeTxn is a non-nil TxnOperator; the hook only passes it through to the
// (mocked) executor, so no behavior is needed.
type fakeTxn struct{ client.TxnOperator }

func (fakeTxn) SnapshotTS() timestamp.Timestamp { return timestamp.Timestamp{} }

// CloneSnapshotOp is called on the cold historical path; the mocked executor ignores
// the operator, so returning the same fake is enough.
func (f fakeTxn) CloneSnapshotOp(timestamp.Timestamp) client.TxnOperator { return f }

func sysCtx() context.Context {
	return context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(7))
}

// gateReq builds a complete request with the hidden tables resolved. With an empty
// process cache the gate takes the cold path and reads MAX(build_ts) from the metadata.
func gateReq(sourceCommit int64) coverage.Request {
	return coverage.Request{
		CNUUID: "cn0", Txn: fakeTxn{}, TableID: 100,
		IndexDef:           &plan.IndexDef{IndexName: "ftj"},
		SourceCommitTS:     ts(sourceCommit),
		IndexStorageTable:  "ftj_index",
		IndexMetadataDB:    "db",
		IndexMetadataTable: "ftj_meta",
	}
}

// A live, running job whose (cold-cache) build_ts has reached the source commit is the
// only shape that grants the probe.
func TestCoversSnapshotCovered(t *testing.T) {
	sql := mockGate(t, []logRow{{state: iscpJobStateRunning}}, 200)
	covered, err := Hooks{}.CoversSnapshot(sysCtx(), gateReq(100))
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

// Equality is coverage: build_ts need only reach the source commit.
func TestCoversSnapshotBuildTSExactlyAtSource(t *testing.T) {
	mockGate(t, []logRow{{state: iscpJobStateCompleted}}, 100)
	covered, err := Hooks{}.CoversSnapshot(sysCtx(), gateReq(100))
	require.NoError(t, err)
	require.True(t, covered)
}

// Every way freshness fails to prove must decline. Returning true here would silently
// drop rows from a strongly consistent query.
func TestCoversSnapshotFailsClosed(t *testing.T) {
	cases := []struct {
		name           string
		jobs           []logRow
		durableBuildTS int64
	}{
		{"no job at all", nil, 200},
		{"build_ts behind the source commit", []logRow{{state: iscpJobStateRunning}}, 50},
		{"build_ts unknown (pre-migration / read failed)", []logRow{{state: iscpJobStateRunning}}, 0},
		{"job pending", []logRow{{state: 0}}, 200},
		{"job errored", []logRow{{state: 4}}, 200},
		{"job canceled", []logRow{{state: 5}}, 200},
		{"only a dropped job", []logRow{{state: iscpJobStateRunning, dropped: true}}, 200},
		{"one of two jobs not running", []logRow{
			{state: iscpJobStateRunning},
			{state: 4},
		}, 200},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mockGate(t, c.jobs, c.durableBuildTS)
			covered, err := Hooks{}.CoversSnapshot(sysCtx(), gateReq(100))
			require.NoError(t, err)
			require.False(t, covered)
		})
	}
}

// A historical ({snapshot=...}) read's bar is the snapshot TS, not SourceCommitTS: with
// build_ts (200) past the snapshot (50) it is covered even though SourceCommitTS (300)
// exceeds build_ts -- SourceCommitTS is ignored for a snapshot read. The cold-cache
// metadata read runs on a cloned txn.
func TestCoversSnapshotHistorical(t *testing.T) {
	mockGate(t, []logRow{{state: iscpJobStateRunning}}, 200)
	r := gateReq(300) // SourceCommitTS deliberately > build_ts; must be ignored
	histTS := timestamp.Timestamp{PhysicalTime: 50}
	r.ScanSnapshotTS = &histTS
	covered, err := Hooks{}.CoversSnapshot(sysCtx(), r)
	require.NoError(t, err)
	require.True(t, covered)
}

// A historical read declines when build_ts has not reached the snapshot TS.
func TestCoversSnapshotHistoricalBehind(t *testing.T) {
	mockGate(t, []logRow{{state: iscpJobStateRunning}}, 50)
	r := gateReq(0) // no SourceCommitTS at all; the bar is the snapshot
	histTS := timestamp.Timestamp{PhysicalTime: 100}
	r.ScanSnapshotTS = &histTS
	covered, err := Hooks{}.CoversSnapshot(sysCtx(), r)
	require.NoError(t, err)
	require.False(t, covered)
}

// A dropped row alongside a live one is ignored rather than poisoning the answer:
// it says nothing about what the index holds now.
func TestCoversSnapshotIgnoresDroppedRows(t *testing.T) {
	mockGate(t, []logRow{
		{state: iscpJobStateRunning, dropped: true},
		{state: iscpJobStateRunning},
	}, 200)
	covered, err := Hooks{}.CoversSnapshot(sysCtx(), gateReq(100))
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
		IndexDef: &plan.IndexDef{IndexName: "ftj"}, SourceCommitTS: ts(100)}

	noIdx := full
	noIdx.IndexDef = nil
	noTxn := full
	noTxn.Txn = nil
	noTable := full
	noTable.TableID = 0
	// An empty source commit ts is no evidence: any watermark is >= zero, so
	// accepting it would fail open. It must decline before touching the catalog.
	noSourceTS := full
	noSourceTS.SourceCommitTS = types.TS{}
	for _, r := range []coverage.Request{noIdx, noTxn, noTable, noSourceTS} {
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
	covered, err := Hooks{}.CoversSnapshot(sysCtx(), gateReq(100))
	require.Error(t, err)
	require.False(t, covered)
}

// A context with no tenant cannot name the account in the predicate.
func TestCoversSnapshotNoAccount(t *testing.T) {
	mockGate(t, []logRow{{state: iscpJobStateRunning}}, 200)
	covered, err := Hooks{}.CoversSnapshot(context.Background(), gateReq(100))
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

// IndexBuildTS returns the searched generation's build_ts for the planner's gap sizing: zero for an
// incomplete request, else the cold-path MAX(build_ts) from the metadata table. It does not check
// liveness (that is CoversSnapshot's job).
func TestIndexBuildTS(t *testing.T) {
	// an incomplete request yields the zero TS (fail closed)
	zero := Hooks{}.IndexBuildTS(sysCtx(), coverage.Request{})
	require.True(t, zero.IsEmpty())

	// cold cache -> MAX(build_ts) from the metadata table (liveness rows are irrelevant here)
	mockGate(t, nil, 4242)
	bts := Hooks{}.IndexBuildTS(sysCtx(), gateReq(0))
	require.Equal(t, int64(4242), bts.Physical())
}
