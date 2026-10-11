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

package iscp

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// collectChangesProbeRelation records the CollectChanges options the engine
// actually receives. Only CollectChanges is implemented; the rest of the
// embedded interface is unreachable here.
type collectChangesProbeRelation struct {
	engine.Relation
	calls           int
	gotSkipDeletes  bool
	gotKeepTrailing bool
	gotPreserveAll  bool
}

func (r *collectChangesProbeRelation) CollectChanges(
	ctx context.Context, from, to types.TS, skipDeletes bool, mp *mpool.MPool,
) (engine.ChangesHandle, error) {
	r.calls++
	r.gotSkipDeletes = skipDeletes
	r.gotKeepTrailing = engine.CollectChangesKeepTrailingDeletesFromContext(ctx)
	r.gotPreserveAll = engine.CollectChangesPreserveAllVersionsFromContext(ctx)
	return nil, nil
}

// TestCollectChangesForwardsCallerPolicy proves the shared wrapper adds no read
// policy of its own: it forwards the caller's context, so only the iteration
// that owns the durable job state can opt a range into keeping trailing deletes
// (#28847). An unconditional opt-in here would also apply to the mo_iscp_log
// self-scan in applyISCPLogWithRel, which maintains no copied index at all.
func TestCollectChangesForwardsCallerPolicy(t *testing.T) {
	from, to := types.BuildTS(1, 0), types.BuildTS(2, 0)

	plain := &collectChangesProbeRelation{}
	_, err := CollectChanges(context.Background(), plain, from, to, nil)
	require.NoError(t, err)
	require.Equal(t, 1, plain.calls)
	require.False(t, plain.gotKeepTrailing,
		"a steady-state range must keep coalescing insert-then-delete pairs")
	require.False(t, plain.gotSkipDeletes,
		"index maintenance still needs deletes; only the net-effect coalescing changes")

	optedIn := &collectChangesProbeRelation{}
	ctx := engine.WithCollectChangesKeepTrailingDeletes(
		engine.WithCollectChangesPreserveAllVersions(context.Background()))
	_, err = CollectChanges(ctx, optedIn, from, to, nil)
	require.NoError(t, err)
	require.True(t, optedIn.gotKeepTrailing, "the caller's policy reaches the engine")
	require.True(t, optedIn.gotPreserveAll, "and every other caller option survives")
}

// TestCollectChangesCtxForJobs pins the opt-in window for #28847: only a job
// generation seeded by InitSQL whose materialized state may still predate the
// range start keeps the trailing DELETE of an insert-then-delete key. Seeding
// happens once (RestoreTable / CREATE TABLE ... CLONE / DATA BRANCH copy an
// already-built index and re-arm CDC at the DDL transaction's PRE-commit
// snapshot TS), so every later range coalesces exactly as before and no consumer
// accumulates extra durable deletes.
//
// The committed column is the durable mo_iscp_log.watermark. It is the only
// progress the sink transaction itself writes, so a range the executor merely
// SELECTED -- job_status.To persisted with ISCPJobState_Running before the
// consumers start -- must not close the window.
func TestCollectChangesCtxForJobs(t *testing.T) {
	seeded := &JobSpec{ConsumerInfo: ConsumerInfo{InitSQL: "U0VMRUNUIDE="}}
	built := &JobSpec{ConsumerInfo: ConsumerInfo{InitSQL: ""}}
	seedWatermark := types.BuildTS(5, 0)
	selected := types.BuildTS(10, 0)
	rangeEnd := types.BuildTS(30, 0)

	cases := []struct {
		name       string
		jobSpecs   []*JobSpec
		prevStatus []*JobStatus
		committed  []types.TS
		want       bool
	}{
		{
			// DATA BRANCH of a copied index: InitSQL armed, no range persisted yet.
			name:       "seeded generation before its first collected range",
			jobSpecs:   []*JobSpec{seeded},
			prevStatus: []*JobStatus{{Stage: JobStage_Running}},
			committed:  []types.TS{seedWatermark},
			want:       true,
		},
		{
			// The InitSQL iteration itself never reaches CollectChanges, but the
			// predicate must not depend on that: Stage is not part of the window.
			name:       "seeded generation still in Init stage",
			jobSpecs:   []*JobSpec{seeded},
			prevStatus: []*JobStatus{{Stage: JobStage_Init}},
			committed:  []types.TS{seedWatermark},
			want:       true,
		},
		{
			// A stop between the Running flush and the consumers: job_status
			// already carries the selected range with a zero ErrorCode, and
			// repairAbandonedJobs completes the row without consuming anything.
			// The watermark still sits at the range start, so the materialized
			// state still predates it and the retry must keep the tombstone.
			name:     "seeded generation stopped before any sink work",
			jobSpecs: []*JobSpec{seeded},
			prevStatus: []*JobStatus{{
				Stage: JobStage_Running, From: selected, To: rangeEnd,
			}},
			committed: []types.TS{selected},
			want:      true,
		},
		{
			// Steady state: the sink transaction advanced the watermark to the
			// range end, so the consumer state matches the next range start and
			// insert-then-delete churn cancels out. This is the bound the
			// append-only cuVS sinks rely on: their cdc_tail retains every
			// distinct deleted pk while CuvsUpdatable counts inserts and upserts
			// only.
			name:     "seeded generation after its first committed range",
			jobSpecs: []*JobSpec{seeded},
			prevStatus: []*JobStatus{{
				Stage: JobStage_Running, From: seedWatermark, To: selected,
			}},
			committed: []types.TS{selected},
			want:      false,
		},
		{
			// A maintenance watermark flush advances the column without replacing
			// job_status, so a committed generation can read watermark > To.
			name:     "seeded generation whose watermark a maintenance flush passed",
			jobSpecs: []*JobSpec{seeded},
			prevStatus: []*JobStatus{{
				Stage: JobStage_Running, From: seedWatermark, To: selected,
			}},
			committed: []types.TS{rangeEnd},
			want:      false,
		},
		{
			// A failed attempt may have been applied only partially, so its range
			// is retried from the same lagging state.
			name:     "seeded generation retrying a failed range",
			jobSpecs: []*JobSpec{seeded},
			prevStatus: []*JobStatus{{
				Stage: JobStage_Running, From: seedWatermark, To: selected,
				ErrorCode: 20601, ErrorMsg: "sink consume failed",
			}},
			committed: []types.TS{seedWatermark},
			want:      true,
		},
		{
			// The retry arm cannot be folded into the watermark comparison: a
			// clean-table pass advances the column past a job_status the failed
			// flush left behind, yet that range was never applied.
			name:     "failed range whose watermark a maintenance flush already passed",
			jobSpecs: []*JobSpec{seeded},
			prevStatus: []*JobStatus{{
				Stage: JobStage_Running, From: seedWatermark, To: selected,
				ErrorCode: 20601, ErrorMsg: "sink consume failed",
			}},
			committed: []types.TS{rangeEnd},
			want:      true,
		},
		{
			// An inline-built index (fulltext2 CREATE INDEX) has no InitSQL: its
			// base is built at the very snapshot its CDC is armed from.
			name:       "inline-built generation before its first range",
			jobSpecs:   []*JobSpec{built},
			prevStatus: []*JobStatus{{Stage: JobStage_Running}},
			committed:  []types.TS{seedWatermark},
			want:       false,
		},
		{
			name:     "inline-built generation retrying a failed range",
			jobSpecs: []*JobSpec{built},
			prevStatus: []*JobStatus{{
				To: selected, ErrorCode: 20601,
			}},
			committed: []types.TS{seedWatermark},
			want:      false,
		},
		{
			// One shared range feeds every job of the iteration, so a single
			// lagging member opts the whole read in.
			name:       "shared iteration with one lagging seeded member",
			jobSpecs:   []*JobSpec{built, seeded},
			prevStatus: []*JobStatus{{To: selected}, {Stage: JobStage_Running}},
			committed:  []types.TS{selected, seedWatermark},
			want:       true,
		},
		{
			name:       "no jobs",
			jobSpecs:   nil,
			prevStatus: nil,
			committed:  nil,
			want:       false,
		},
		{
			name:       "nil spec and status are not a lagging state",
			jobSpecs:   []*JobSpec{nil, seeded},
			prevStatus: []*JobStatus{{Stage: JobStage_Running}, nil},
			committed:  []types.TS{seedWatermark, seedWatermark},
			want:       false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := collectChangesCtxForJobs(
				context.Background(), tc.jobSpecs, tc.prevStatus, tc.committed)
			require.Equal(t, tc.want, engine.CollectChangesKeepTrailingDeletesFromContext(ctx))
		})
	}
}

// TestSeedWindowAcrossAbandonedRunningRow drives the durable row round trip the
// abandoned-Running counterexample depends on, through the production encoder,
// the production SELECT/decode and the production predicate: the executor
// persists the selected range with ISCPJobState_Running before the consumers
// run, a stop there leaves job_status claiming a range nothing applied, and only
// the sink transaction's UpdateWatermark moves the watermark to that To.
func TestSeedWindowAcrossAbandonedRunningRow(t *testing.T) {
	oldExecWithResult := ExecWithResult
	defer func() {
		ExecWithResult = oldExecWithResult
	}()

	const jobName = "index_copied_ft2"
	const jobID = uint64(7)

	specJSON, err := MarshalJobSpec(&JobSpec{ConsumerInfo: ConsumerInfo{
		IndexName: "copied_ft2",
		// registerJob persists InitSQL base64-encoded, so the durable row and
		// everything decoded from it carries this form.
		InitSQL: base64.StdEncoding.EncodeToString([]byte("SELECT 1")),
	}})
	require.NoError(t, err)

	from := types.BuildTS(10, 0)
	to := types.BuildTS(30, 0)

	// Exactly what ExecuteIterationWithRuntime builds for the range and stamps
	// with the iteration LSN before the pre-consumer Running flush persists it.
	selected := normalizeJobStatuses([]*JobStatus{{
		From: from, To: to, Stage: JobStage_Running, StartAt: from,
	}}, []uint64{4})
	statusJSON, err := MarshalJobStatus(selected[0])
	require.NoError(t, err)

	keepTrailingDeletes := func(watermark types.TS) bool {
		result, mp := newISCPLogResult(t, []iscpLogBatch{{
			jobNames:   []string{jobName},
			jobIDs:     []uint64{jobID},
			jobSpecs:   []string{specJSON},
			jobStatuss: []string{statusJSON},
			watermarks: []string{watermark.ToString()},
		}})
		defer func() {
			require.Equal(t, int64(0), mp.CurrNB())
			mpool.DeleteMPool(mp)
		}()
		ExecWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
			return result, nil
		}

		jobSpecs, prevStatus, committed, err := GetJobSpecs(
			context.Background(), "", nil, nil, nil, 0, 42,
			[]string{jobName}, []uint64{4}, types.TS{},
			make([]*JobStatus, 1), []uint64{jobID},
		)
		require.NoError(t, err)
		require.Equal(t, to, prevStatus[0].To, "the selected range survives the row round trip")
		require.Zero(t, prevStatus[0].ErrorCode, "an abandoned Running row records no error")
		require.Equal(t, watermark, committed[0], "the durable watermark reaches the gate")

		ctx := collectChangesCtxForJobs(context.Background(), jobSpecs, prevStatus, committed)
		return engine.CollectChangesKeepTrailingDeletesFromContext(ctx)
	}

	// The Running flush wrote watermark = fromTS and no consumer ran; a stop here
	// leaves exactly this row behind, and repairAbandonedJobs only rewrites
	// job_state, which the window does not read.
	require.True(t, keepTrailingDeletes(from),
		"a range that was selected but never consumed must keep delivering the tombstone")

	// The sink transaction's UpdateWatermark is the commit boundary.
	require.False(t, keepTrailingDeletes(to),
		"a committed range must coalesce again so no sink accumulates durable deletes")
}

// TestCollectChangesCtxForJobsKeepsCallerContext proves the opt-in chains onto
// the iteration's own context instead of replacing it: the range read still
// carries the account and deadline the caller attached.
func TestCollectChangesCtxForJobsKeepsCallerContext(t *testing.T) {
	type ctxKey struct{}
	parent := context.WithValue(context.Background(), ctxKey{}, "iteration")
	parent = engine.WithCollectChangesPreserveAllVersions(parent)

	seedWatermark := types.BuildTS(5, 0)
	seeded := []*JobSpec{{ConsumerInfo: ConsumerInfo{InitSQL: "U0VMRUNUIDE="}}}
	lagging := []*JobStatus{{Stage: JobStage_Running}}

	got := collectChangesCtxForJobs(parent, seeded, lagging, []types.TS{seedWatermark})
	require.True(t, engine.CollectChangesKeepTrailingDeletesFromContext(got))
	require.True(t, engine.CollectChangesPreserveAllVersionsFromContext(got))
	require.Equal(t, "iteration", got.Value(ctxKey{}))

	// A steady-state generation returns the caller's context untouched.
	settled := []*JobStatus{{Stage: JobStage_Running, From: seedWatermark, To: types.BuildTS(10, 0)}}
	require.Equal(t, parent, collectChangesCtxForJobs(
		parent, seeded, settled, []types.TS{types.BuildTS(10, 0)}))
}
