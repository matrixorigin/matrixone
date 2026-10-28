// Copyright 2026 MatrixOne
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
	"fmt"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbstats "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	pbtimestamp "github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func markedLockingReadPlan() *planpb.Plan {
	return &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		Nodes: []*planpb.Node{{NodeType: planpb.Node_LOCK_OP,
			LockTargets: []*planpb.LockTarget{{TableId: 42, PrimaryColIdxInBat: 0, LockingRead: true}}}}}}}
}

func legacyLockingReadPlan() *planpb.Plan {
	return &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		Nodes: []*planpb.Node{{NodeType: planpb.Node_LOCK_OP,
			LockTargets: []*planpb.LockTarget{{TableId: 42, PrimaryColIdxInBat: 0}}}}}}}
}

func markedPlanHasPresence(pn *planpb.Plan) bool {
	qry, _ := plan2.PlanLockingReadQuery(pn)
	return plan2.PlanHasLockingCurrentRead(qry)
}

func newLockingCacheTransitionSession(t *testing.T) (*Session, *UserInput, func(), func(int64)) {
	t.Helper()
	const service = "locking-capability-cache-transition"
	InitServerLevelVars(service)
	t.Cleanup(func() { serverVarsMap.Delete(service) })

	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	require.NotNil(t, rt)
	orig, had := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, orig)
		}
	})
	setCap := func(version int64) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
	}

	ses := &Session{
		feSessionImpl: feSessionImpl{service: service},
		planCache:     newPlanCache(8),
		proc:          proc,
	}
	input := &UserInput{sql: "select a from index_hint_t where a = 1 for update"}
	input.genHash()
	return ses, input, func() {}, setCap
}

// Pre-PR review round 4, finding 4: the capability-transition proof must drive
// the REAL session-cache machinery — publication, guarded lookup, eviction —
// in BOTH directions, not re-validation of one synthetic plan. This member
// drives cachedPlanForInput against the production planCache through the
// full transition sequence: above the allocated gate the marked entry is
// published, retained and REUSED (the same entry object served twice); the
// capability drop evicts it eagerly behind the lookup guards; the rise
// restores marked reuse after republication; and an UNMARKED (legacy) entry
// is conserved in every state — below the gate the cache behaves exactly as
// today (uniform legacy execution).
func TestSessionCacheServesAndEvictsMarkedPlanAcrossCapabilityTransitions(t *testing.T) {
	ses, input, teardown, setCap := newLockingCacheTransitionSession(t)
	defer teardown()

	// ABOVE the allocated gate: publication records the live negotiated
	// protocol version, and the marked entry is retained and REUSED.
	setCap(defines.MORPCVersion135)
	marked := markedLockingReadPlan()
	require.True(t, markedPlanHasPresence(marked), "the fixture carries bind-time presence")
	stmt := &trackedStatement{}
	ses.cachePlanWithStatsVersions(input.getHash(),
		[]tree.Statement{stmt}, []*planpb.Plan{marked}, map[optimizerStatsTableKey]uint64{})
	require.True(t, ses.isCached(input.getHash()))
	entry := ses.getCachedPlan(input.getHash())
	require.NotNil(t, entry)
	require.Equal(t, defines.MORPCVersion135, entry.protocolVersion,
		"publication records the live negotiated version (the rebuild decision's input)")
	require.Zero(t, stmt.freed)

	first := cachedPlanForInput(ses, input)
	require.NotNil(t, first, "above the gate the marked entry is served")
	second := cachedPlanForInput(ses, input)
	require.NotNil(t, second, "the marked entry is served again: actual retained reuse")
	require.Same(t, first, second)
	require.Same(t, marked, first.plans[0])
	require.Zero(t, stmt.freed, "retained reuse frees nothing")

	// THE DROP (the check side of the frontend/Run seam): the guarded lookup
	// evicts the marked entry eagerly — a marked plan never RE-EXECUTES under
	// a lowered gate; the next execution rebinds through the rebuild path.
	setCap(128)
	require.Nil(t, cachedPlanForInput(ses, input),
		"after the drop the marked entry is refused at lookup — evicted, not degraded")
	require.False(t, ses.isCached(input.getHash()))
	require.Equal(t, 1, stmt.freed,
		"eviction releases the cached entry's owned AST eagerly")

	// A protocol-current marked entry below the gate exercises the MARKING
	// guard itself (D-8 layer 2): defense in depth behind the version guard,
	// for any entry that reaches the cache recorded at the lowered version
	// while still carrying presence (the mid-drop window).
	stmtMid := &trackedStatement{}
	ses.cachePlanWithSnapshotsAndStatsVersions(input.getHash(),
		[]tree.Statement{stmtMid}, []*planpb.Plan{markedLockingReadPlan()},
		make([]pbtimestamp.Timestamp, 1), make([]map[optimizerStatsTableKey]uint64, 1),
		int64(128))
	require.True(t, ses.isCached(input.getHash()))
	require.Nil(t, cachedPlanForInput(ses, input),
		"presence below the allocated gate is evicted even when the recorded version is current")
	require.False(t, ses.isCached(input.getHash()))
	require.Equal(t, 1, stmtMid.freed)

	// The UNMARKED legacy entry is conserved across the same states: below
	// the gate it is served untouched — byte-identical legacy behavior.
	stmtLegacy := &trackedStatement{}
	ses.cachePlanWithSnapshotsAndStatsVersions(input.getHash(),
		[]tree.Statement{stmtLegacy}, []*planpb.Plan{legacyLockingReadPlan()},
		make([]pbtimestamp.Timestamp, 1), make([]map[optimizerStatsTableKey]uint64, 1),
		int64(128))
	require.True(t, ses.isCached(input.getHash()))
	served := cachedPlanForInput(ses, input)
	require.NotNil(t, served, "an unmarked entry below the gate is served: uniform legacy execution")
	require.Zero(t, stmtLegacy.freed)

	// THE RISE (both directions): the legacy entry recorded at 128 mismatches
	// the risen protocol version and is revalidated away; a freshly published
	// marked entry under the risen gate is served again.
	setCap(defines.MORPCVersion135)
	require.Nil(t, ses.getCachedPlan(input.getHash()),
		"the risen protocol version revalidates the below-gate recording away — EXECUTE rebuilds")
	stmtRise := &trackedStatement{}
	ses.cachePlanWithStatsVersions(input.getHash(),
		[]tree.Statement{stmtRise}, []*planpb.Plan{markedLockingReadPlan()},
		map[optimizerStatsTableKey]uint64{})
	served = cachedPlanForInput(ses, input)
	require.NotNil(t, served, "after the rise a newly published marked entry is reused again")
	require.Same(t, stmtRise, served.stmts[0])
}

// Pre-PR review round 6, finding 3: the round's predecessor asserted the
// pure rebuild predicate over local constants and a literal true — no real
// prepareStmt recording, no real initExecuteStmtParam, no retained compiled
// locking plan, no rebuilt markings. This member drives the REAL prepared
// machinery through the LIVE allocated gate in both directions: the
// production single-statement env PREPAREs a real locking statement (the
// shipped production statistics context supplies the locked table through
// the REAL BuildPlan), the env's constructor records the live negotiated
// protocol version on the prepareStmt through REAL currentProtocolVersion,
// and every EXECUTE below goes through the production decision inside
// initExecuteStmtParamWithResolverInSession — the production rebuild driver —
// against the SAME plan (revision 14, item 8(vii)):
//   - a matching recording RETAINS and REUSES the compiled locking plan
//     generation (the retained-reuse proof at constant capability);
//   - the DROP below the allocated gate flips the live negotiated version,
//     the recording mismatches, EXECUTE REALLY REBUILDS, and the rebuilt
//     generation binds UNIFORM LEGACY markings below the gate;
//   - the RISE restores the gate, EXECUTE REALLY REBUILDS again, and the
//     rebuilt generation binds the marked locking-read presence;
//   - each rebuild UPDATES the protocol recording to the live version (the
//     next EXECUTE at constant capability reuses the rebuilt generation).
func TestPreparedCapabilityTransitionsRebuildLockingPlanInBothDirections(t *testing.T) {
	base := plan2.NewMockCompilerContext(true, newPlanTestProcess(t))
	ctx := &preparedStatsTestCompiler{preparedTestCompiler: &preparedTestCompiler{CompilerContext: base, proc: base.GetProcess()},
		stats: &pbstats.StatsInfo{TableCnt: 128, BlockNumber: 1}}
	ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQLWithCompilerContext(t,
		231, "select n_name from nation where n_nationkey=? for update", ctx)
	defer prepared.Close()
	t.Cleanup(func() { ses.GetTxnCompileCtx().Close() })
	ctx.proc = cw.proc

	// The live negotiated version the EXECUTE decision reads: the env's top
	// process has no lock service, so the "" service runtime — the same
	// runtime currentProtocolVersion consults — IS the negotiation source,
	// and it initialized at the latest protocol (>= the allocated gate).
	rt := moruntime.ServiceRuntime(cw.proc.GetService())
	require.NotNil(t, rt)
	orig, had := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, orig) })
	require.True(t, had)
	liveVersion, ok := orig.(int64)
	require.True(t, ok)
	require.GreaterOrEqual(t, liveVersion, defines.MORPCVersion135,
		"the default negotiation initialized at the latest protocol version")
	require.Equal(t, liveVersion, prepared.protocolVersion,
		"PREPARE recorded the LIVE negotiated version (the rebuild decision's real input)")
	setNegotiated := func(version int64) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
	}

	prepared.defaultDatabase = "tpch"
	_, table, err := base.Resolve("tpch", "nation", nil)
	require.NoError(t, err)
	installStatsAdmissionStorage(t, ses, table, func() *pbstats.StatsInfo { return ctx.stats })

	bind := func(value int) {
		cw.proc.SetPrepareParams(nil)
		if prepared.params != nil {
			prepared.params.Free(cw.proc.Mp())
		}
		prepared.params = vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(prepared.params, []byte(fmt.Sprintf("%d", value)), false, cw.proc.Mp()))
		prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONG), 0}
	}
	execute := func() (*planpb.Plan, error) {
		bind(1)
		_, runtimePlan, executionStmt, _, owned, err := initExecuteStmtParamWithResolverInSession(
			execCtx, ses, ses, cw, nil, prepared.Name, ctx.Resolve, ctx)
		if owned && executionStmt != nil {
			executionStmt.Free()
		}
		return runtimePlan, err
	}

	// Above the gate the PREPARE-built locking plan carries presence.
	initialPlan := prepared.PreparePlan.GetDcl().GetPrepare().Plan
	require.True(t, markedPlanHasPresence(initialPlan),
		"the PREPARE-built locking plan binds marked presence above the allocated gate")

	// RETAINED REUSE before any transition: the recording matches the live
	// version; EXECUTE reuses the same captured generation, twice.
	planReuse, err := execute()
	require.NoError(t, err)
	require.True(t, cw.planGenerationReused,
		"a matching protocol recording keeps the compiled locking plan generation")
	require.Same(t, initialPlan, prepared.PreparePlan.GetDcl().GetPrepare().Plan,
		"the retained generation is the PREPARE-captured plan object")
	require.NotNil(t, planReuse)
	require.True(t, markedPlanHasPresence(planReuse), "the reused generation keeps its markings")
	planReuse2, err := execute()
	require.NoError(t, err)
	require.True(t, cw.planGenerationReused, "constant capability keeps reusing the generation")
	require.Same(t, initialPlan, prepared.PreparePlan.GetDcl().GetPrepare().Plan,
		"the PREPARE-captured generation survives repeated EXECUTEs: actual retained reuse")
	require.True(t, markedPlanHasPresence(planReuse2),
		"the reused generation keeps serving marked runtime specializations")

	// THE DROP below the allocated gate: the live negotiated version flips,
	// the recording mismatches, and EXECUTE REALLY REBUILDS through the
	// production rebuild driver — the rebuilt generation binds uniform
	// LEGACY markings under the lowered gate, and the recording updates.
	setNegotiated(128)
	dropped, err := execute()
	require.NoError(t, err)
	require.False(t, cw.planGenerationReused,
		"the capability drop forces a real rebuild behind the version mismatch")
	require.NotSame(t, initialPlan, prepared.PreparePlan.GetDcl().GetPrepare().Plan,
		"the rebuild installed a NEW plan generation")
	require.NoError(t, err)
	require.Equal(t, int64(128), prepared.protocolVersion,
		"the rebuild UPDATED the protocol recording to the live version")
	require.NotSame(t, initialPlan, dropped, "the rebuilt runtime plan is a fresh binding")
	require.False(t, markedPlanHasPresence(dropped),
		"the rebuilt generation binds UNIFORM LEGACY markings below the allocated gate")

	// THE RISE: the live negotiated version returns to the allocated gate;
	// the below-gate recording mismatches, EXECUTE rebuilds again, and the
	// rebuilt generation re-binds the marked locking-read presence.
	setNegotiated(defines.MORPCVersion135)
	risen, err := execute()
	require.NoError(t, err)
	require.False(t, cw.planGenerationReused,
		"the capability rise forces a real rebuild behind the restored version mismatch")
	require.Equal(t, defines.MORPCVersion135, prepared.protocolVersion,
		"the rebuild UPDATED the protocol recording to the risen version")
	require.True(t, markedPlanHasPresence(risen),
		"the rebuilt generation re-binds marked presence above the allocated gate")
	require.True(t, markedPlanHasPresence(prepared.PreparePlan.GetDcl().GetPrepare().Plan),
		"the rebuilt generation is installed as the prepareStmt's plan and carries presence")
	risenGeneration := prepared.PreparePlan.GetDcl().GetPrepare().Plan

	// RETAINED REUSE again at constant risen capability: the updated
	// recording matches, and EXECUTE serves the rebuilt generation as-is.
	risenAgain, err := execute()
	require.NoError(t, err)
	require.True(t, cw.planGenerationReused,
		"the updated recording keeps the rebuilt generation retained at constant capability")
	require.Same(t, risenGeneration, prepared.PreparePlan.GetDcl().GetPrepare().Plan,
		"the rebuilt marked generation is reused, not rebound")
	require.True(t, markedPlanHasPresence(risenAgain),
		"the reused rebuilt generation keeps serving marked specializations")
}
