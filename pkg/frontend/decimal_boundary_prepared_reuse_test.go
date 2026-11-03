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

package frontend

import (
	"fmt"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbstats "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// requirePreparedBoundaryTyp pins one DECIMAL256 plan type's width and scale.
func requirePreparedBoundaryTyp(t testing.TB, typ pb.Type, width, scale int32, msgAndArgs ...any) {
	t.Helper()
	message := append([]any{typ.String()}, msgAndArgs...)
	require.Equal(t, int32(types.T_decimal256), typ.Id, message...)
	require.Equal(t, width, typ.Width, message...)
	require.Equal(t, scale, typ.Scale, message...)
}

// TestPreparedCTASDecimalBoundaryCachedGenerationReuse is the cached-generation
// reuse witness of issue #30938 (design revision 20, decision log 66 (v)). The
// runtime protocol is pinned below the database-defaults floor so the CTAS plan
// forms without DatabaseDefaults; the preserved CTAS rebuild policy then stays
// quiet across quiescent EXECUTE calls and the PREPARE-claimed evidence carrier
// must be consumed repeatedly with no rebuild and no fresh claim. One
// controlled ddlVersion flip must produce exactly one rebuild whose own
// generation replaces the carrier in the same execute, after which a quiescent
// EXECUTE reuses the rebuilt carrier.
func TestPreparedCTASDecimalBoundaryCachedGenerationReuse(t *testing.T) {
	// DatabaseDefaultsEnabled requires the runtime protocol at or above
	// MORPCVersion118; one version below keeps the floor-gated formation
	// absent while plan-generation reuse (>= MORPCVersion32) stays supported.
	rt := moruntime.ServiceRuntime("")
	old, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion118)-1)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, old) })

	// CTAS planning reads the FK referrers of the created table through the
	// runtime-scoped internal SQL executor (the frontend wrapper here cannot
	// host the plan package's unexported executor provider, so the global
	// fallback is used). No table FK-references the fixture target, so the
	// memory executor's empty result is the production answer.
	oldExecutor, hadOldExecutor := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(
		func(string) (executor.Result, error) { return executor.Result{}, nil },
	))
	t.Cleanup(func() {
		if hadOldExecutor {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, oldExecutor)
		} else {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, nil)
		}
	})

	// Decimal tpch stand-ins, envelope-equivalent to the approved DECIMAL
	// columns of the design fixture: a x b is (38,10) x (38,10) and the
	// COALESCE default is (39,1), so the decision-owned <=65 alignment casts
	// and every boundary-oracle value match the approved plan-level witness.
	const fixture = "create table t30938_boundary as select truncate(coalesce(truncate(" +
		"cast(n_nationkey as decimal(38,10)) * cast(n_nationkey as decimal(38,10)), 0), " +
		"cast(n_regionkey as decimal(39,1))), ?) as v from nation"
	base := plan2.NewMockCompilerContext(false, newPlanTestProcess(t))
	ctx := &preparedTestCompiler{CompilerContext: base, proc: base.GetProcess()}
	ses, prepared, cw, execCtx := newPreparedExecuteEnvForSQLWithCompilerContext(t, 311, fixture, ctx)
	defer prepared.Close()
	ctx.proc = cw.proc
	prepared.defaultDatabase = "tpch"
	defer ses.GetTxnCompileCtx().Close()

	// Preconditions first (decision log 66 (v)): the formation is only
	// reachable when the prepared CTAS carries no DatabaseDefaults and the
	// preserved rebuild policy leaves the cached generation alone.
	snapshot := prepared.PreparePlan.GetDcl().GetPrepare().Plan
	require.NotNil(t, snapshot.GetDdl(), "fixture must plan as DDL")
	require.NotNil(t, snapshot.GetDdl().GetQuery(), "fixture must be a CTAS plan")
	create := snapshot.GetDdl().GetCreateTable()
	require.NotNil(t, create)
	require.Nil(t, create.DatabaseDefaults,
		"the below-floor protocol must withhold database defaults from the formation")
	require.False(t, shouldRebuildPreparePlan(false, snapshot),
		"the preserved CTAS rebuild policy must not force per-execute rebuilds")

	// Mirror the production prepare-time claim: the PREPARE build ran on the
	// compiler-context process, so its generation's evidence is claimed there.
	prepared.claimDecimalBoundaryEvidence(base.GetProcess())
	require.NotEmpty(t, prepared.retainedDecimalBoundaryLedger,
		"the prepared CTAS build must publish the boundary decision ledger")
	require.False(t, base.GetProcess().HasUnclaimedDecimalBoundaryEvidence(),
		"the prepare-time claim must consume the generation's read-once slot")

	// The retained carrier must hold the provenance merge decision whose
	// decision-owned <=65 alignment casts make the boundary reconstruction
	// require retained evidence: pre (64,0)/(39,1), aligned (65,1)/(39,1),
	// result (65,1) (design decision log 66 (i)-(iii)); the d arm matches
	// the family's OID and scale, so the binder's same-scale alignment skip
	// leaves it uncast and an uncast position records its pre type verbatim.
	foundMergeDecision := false
	for _, evidence := range prepared.retainedDecimalBoundaryLedger {
		entry, ok := evidence.(*rule.DecimalBoundaryDecisionEntry)
		// The widened multiply root and the TRUNCATE wrappers are also
		// two-argument provenance entries on this fixture; the merge decision
		// the carrier reconstructs from is the COALESCE family's.
		if !ok || entry.ArgCount != 2 || !entry.Provenance || entry.Name != "coalesce" {
			continue
		}
		require.Len(t, entry.PreTypes, 2)
		require.Len(t, entry.AlignedTypes, 2)
		requirePreparedBoundaryTyp(t, entry.PreTypes[0], 64, 0)
		requirePreparedBoundaryTyp(t, entry.PreTypes[1], 39, 1)
		requirePreparedBoundaryTyp(t, entry.AlignedTypes[0], 65, 1)
		requirePreparedBoundaryTyp(t, entry.AlignedTypes[1], 39, 1)
		requirePreparedBoundaryTyp(t, entry.ResultType, 65, 1)
		foundMergeDecision = true
	}
	require.True(t, foundMergeDecision,
		"the retained ledger must carry the widened COALESCE family decision")

	// Select the v output column by name: the frontend CTAS route also
	// carries the hidden __mo_fake_pk_col infrastructure column.
	snapshotColIdx := -1
	for i, c := range create.GetTableDef().GetCols() {
		if c.GetName() == "v" {
			snapshotColIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, snapshotColIdx, 0,
		"the PREPARE snapshot must carry the v output column")
	requirePreparedBoundaryTyp(t, create.GetTableDef().GetCols()[snapshotColIdx].GetTyp(), 54, 1,
		"the PREPARE snapshot keeps the parent's envelope-derived tag")

	// The controlled-invalidation variant below rebuilds through the session
	// compiler context; install its storage view before any execute.
	_, nationTable, err := base.Resolve("tpch", "nation", nil)
	require.NoError(t, err)
	installStatsAdmissionStorage(t, ses, nationTable,
		func() *pbstats.StatsInfo { return &pbstats.StatsInfo{TableCnt: 5} })

	bind := func() {
		t.Helper()
		cw.proc.SetPrepareParams(nil)
		if prepared.params != nil {
			prepared.params.Free(cw.proc.Mp())
		}
		prepared.params = vector.NewVec(types.T_text.ToType())
		prepared.ParamTypes = nil
		require.NoError(t, vector.AppendBytes(prepared.params, []byte("0"), false, cw.proc.Mp()))
		prepared.ParamTypes = append(prepared.ParamTypes, byte(defines.MYSQL_TYPE_LONGLONG), 0)
	}

	persistedTyp := func(p *pb.Plan) pb.Type {
		t.Helper()
		for _, c := range p.GetDdl().GetCreateTable().GetTableDef().GetCols() {
			if c.GetName() == "v" {
				return c.GetTyp()
			}
		}
		require.FailNow(t, "the executed CTAS plan must carry the v output column")
		return pb.Type{}
	}
	execute := func() *pb.Plan {
		t.Helper()
		bind()
		_, runtimePlan, stmt, _, owned, err := initExecuteStmtParamWithResolverInSession(
			execCtx, ses, ses, cw, nil, prepared.Name, base.Resolve, ctx)
		if owned && stmt != nil {
			stmt.Free()
		}
		require.NoError(t, err)
		require.NotNil(t, runtimePlan)
		return runtimePlan
	}
	snapshotTyp := func() pb.Type {
		t.Helper()
		return prepared.PreparePlan.GetDcl().GetPrepare().Plan.GetDdl().
			GetCreateTable().GetTableDef().GetCols()[0].GetTyp()
	}

	prepareNonce := prepared.retainedDecimalBoundaryNonce
	prepareCarrier := fmt.Sprintf("%p", prepared.retainedDecimalBoundaryLedger)

	// No-rebuild probe, then nonce identity and repeated consumption: two
	// quiescent executes reuse the PREPARE generation, install no fresh
	// evidence, and both persist the parent's envelope-derived tag by
	// consuming the same PREPARE-claimed carrier.
	for repetition := 0; repetition < 2; repetition++ {
		runtimePlan := execute()
		require.True(t, cw.planGenerationReused,
			"quiescent execute %d must reuse the cached generation", repetition)
		// Every execute route persists the PREPARE snapshot tag (54,1):
		// parameter specialization through a function-headed output
		// expression does not respecialize declared CTAS output types on
		// either engine (design section 9: EXECUTE keeps parent's (54,1)).
		requirePreparedBoundaryTyp(t, persistedTyp(runtimePlan), 54, 1,
			"quiescent execute %d must persist the parent's snapshot tag", repetition)
		require.Equal(t, prepareNonce, prepared.retainedDecimalBoundaryNonce,
			"a reuse execute must not claim fresh evidence")
		require.Equal(t, prepareCarrier, fmt.Sprintf("%p", prepared.retainedDecimalBoundaryLedger))
		require.False(t, cw.proc.HasUnclaimedDecimalBoundaryEvidence(),
			"a reuse execute runs no boundary generation on the session process")
		requirePreparedBoundaryTyp(t, snapshotTyp(), 54, 1,
			"the reusable snapshot keeps its own envelope-derived tag")
	}

	// Controlled single invalidation: exactly one rebuild-and-replace claims
	// its own generation's evidence and the refreshed execute still persists
	// the parent's envelope-derived tag.
	ses.advanceDDLVersion()
	rebuiltPlan := execute()
	require.False(t, cw.planGenerationReused,
		"the ddlVersion flip must force exactly one rebuild")
	requirePreparedBoundaryTyp(t, persistedTyp(rebuiltPlan), 54, 1)
	require.False(t, cw.proc.HasUnclaimedDecimalBoundaryEvidence(),
		"the rebuilding execute must claim its own generation's evidence")
	rebuiltNonce := prepared.retainedDecimalBoundaryNonce
	rebuiltCarrier := fmt.Sprintf("%p", prepared.retainedDecimalBoundaryLedger)
	require.NotEqual(t, prepareCarrier, rebuiltCarrier,
		"the rebuilt generation replaces the evidence carrier")

	// The following quiescent execute reuses the rebuilt carrier.
	reusedPlan := execute()
	require.True(t, cw.planGenerationReused,
		"a quiescent execute after the rebuild must reuse the rebuilt generation")
	requirePreparedBoundaryTyp(t, persistedTyp(reusedPlan), 54, 1)
	require.Equal(t, rebuiltNonce, prepared.retainedDecimalBoundaryNonce)
	require.Equal(t, rebuiltCarrier, fmt.Sprintf("%p", prepared.retainedDecimalBoundaryLedger))
	requirePreparedBoundaryTyp(t, snapshotTyp(), 54, 1,
		"the rebuilt snapshot keeps its own envelope-derived tag")
}
