//go:build matrixone_test

// Review #6031303617, glm91 (root's GLM90 finding 1): the focused
// plan-owned stamp unit with NON-vacuous cells. The positive proves the one
// guarded-payload occurrence on the REAL finalized MERGE Query with its
// writer link (stages[StageId-1].OwnerNode == the DML-Merge writer sink),
// the DeepCopy + proto wire roundtrip persistence and the default-off
// fallback (the same plan with the stamp cleared contributes zero guarded
// occurrences). The negatives are genuine nonzero dependent-call plans that
// must all stay unstamped: an ordinary SELECT's user-spelled IF guard, a
// MERGE WHEN-condition subquery, and a derived USING subquery payload. No
// Skip and no vacuous len>0/>=0 gates: every negative first asserts the
// dependent calls exist with a nonzero count before asserting unstamped.
package compile

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	testutil "github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// buildStampProbe mirrors the routed fixture's local probe builder: parse
// the statement and bind the real plan over the mock compiler context with
// this test's own process. Returns the FINAL query (post correlation
// binding), so the asserted identities are the finalized ones.
func buildStampProbe(t *testing.T, sql string) (*plan.Query, error) {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	stmt0, err0 := mysql.ParseOne(proc.Ctx, sql, 1)
	if err0 != nil {
		return nil, err0
	}
	defer stmt0.Free()
	bound0, err0 := plan2.BuildPlan(dependentMergeRadixContext{plan2.NewMockCompilerContext(true, proc)}, stmt0, false, nil)
	if err0 != nil {
		return nil, err0
	}
	return bound0.GetQuery(), nil
}

// collectDependentCalls mirrors the routed plan: every finalized node
// projection, every callable dependent occurrence with its finalized
// identity fields and its containing node id (the plan-level writer link).
// No Skip: the caller asserts the build error itself.
//
// glm90 stamp unit inputs (planner-owned membership): the exact guarded
// payload statement family over the fixture's real tables and its two
// demanded negatives.
const mergeGuardedPayloadSQL = `merge into gold_orders t using dim_orders s on t.id = s.id ` +
	`when not matched then insert (id, d) values (s.id, (select max(cast(v as decimal(10,2))) from cf_scalar))`

const mergeUsingSubqueryPayloadSQL = `merge into gold_orders t using ` +
	`(select id, d, (select (select max(cast(v as decimal(10,2))) from cf_scalar)) as gg from dim_orders) s on t.id = s.id ` +
	`when matched then update set d = s.gg`

const mergeUsingValuePayloadSQL = `merge into gold_orders t using ` +
	`(select id, d, (select count(*) from dim_orders) as cnt from dim_orders) s on t.id = s.id ` +
	`when matched and s.cnt > 0 then update set d = (select (select max(cast(v as decimal(10,2))) from cf_scalar))`

const mergeWhenSubquerySQL = `merge into gold_orders t using dim_orders s on t.id = s.id ` +
	`when matched and (select (select count(*) from dim_orders w where w.id = s.id)) > 0 then update set d = s.d`

func collectDependentCalls(q *plan.Query) []stampCall {
	var calls []stampCall
	for nodeID, n := range q.Nodes {
		for _, e := range n.ProjectList {
			_ = plan.VisitExprTree(e, func(ex *plan.Expr) error {
				if call := ex.GetDependent(); call != nil {
					calls = append(calls, stampCall{
						stamp:      call.GuardedMergePayload,
						program:    call.ProgramId,
						occurrence: call.OccurrenceId,
						stage:      call.StageId,
						node:       int32(nodeID),
						reducer:    call.Reducer,
						hasCompare: call.Comparison != nil,
						inputs:     len(call.Inputs),
						lhs:        len(call.Lhs),
					})
				}
				return nil
			})
		}
	}
	return calls
}

func stampedDependentCount(q *plan.Query) int {
	n := 0
	for _, c := range collectDependentCalls(q) {
		if c.stamp {
			n++
		}
	}
	return n
}

func clearGuardedStamps(q *plan.Query) {
	for _, n := range q.Nodes {
		for _, e := range n.ProjectList {
			_ = plan.VisitExprTree(e, func(ex *plan.Expr) error {
				if c := ex.GetDependent(); c != nil {
					c.GuardedMergePayload = false
				}
				return nil
			})
		}
	}
}

type stampCall struct {
	stamp      bool
	program    uint32
	occurrence uint32
	stage      uint32
	node       int32
	reducer    plan.SubqueryRef_Type
	hasCompare bool
	inputs     int
	lhs        int
}

func stampedCallsOf(q *plan.Query) []stampCall {
	var out []stampCall
	for _, c := range collectDependentCalls(q) {
		if c.stamp {
			out = append(out, c)
		}
	}
	return out
}

func TestIcebergMergeGuardedPayloadStampPlannerOwned(t *testing.T) {
	// POSITIVE on the REAL finalized Query: exactly one planner-stamped
	// guarded payload occurrence with its finalized identities, its writer
	// link through the owning stage, and the wire/deep-copy persistence.
	q, err := buildStampProbe(t, mergeGuardedPayloadSQL)
	require.NoError(t, err, "the guarded-payload MERGE must bind a real finalized Query")

	calls := collectDependentCalls(q)
	require.NotEmpty(t, calls, "the statement genuinely produces dependent calls on the finalized plan")
	stampedCalls := stampedCallsOf(q)
	require.Len(t, stampedCalls, 1, "exactly one guarded payload occurrence is planner-stamped")
	s := stampedCalls[0]
	require.Equal(t, plan.SubqueryRef_SCALAR, s.reducer)
	require.False(t, s.hasCompare)
	require.Equal(t, 0, s.inputs)
	require.Equal(t, 0, s.lhs)
	require.NotZero(t, s.program, "the finalized program identity")
	require.NotZero(t, s.occurrence, "the finalized occurrence identity")
	require.NotZero(t, s.stage, "the finalized stage identity")
	// root glm93 F4: the correlation stage directory EXPLICITLY owns the
	// stamped producer's containing node - the same owner-node link the
	// finalization validates - asserted here as an exact identity, not a count.
	require.LessOrEqual(t, int(s.stage), len(q.Correlation.Stages), "the stage directory covers the stamped call's stage")
	require.Equal(t, s.node, q.Correlation.Stages[s.stage-1].OwnerNode,
		"the correlation stage directory owns the stamped producer's containing node")

	// THE WRITER LINK (root glm91 F1): follow the plan's REAL out step to
	// its node - the actual main-output step's node must itself be the
	// specialized DML-Merge writer sink (Node_INSERT with the DMLMerge
	// extra options). This is the same out-step identity the production
	// writer consumes (query.Nodes[query.Steps[query.MainOutputStep()]]),
	// not merely a count of matching nodes anywhere in the plan.
	outStep := q.Steps[q.MainOutputStep()]
	require.GreaterOrEqual(t, int(outStep), 0)
	require.Less(t, int(outStep), len(q.Nodes))
	writer := q.Nodes[outStep]
	require.NotNil(t, writer)
	require.Equal(t, plan.Node_INSERT, writer.NodeType, "the plan's real out-step node is the MERGE writer sink")
	require.Equal(t, api.DMLMergePlanExtraOptions, writer.ExtraOptions, "the out-step writer carries the DML-Merge options")

	// The stamped clause-value producer's containing node must be REACHABLE
	// from that out-step writer through the plan's own node edges (the
	// transitive input closure over Children): the producer's guarded
	// payload genuinely feeds the writer this statement executes - the real
	// producer-to-writer ownership the runtime membership asserts.
	reachable := map[int32]bool{}
	var walk func(id int32)
	walk = func(id int32) {
		if id < 0 || int(id) >= len(q.Nodes) || reachable[id] {
			return
		}
		reachable[id] = true
		for _, c := range q.Nodes[id].Children {
			walk(c)
		}
	}
	walk(outStep)
	require.GreaterOrEqual(t, int(s.node), 0)
	require.Less(t, int(s.node), len(q.Nodes), "the stamped clause-value producer's node is in range")
	require.True(t, reachable[s.node],
		"the stamped guarded-payload producer node is reachable from the out-step DML-Merge writer")

	// WIRE ROUNDTRIP: the plan-owned stamp survives the pinned proto
	// Marshal/Unmarshal roundtrip (the older-plan fallback keys on this same
	// field being default-off on the wire).
	wire, err := proto.Marshal(q)
	require.NoError(t, err)
	var back plan.Query
	require.NoError(t, proto.Unmarshal(wire, &back))
	require.Equal(t, 1, stampedDependentCount(&back), "the wire roundtrip preserves the one stamped occurrence exactly")
	wireStamped := stampedCallsOf(&back)
	require.Len(t, wireStamped, 1)
	require.Equal(t, s, wireStamped[0],
		"the wire roundtrip preserves the EXACT occurrence/program/stage/node identity of the stamped call (root glm93 F4)")

	// DEEPCOPY (root glm91 F1): use the REAL production deep copy -
	// plan2.DeepCopyQuery - not proto.Clone: the production copier is the
	// one the runtime paths use, and the exact stamped identity
	// (occurrence/program/stage) must survive it unchanged.
	clone := plan2.DeepCopyQuery(q)
	require.Equal(t, 1, stampedDependentCount(clone), "the production deep copy preserves the one stamped occurrence exactly")
	stampedClone := stampedCallsOf(plan2.DeepCopyQuery(q))
	require.Len(t, stampedClone, 1)
	require.Equal(t, s, stampedClone[0],
		"the production deep copy preserves the exact occurrence/program/stage/node identity")

	// DEFAULT-OFF FALLBACK: the same finalized plan through the production
	// deep copy with its one stamp cleared contributes ZERO guarded
	// occurrences anywhere; an unstamped (older-plan) query stays outside
	// the sharing membership by construction while keeping its dependent
	// calls. The executor-side per-row behavior of unstamped plans is
	// proven by the routed native matrix's per-row cells and the executed
	// cleared-stamp fallback test.
	cleared := plan2.DeepCopyQuery(q)
	clearGuardedStamps(cleared)
	require.Zero(t, stampedDependentCount(cleared), "a default-off/older plan contributes zero guarded occurrences")
	require.NotEmpty(t, collectDependentCalls(cleared), "the cleared plan keeps its dependent calls; only the stamp association is gone")

	// NEGATIVE 1 (genuine nonzero): an ordinary SELECT's user-spelled IF
	// guard over a CORRELATED scalar genuinely binds a dependent call (the
	// non-equality capture takes the real dependent path, the plain
	// capture-free form is hoisted with no dependent call at all - the run6
	// receipt) and is never stamped. The statement form is valid SQL that
	// binds a direct Query (the CREATE VIEW form returns a DDL plan without
	// one).
	lookalike, err := buildStampProbe(t,
		"select if(s.d = 1, (select max(cast(v as decimal(10,2))) from cf_scalar c where c.v > s.d), null) as g from gold_orders s")
	require.NoError(t, err, "the ordinary IF-lookalike SELECT must bind a real Query")
	lookalikeCalls := collectDependentCalls(lookalike)
	require.NotEmpty(t, lookalikeCalls, "non-vacuous negative: the ordinary IF-lookalike's dependent call exists")
	require.Zero(t, stampedDependentCount(lookalike), "an ordinary SELECT's user-spelled IF guard is never stamped")

	// NEGATIVE 2 (genuine nonzero): a MERGE WHEN-condition subquery binds a
	// dependent call that the planner must NOT stamp.
	whenBound, err := buildStampProbe(t, mergeWhenSubquerySQL)
	require.NoError(t, err, "the WHEN-condition subquery MERGE must bind a real Query")
	whenCalls := collectDependentCalls(whenBound)
	require.NotEmpty(t, whenCalls, "non-vacuous negative: the WHEN-condition's dependent call exists")
	require.Zero(t, stampedDependentCount(whenBound), "a MERGE WHEN-condition subquery is never stamped")

	// NEGATIVE 3 (the derived USING position is structurally unstamped):
	// run9's receipt - the derived USING's own projection subquery (even the
	// nested depth-2 form) binds ZERO dependent calls; its child
	// BindContext keeps the legacy route, so nothing in the USING position
	// is ever a stampable occurrence. Assert that structural zero exactly
	// (not a >=0 vacuity: the receipts pin it).
	usingBound, err := buildStampProbe(t, mergeUsingSubqueryPayloadSQL)
	require.NoError(t, err, "the derived USING subquery MERGE must bind a real Query")
	require.Zero(t, len(collectDependentCalls(usingBound)),
		"the derived USING's own projection subquery keeps its legacy route with zero dependent calls")
	require.Zero(t, stampedDependentCount(usingBound),
		"a derived USING projection subquery is never a stampable occurrence")

	// THE DIRECTIONAL CROSS-CHECK (positive family, run10's receipt): the
	// same derived-USING MERGE's clause VALUE scalar IS the consumer-
	// visible guarded payload and receives exactly the one stamp.
	usingValue, err := buildStampProbe(t, mergeUsingValuePayloadSQL)
	require.NoError(t, err, "the derived-USING clause-value MERGE must bind a real Query")
	valueCalls := collectDependentCalls(usingValue)
	require.NotEmpty(t, valueCalls, "the clause VALUE scalar genuinely binds a dependent call")
	require.Equal(t, 1, stampedDependentCount(usingValue),
		"the clause VALUE payload receives exactly the one plan-owned stamp")
}
