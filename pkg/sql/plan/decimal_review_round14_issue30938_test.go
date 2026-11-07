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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// This file pins the planner regression of issue #30938 review round 14
// (P2) against approved design revision 41 (sha256
// ac4827f137577d937bce9ef8ff8740958b19cbbe9a26a3836bb5301617ba1780):
//
//	A value merge over a TIME_WINDOW pending aggregate output declined
//	parent-domain recovery: the decimalBoundaryOperandParentDomain timeTag
//	case returned the widened surface type with provenance=false for every
//	timeTag reference, but BindTimeWindowFunc also routes retained
//	aggregate results through the same tag (having_binder.go's
//	makeTimeWindowProjectionExpr publishes ctx.times[colPos].Typ as
//	ColRef{timeTag, colPos}). COALESCE(MAX(a*b), CAST(0 AS DECIMAL(65,1)))
//	over DECIMAL(38,0) operands attempts DECIMAL(77,1) on the widened
//	surface - 76 integral digits plus the fixed scale 1 exceed the 76-digit
//	carrier - and the retry funnel declined at the !proven early-return,
//	surfacing the binding error where the parent engine (multiplication
//	capped at (65,0)) binds the merge (66,1).
//
// The repair implements approved design decision log entry 61: the timeTag
// descent resolves by entry shape of ctx.times[colPos] - an Expr_Col entry
// is a timestamp boundary self-reference and resolves nothing; an Expr_F
// entry is the retained producing expression and resolves through the
// aggregate parent-domain resolver, whose ColRef{aggregateTag, k}
// arguments descend ctx.aggregates[k]. Every expected tag below is derived
// from the shared typing owners (the parent's registered MAX result rule
// over the parent (65,0) product domain and the coalesce common over
// (65,0)+(65,1) = (66,1)), never from observed candidate behavior.

// queryPlanHeadExprs returns the final projection list of a SELECT plan.
func queryPlanHeadExprs(t *testing.T, built *pb.Plan) []*pb.Expr {
	t.Helper()
	query := built.GetQuery()
	require.NotNil(t, query)
	head := query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList
	require.NotEmpty(t, head)
	return head
}

func TestDecimalTimeWindowAggregateParentDomainReviewRound14Issue30938(t *testing.T) {
	t.Run("selectCastZeroDefault", func(t *testing.T) {
		// The review witness: the widened (76,0) retained MAX merged with
		// the fixed (65,1) cast overflows the carrier on the surface; the
		// recovery converges the parent's (66,1).
		built, err := buildWindowFixtureStatement(t,
			"select coalesce(max(a * b), cast(0 as decimal(65,1))) as product "+
				"from tws30938 interval(ts, 1, minute)")
		require.NoError(t, err,
			"the merge over the retained window output must recover the parent domain")
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 66, 1,
			"the parent merge over (65,0)+(65,1) binds (66,1)")
	})
	t.Run("selectCastOneDefault", func(t *testing.T) {
		// The design decision 61 witness.
		built, err := buildWindowFixtureStatement(t,
			"select coalesce(max(a * b), cast(1 as decimal(65,1))) as product "+
				"from tws30938 interval(ts, 1, minute)")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 66, 1,
			"the parent merge over (65,0)+(65,1) binds (66,1)")
	})
	t.Run("slidingForm", func(t *testing.T) {
		// The sliding remap varies only window configuration, never the
		// registered result rule (decision 61).
		built, err := buildWindowFixtureStatement(t,
			"select coalesce(max(a * b), cast(0 as decimal(65,1))) as product "+
				"from tws30938 interval(ts, 2, minute) sliding(1, minute)")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 66, 1,
			"the sliding route recovers the same parent merge")
	})
	t.Run("havingForm", func(t *testing.T) {
		// The decision 61 HAVING witness binds identically through the same
		// routing (the grammar places HAVING before the INTERVAL clause);
		// the head keeps the widened MAX publication.
		built, err := buildWindowFixtureStatement(t,
			"select max(a * b) as product from tws30938 "+
				"having coalesce(max(a * b), cast(1 as decimal(65,1))) > 0 "+
				"interval(ts, 1, minute)")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 76, 0,
			"the widened product's MAX keeps the expression metadata widened")
	})
	t.Run("genuineWideNoProvenanceDeclines", func(t *testing.T) {
		// A genuine 76-digit literal carries no multiplication provenance:
		// the recovery declines and the family's original checked outcome
		// re-arms verbatim on both pre- and post-repair engines.
		_, err := buildWindowFixtureStatement(t,
			"select coalesce("+
				"max(9999999999999999999999999999999999999999999999999999999999999999999999999999), "+
				"cast(0 as decimal(65,1))) as product from tws30938 interval(ts, 1, minute)")
		require.Error(t, err,
			"a genuine-wide merge without provenance never retries")
		require.Contains(t, err.Error(),
			"invalid argument function coalesce, bad value [DECIMAL256 DECIMAL256]",
			"the declined merge re-arms the family's original checked outcome")
	})
	t.Run("plainAggregationControl", func(t *testing.T) {
		// No value merge: the retained window MAX keeps its widened
		// publication; the recovery is unreachable and the repair cannot
		// alter this shape.
		built, err := buildWindowFixtureStatement(t,
			"select max(a * b) as product from tws30938 interval(ts, 1, minute)")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 76, 0)
	})
	t.Run("zeroDefaultControl", func(t *testing.T) {
		// The unscaled zero default merges below the carrier on the first
		// pass (76 integral digits, scale 0), so the retry funnel is
		// unreachable and the publication stays the widened (76,0).
		built, err := buildWindowFixtureStatement(t,
			"select coalesce(max(a * b), 0) as product from tws30938 interval(ts, 1, minute)")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 1)
		requireDecimalTyp(t, head[0].Typ, 76, 0)
	})
	t.Run("timestampSelfReferenceControl", func(t *testing.T) {
		// The Expr_Col boundary self-references stay neutral: the _wstart
		// projection binds its timestamp identity exactly as before.
		built, err := buildWindowFixtureStatement(t,
			"select _wstart, max(a * b) as product from tws30938 interval(ts, 1, minute) "+
				"order by _wstart")
		require.NoError(t, err)
		head := queryPlanHeadExprs(t, built)
		require.Len(t, head, 2)
		require.Equal(t, int32(types.T_timestamp), head[0].Typ.Id,
			"the boundary self-reference keeps its timestamp identity")
		requireDecimalTyp(t, head[1].Typ, 76, 0)
	})
}
