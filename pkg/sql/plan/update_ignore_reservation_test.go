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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func updateIgnoreReservationNull(expr *plan.Expr) bool {
	return expr != nil && expr.GetLit() != nil && expr.GetLit().Isnull
}

func updateIgnoreMergedReservationChecks(t *testing.T, sql string) int {
	t.Helper()
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	logicPlan, err := runOneStmt(mock, t, sql)
	require.NoError(t, err)
	query := logicPlan.GetQuery()

	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_UNION_ALL {
			continue
		}
		for _, expr := range node.ProjectList {
			require.NotNil(t, expr, "UNION inputs must not contain positional holes")
		}
	}

	checks := 0
	for outerIdx, outer := range query.Nodes {
		if outer.NodeType != plan.Node_UNION_ALL || len(outer.Children) != 2 {
			continue
		}
		release := query.Nodes[outer.Children[1]]
		if len(release.ProjectList) < 3 ||
			release.ProjectList[len(release.ProjectList)-1].GetF() == nil ||
			release.ProjectList[len(release.ProjectList)-1].GetF().GetFunc().GetObjName() != "not" {
			continue
		}
		inner := query.Nodes[outer.Children[0]]
		if inner.NodeType != plan.Node_UNION_ALL || len(inner.Children) != 2 {
			continue
		}
		reservation := query.Nodes[inner.Children[0]]
		candidate := query.Nodes[inner.Children[1]]
		if len(reservation.ProjectList) < 3 || len(candidate.ProjectList) < 3 {
			continue
		}
		reservationOldKey := reservation.ProjectList[len(reservation.ProjectList)-3]
		reservationNewKey := reservation.ProjectList[len(reservation.ProjectList)-2]
		candidateOldKey := candidate.ProjectList[len(candidate.ProjectList)-3].GetCol()
		require.True(t, updateIgnoreReservationNull(reservationOldKey),
			"reservations must not release or delete statement-old keys")
		require.NotNil(t, reservationNewKey)
		require.False(t, updateIgnoreReservationNull(reservationNewKey),
			"reservations must carry the accepted current key")
		require.NotNil(t, candidateOldKey)
		candidateNewKey := candidate.ProjectList[len(candidate.ProjectList)-2].GetCol()
		require.NotNil(t, candidateNewKey)
		require.NotEqual(t, candidateOldKey.ColPos, candidateNewKey.ColPos,
			"ownership must use the captured preceding image, not the candidate")
		releaseKey := release.ProjectList[len(release.ProjectList)-2].GetF()
		require.NotNil(t, releaseKey)
		require.Equal(t, "if", releaseKey.GetFunc().GetObjName())
		require.Len(t, releaseKey.Args, 3)
		require.Equal(t, release.ProjectList[len(release.ProjectList)-3], releaseKey.Args[1],
			"changed releases must hash on the released snapshot key")
		require.True(t, updateIgnoreReservationNull(releaseKey.Args[2]))

		rowIDChecked := false
		for dedupIdx, dedup := range query.Nodes {
			if dedup.NodeType != plan.Node_JOIN || dedup.JoinType != plan.Node_DEDUP ||
				len(dedup.Children) != 2 || dedup.Children[1] != int32(outerIdx) {
				continue
			}
			require.NotNil(t, dedup.DedupJoinCtx.AuxiliaryRowIdCol)
			copied := DeepCopyDedupJoinCtx(dedup.DedupJoinCtx)
			require.Equal(t, dedup.DedupJoinCtx.AuxiliaryRowIdCol, copied.AuxiliaryRowIdCol)
			require.NotSame(t, dedup.DedupJoinCtx.AuxiliaryRowIdCol, copied.AuxiliaryRowIdCol)
			for _, filter := range query.Nodes {
				if filter.NodeType != plan.Node_FILTER || len(filter.Children) != 1 ||
					filter.Children[0] != int32(dedupIdx) {
					continue
				}
				for _, predicate := range filter.FilterList {
					isNotNull := predicate.GetF()
					if isNotNull == nil || isNotNull.GetFunc().GetObjName() != "isnotnull" ||
						len(isNotNull.Args) != 1 {
						continue
					}
					rowID := isNotNull.Args[0].GetCol()
					if rowID == nil {
						continue
					}
					rowIDPos := int(rowID.ColPos)
					// The optimizer may compact the DEDUP output projection. Follow
					// that projection back to the UNION/reservation source position.
					if rowIDPos < len(dedup.ProjectList) {
						if sourceCol := dedup.ProjectList[rowIDPos].GetCol(); sourceCol != nil {
							rowIDPos = int(sourceCol.ColPos)
						}
					}
					require.Less(t, rowIDPos, len(reservation.ProjectList))
					require.True(t, updateIgnoreReservationNull(reservation.ProjectList[rowIDPos]),
						"the reservation row ID consumed by the candidate filter must be NULL")
					rowIDChecked = true
				}
			}
		}
		require.True(t, rowIDChecked, "the merged check must have a candidate row ID filter")
		checks++
	}
	return checks
}

func TestUpdateIgnoreMergedUniqueReservationPlan(t *testing.T) {
	twoKeyTargets := updateIgnoreMergedReservationChecks(t,
		"UPDATE IGNORE dept a JOIN dept b ON a.deptno = b.deptno "+
			"SET a.dname = 'a', a.loc = 'a', b.dname = 'b', b.loc = 'b'")
	require.Equal(t, 2, twoKeyTargets,
		"each key-mutating alias needs one merged arbitration stage")

	oneKeyTarget := updateIgnoreMergedReservationChecks(t,
		"UPDATE IGNORE dept a JOIN dept b ON a.deptno = b.deptno "+
			"SET a.dname = 'a', b.loc = 'b'")
	require.Equal(t, 1, oneKeyTarget,
		"a candidate that changes no UNIQUE key must not add a redundant arbitration stage")
}
