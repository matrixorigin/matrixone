// Copyright 2026 Matrix Origin
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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInsertIgnoreCheckSelectUsesBulkReservation(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	emp := mock.ctxt.tables["emp"]
	emp.Cols[0].Typ.AutoIncr = true
	emp.Fkeys = nil
	addPositiveCheck(t, mock, "emp", "deptno")
	p, err := runOneStmt(mock, t, "insert ignore into emp(ename,job,deptno) select ename,job,deptno from emp")
	require.NoError(t, err)
	found := false
	for _, node := range p.GetQuery().Nodes {
		if ctx := node.GetPreInsertCtx(); ctx != nil {
			found = true
			require.NotNil(t, ctx.CheckEligibility)
			require.True(t, ctx.TrackAutoIncrementGenerated)
			require.True(t, ctx.DeferAutoIncrement)
		}
	}
	require.True(t, found)
	for _, node := range p.GetQuery().Nodes {
		if spec := node.GetPreInsertUkCtx().GetOrderedAutoIncrement(); spec != nil {
			require.Zero(t, spec.ValuesRows)
		}
	}
}
