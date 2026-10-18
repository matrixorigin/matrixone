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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestOrderedForeignKeyAccessMetadata(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	child, parent := mock.ctxt.tables["emp"], mock.ctxt.tables["dept"]
	parent.Name2ColIndex = map[string]int32{"deptno": 0, "dname": 1, "loc": 2}
	child.Cols[0].Typ = parent.Cols[0].Typ
	child.Cols[0].Typ.AutoIncr = true
	child.Fkeys[0].Cols = []uint64{child.Cols[0].ColId}
	child.Fkeys[0].ForeignCols = []uint64{parent.Cols[0].ColId}
	p, err := runOneStmt(mock, t, "insert ignore into emp(ename,job,deptno) values ('a','x',1)")
	require.NoError(t, err)
	var metadata *plan.PreInsertUkCtx
	for _, node := range p.GetQuery().Nodes {
		if node.GetPreInsertUkCtx().GetOrderedAutoIncrement() != nil {
			metadata = node.PreInsertUkCtx
			break
		}
	}
	require.NotNil(t, metadata)
	require.NoError(t, ValidateOrderedInsertIgnore(t.Context(), metadata))
	fk := metadata.OrderedAutoIncrement.ForeignKeys[0]
	require.Equal(t, plan.OrderedForeignKeyAccess_PK_POINT, fk.ParentAccess.Kind)
	copy := DeepCopyPreInsertUkCtx(metadata)
	require.NotSame(t, fk.ParentAccess, copy.OrderedAutoIncrement.ForeignKeys[0].ParentAccess)
	require.NotSame(t, fk.ParentAccess.LookupFilter, copy.OrderedAutoIncrement.ForeignKeys[0].ParentAccess.LookupFilter)
	for _, mode := range []string{"missing", "unknown", "table_lock", "wrong_table", "witness_ordinal", "child_slot", "mixed_key", "lookup_key"} {
		t.Run(mode, func(t *testing.T) {
			copy := proto.Clone(metadata).(*plan.PreInsertUkCtx)
			fk := copy.OrderedAutoIncrement.ForeignKeys[0]
			switch mode {
			case "missing":
				fk.ParentAccess = nil
			case "unknown":
				fk.ParentAccess.Kind = plan.OrderedForeignKeyAccess_Kind(100)
			case "table_lock":
				fk.ParentAccess.LockTarget.LockTable = true
			case "wrong_table":
				fk.ParentAccess.LockTarget.TableId++
			case "witness_ordinal":
				fk.ParentAccess.WitnessLockKey.GetCol().ColPos = int32(len(parent.Cols))
			case "child_slot":
				fk.Predicate.GetF().Args[1].GetCol().ColPos = 9
			case "lookup_key":
				fk.ParentAccess.LookupFilter.GetF().Args[0].GetCol().ColPos = 1
			case "mixed_key":
				fk.Predicate.GetF().Args[0].GetCol().RelPos = 1
			}
			require.Error(t, ValidateOrderedInsertIgnore(t.Context(), copy))
		})
	}
}

func TestLoadLocalCheckPolicyWithOrderedAllocation(t *testing.T) {
	for _, mode := range []string{"", "ignore"} {
		t.Run(mode, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			mock.ctxt.tables["dept"].Cols[0].Typ.AutoIncr = true
			addPositiveCheck(t, mock, "dept", "deptno")
			p, err := runOneStmt(mock, t, "load data local infile 'client.csv' "+mode+" into table dept fields terminated by ','")
			require.NoError(t, err)
			checks := 0
			for id := range reachableODKUPlanNodes(p.GetQuery()) {
				node := p.GetQuery().Nodes[id]
				if node.GetPreInsertCtx().GetCheckEligibility() != nil {
					checks++
				}
				for _, expr := range node.FilterList {
					if exprContainsFuncName(expr, "_check_constraint_assert") {
						checks++
						want := plan.Node_ASSERT
						if mode == "ignore" {
							want = plan.Node_FILTER
						}
						require.Equal(t, want, node.NodeType, "CHECK follows explicit IGNORE independently of LOCAL row ignore")
					}
				}
			}
			require.Equal(t, 1, checks, "CHECK evaluates exactly once")
		})
	}
}
