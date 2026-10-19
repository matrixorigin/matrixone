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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestLoadForeignKeyRoutingAndPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, table, columns, policy string
		off, self, mixed, warning    bool
	}{
		{name: "ignore", table: "replace_fk_c", columns: "id,pid", policy: "ignore", warning: true},
		{name: "ordinary", table: "replace_fk_c", columns: "id,pid"},
		{name: "checks disabled", table: "replace_fk_c", columns: "id,pid", policy: "ignore", off: true},
		{name: "parent only", table: "replace_fk_p", columns: "id,v", policy: "ignore"},
		{name: "self", table: "self_ref", columns: "id,parent_id,name", policy: "ignore", self: true},
		{name: "self disabled", table: "self_ref", columns: "id,parent_id,name", policy: "ignore", off: true},
		{name: "mixed self", table: "self_ref", columns: "id,parent_id,name", policy: "ignore", self: true, mixed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			if tc.off {
				mock.ctxt.ResolveVariableFunc = func(name string, _, _ bool) (interface{}, error) {
					if name == "foreign_key_checks" {
						return int64(0), nil
					}
					if name == "sql_mode" {
						return "", nil
					}
					return nil, moerr.NewInternalError(context.Background(), "unexpected variable")
				}
			}
			if tc.mixed {
				fk := *mock.ctxt.tables["replace_fk_c"].Fkeys[0]
				fk.Cols = []uint64{1}
				mock.ctxt.tables[tc.table].Fkeys = append(mock.ctxt.tables[tc.table].Fkeys, &fk)
			}
			if tc.self {
				table := mock.ctxt.tables[tc.table]
				table.Name2ColIndex = make(map[string]int32)
				for i, col := range table.Cols {
					table.Name2ColIndex[col.Name] = int32(i)
				}
				addPositiveCheck(t, mock, tc.table, "id")
			}
			p, err := runOneStmt(mock, t, "load data inline format='csv', data='1,1,x' "+tc.policy+" into table "+tc.table+" fields terminated by ',' ("+tc.columns+")")
			require.NoError(t, err)
			q := p.GetQuery()
			modern, legacy, warnings, assertions, checks := 0, 0, 0, 0, 0
			for id := range reachableODKUPlanNodes(q) {
				n := q.Nodes[id]
				if n.NodeType == pb.Node_MULTI_UPDATE {
					modern++
				}
				if n.NodeType == pb.Node_INSERT {
					legacy++
				}
				require.False(t, n.GetPreInsertUkCtx().GetInsertIgnoreMultiDedup())
				if n.NodeType == pb.Node_JOIN && n.JoinType == pb.Node_DEDUP {
					require.Equal(t, pb.Node_FAIL, n.OnDuplicateAction)
				}
				for _, e := range n.FilterList {
					if exprContainsFuncName(e, "_load_fk_ignore_check") {
						warnings++
						require.Equal(t, pb.Node_FILTER, n.NodeType)
						require.True(t, n.FilterIsBarrier)
					}
					if exprContainsFuncName(e, "assert") {
						assertions++
					}
					if exprContainsFuncName(e, "_check_constraint_assert") {
						checks++
						require.Equal(t, pb.Node_FILTER, n.NodeType)
						require.True(t, n.FilterIsBarrier)
					}
				}
			}
			require.Equal(t, tc.warning, warnings == 1)
			if tc.self {
				require.Positive(t, legacy)
				require.Zero(t, modern)
				require.NotEmpty(t, q.DetectSqls)
				require.Equal(t, 1, checks)
			} else {
				require.Positive(t, modern)
				require.Zero(t, legacy)
				require.Empty(t, q.DetectSqls)
			}
			if tc.name == "ordinary" {
				require.Positive(t, assertions)
			}
			if !tc.self && tc.table == "replace_fk_c" {
				require.True(t, q.HasForeignKeyAction)
			}
		})
	}
}

func TestLoadForeignKeyIgnoreProtocolGate(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	service := mock.ctxt.GetProcess().GetService()
	for _, version := range []int64{defines.MORPCVersion124, defines.MORPCVersion125} {
		setPlanTestGlobalVariable(t, service, runtime.MOProtocolVersion, version)
		p, err := runOneStmt(mock, t, "load data inline format='csv', data='1,1' ignore into table replace_fk_c fields terminated by ',' (id,pid)")
		if version < defines.MORPCVersion125 {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), err)
			require.ErrorContains(t, err, "protocol version 125")
		} else {
			require.NoError(t, err)
			wire, err := p.Marshal()
			require.NoError(t, err)
			var decoded pb.Plan
			require.NoError(t, decoded.Unmarshal(wire))
			features, err := pb.RequiredRemoteExpressionFeatures(&decoded)
			require.NoError(t, err)
			require.True(t, features.LoadFKIgnoreCheck)
		}
		// Ordinary FK LOAD has no new diagnostic identity, even on main's
		// immediate predecessor epoch. Its assertion semantics are unchanged.
		ordinary, err := runOneStmt(mock, t, "load data inline format='csv', data='1,1' into table replace_fk_c fields terminated by ',' (id,pid)")
		require.NoError(t, err)
		features, err := pb.RequiredRemoteExpressionFeatures(ordinary)
		require.NoError(t, err)
		require.False(t, features.LoadFKIgnoreCheck)
	}
}

func TestLoadForeignKeyBarrierBeforeIndexFanout(t *testing.T) {
	for _, tc := range []struct {
		name, policy string
		check        bool
	}{
		{"fk only", "ignore", false}, {"check and fk", "ignore", true}, {"ordinary", "", false}, {"prefix unique", "ignore", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			table := mock.ctxt.tables["docs_ft"]
			fk := *mock.ctxt.tables["replace_fk_c"].Fkeys[0]
			fk.Cols = []uint64{table.Cols[2].ColId}
			table.Fkeys = []*pb.ForeignKeyDef{&fk}
			if tc.check {
				addPositiveCheck(t, mock, table.Name, "payload")
			}
			unique := *mock.ctxt.tables["dept"].Indexes[0]
			unique.IndexName = "uk_body_payload"
			unique.Parts = []string{"body", "payload"}
			if tc.name == "prefix unique" {
				unique.Parts = []string{"body"}
				unique.IndexAlgoParams = `{"prefix_lengths":"body:2"}`
			}
			table.Indexes = append(table.Indexes, &unique)
			idx := mock.ctxt.tables[unique.IndexTableName]
			idx.Cols[idx.Name2ColIndex[catalog.IndexTablePrimaryColName]].Typ = table.Cols[0].Typ
			p, err := runOneStmt(mock, t, "load data inline format='csv', data='1,alpha,1' "+tc.policy+" into table docs_ft fields terminated by ',' (id,body,payload)")
			require.NoError(t, err)
			q := p.GetQuery()
			assertEverySinkStepHasConsumer(t, q)
			predicate := "_load_fk_ignore_check"
			if tc.policy == "" {
				predicate = "assert"
			}
			var reaches func(int32) bool
			reaches = func(id int32) bool {
				n := q.Nodes[id]
				for _, e := range n.FilterList {
					if exprContainsFuncName(e, predicate) {
						return true
					}
				}
				for _, c := range n.Children {
					if reaches(c) {
						return true
					}
				}
				for _, s := range n.SourceStep {
					if reaches(q.Steps[s]) {
						return true
					}
				}
				return false
			}
			writes, tokenizers, predicates := 0, 0, 0
			for id := range reachableODKUPlanNodes(q) {
				n := q.Nodes[id]
				for _, e := range n.FilterList {
					if exprContainsFuncName(e, predicate) {
						predicates++
					}
				}
				if n.NodeType == pb.Node_MULTI_UPDATE {
					writes++
					require.True(t, reaches(id), "every table/index write must follow FK admission")
				}
				if n.NodeType == pb.Node_APPLY {
					tokenizers++
					require.True(t, reaches(id), "tokenization must follow FK admission")
				}
			}
			require.Equal(t, 1, predicates, "FK diagnostic/assert executes once before fanout")
			require.Positive(t, writes)
			require.Positive(t, tokenizers)
		})
	}
}
