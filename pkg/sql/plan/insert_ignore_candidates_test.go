// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestInsertIgnoreRetainsConstraintCandidates(t *testing.T) {
	for _, tc := range []struct {
		name              string
		check, fk, unique bool
	}{
		{"check", true, false, true},
		{"two_checks", true, false, true},
		{"fk", false, true, true},
		{"mixed", true, true, true},
		{"check_without_uk", true, false, false},
		{"fk_without_uk", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			table := mock.ctxt.tables["emp"]
			table.Cols[0].Typ.AutoIncr = true
			if !tc.fk {
				table.Fkeys = nil
			}
			if !tc.unique {
				table.Indexes = nil
			}
			if tc.check {
				addPositiveCheck(t, mock, "emp", "deptno")
				if tc.name == "two_checks" {
					addPositiveCheck(t, mock, "emp", "mgr")
				}
			}
			p, err := runOneStmt(mock, t,
				"INSERT IGNORE INTO emp (ename, job, deptno) VALUES ('a', 'x', 1), ('b', 'y', 0)")
			require.NoError(t, err)
			found := false
			foundCheck := false
			for _, node := range p.GetQuery().Nodes {
				if preCtx := node.GetPreInsertCtx(); preCtx.GetCheckEligibility() != nil {
					foundCheck = true
					child := p.GetQuery().Nodes[node.Children[0]]
					require.Equal(t, int32(types.T_bool), child.ProjectList[preCtx.CheckEligibility.ColPos].Typ.Id)
					copy := DeepCopyPreInsertCtx(preCtx)
					require.Equal(t, preCtx.CheckEligibility, copy.CheckEligibility)
					require.NotSame(t, preCtx.CheckEligibility, copy.CheckEligibility)
				}
				ctx := node.GetPreInsertUkCtx()
				if !ctx.GetAutoIncrementReorder() {
					continue
				}
				found = true
				if tc.fk {
					require.NotNil(t, ctx.RowEligibility)
					child := p.GetQuery().Nodes[node.Children[0]]
					require.Equal(t, int32(types.T_bool), child.ProjectList[ctx.RowEligibility.ColPos].Typ.Id)
				} else {
					require.Nil(t, ctx.RowEligibility)
				}
				require.Less(t, ctx.AutoIncrementOutputColumn, ctx.OutputColumns)
				if !tc.unique {
					require.Len(t, ctx.KeyColumns, 1)
				}
				copy := DeepCopyPreInsertUkCtx(ctx)
				require.Equal(t, ctx.RowEligibility, copy.RowEligibility)
				if ctx.RowEligibility != nil {
					require.NotSame(t, ctx.RowEligibility, copy.RowEligibility)
				}
				wire, err := ctx.Marshal()
				require.NoError(t, err)
				var decoded plan.PreInsertUkCtx
				require.NoError(t, decoded.Unmarshal(wire))
				require.Equal(t, ctx.RowEligibility, decoded.RowEligibility)
			}
			require.True(t, found)
			require.Equal(t, tc.check, foundCheck)
		})
	}
}

func TestInsertIgnoreFinalCandidateForeignKey(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	table := mock.ctxt.tables["emp"]
	table.Cols[0].Typ.AutoIncr = true
	table.Fkeys[0].Cols = []uint64{table.Cols[0].ColId}
	parent := mock.ctxt.tables["dept"]
	table.Fkeys[0].ForeignCols = []uint64{parent.Cols[0].ColId}
	p, err := runOneStmt(mock, t, "insert ignore into emp(ename,job,deptno) values ('a','x',1),('b','y',2)")
	require.NoError(t, err)
	found := false
	for _, node := range p.GetQuery().Nodes {
		ctx := node.GetPreInsertUkCtx()
		spec := ctx.GetOrderedAutoIncrement()
		if spec == nil {
			continue
		}
		found = true
		require.Equal(t, uint64(2), spec.ValuesRows)
		require.Len(t, spec.ForeignKeys, 1)
		require.Equal(t, ctx.AutoIncrementColumn, spec.ForeignKeys[0].ChildColumns[0].ColPos)
		require.Nil(t, ctx.RowEligibility, "final-ID FK must not filter the unallocated NULL")
		require.Equal(t, parent.TblId, spec.ForeignKeys[0].TableDef.TblId)
		copy := DeepCopyPreInsertUkCtx(ctx)
		require.True(t, proto.Equal(spec, copy.OrderedAutoIncrement))
		require.NotSame(t, spec.ForeignKeys[0].Predicate, copy.OrderedAutoIncrement.ForeignKeys[0].Predicate)
		wire, err := ctx.Marshal()
		require.NoError(t, err)
		decoded := &plan.PreInsertUkCtx{}
		require.NoError(t, decoded.Unmarshal(wire))
		require.True(t, proto.Equal(spec, decoded.OrderedAutoIncrement))
	}
	require.True(t, found)
	require.NotEmpty(t, p.GetQuery().CatalogDependencies, "cached plan must retain the FK parent schema dependency")
}

func TestInsertIgnoreOrderedSelfForeignKey(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	table := mock.ctxt.tables["emp"]
	table.Cols[0].Typ.AutoIncr = true
	table.Fkeys[0].ForeignTbl = 0
	table.Fkeys[0].Cols = []uint64{table.Cols[0].ColId}
	table.Fkeys[0].ForeignCols = []uint64{table.Cols[3].ColId}
	p, err := runOneStmt(mock, t, "insert ignore into emp(ename,job,mgr) values ('a','x',1),('b','y',2)")
	require.NoError(t, err)
	found := false
	for _, node := range p.GetQuery().Nodes {
		spec := node.GetPreInsertUkCtx().GetOrderedAutoIncrement()
		if spec == nil {
			continue
		}
		found = true
		require.Len(t, spec.ForeignKeys, 1)
		fk := spec.ForeignKeys[0]
		require.Len(t, fk.SelfParentColumns, 1)
		child := p.GetQuery().Nodes[node.Children[0]]
		require.Equal(t, int32(types.T_uint32), child.ProjectList[fk.SelfParentColumns[0].ColPos].Typ.Id)
	}
	require.True(t, found)
	require.Empty(t, p.GetQuery().DetectSqls, "ordered self checks must not scan unrelated orphan rows")
}
