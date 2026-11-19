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
	"strings"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestUpdateSelfFKDetectionUsesOnlyWrittenTargets(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		{"mixed targets", "update constraint_test.self_ref_cascade s join tpch.nation n on s.id=n.n_nationkey set s.id=s.id+10,n.n_name='changed'", []string{"self_ref_cascade"}},
		{"two self references", "update constraint_test.self_ref s join constraint_test.self_ref_cascade c on s.id=c.id set s.parent_id=s.parent_id,c.parent_id=c.parent_id", []string{"self_ref", "self_ref_cascade"}},
		{"right join retains source order", "update constraint_test.self_ref s right join constraint_test.self_ref_cascade c on s.id=c.id set s.parent_id=s.parent_id,c.parent_id=c.parent_id", []string{"self_ref", "self_ref_cascade"}},
		{"repeated physical target", "update constraint_test.self_ref s join constraint_test.self_ref x on s.id=x.id set s.parent_id=s.parent_id,x.parent_id=x.parent_id", []string{"self_ref", "self_ref"}},
		{"self reference only read", "update constraint_test.self_ref s join tpch.nation n on s.id=n.n_nationkey set n.n_name='changed'", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, tc.sql)
			require.NoError(t, err)
			var checks []string
			for _, sql := range p.GetQuery().DetectSqls {
				if strings.HasPrefix(sql, "select count(*)") {
					checks = append(checks, sql)
				}
			}
			require.Len(t, checks, len(tc.want))
			for i, table := range tc.want {
				require.Contains(t, checks[i], "`constraint_test`.`"+table+"`")
			}
		})
	}
}

func TestUpdateGeneratedAssignmentsValidateBeforeDeduplication(t *testing.T) {
	for _, tc := range []struct {
		name, sql, table, column string
		invalid                  bool
	}{
		{"self fk invalid then default", "update constraint_test.self_ref s join tpch.nation n on s.id=n.n_nationkey set s.name='bad',s.name=default,s.parent_id=s.parent_id", "self_ref", "name", true},
		{"self fk default then invalid", "update constraint_test.self_ref s join tpch.nation n on s.id=n.n_nationkey set s.name=default,s.name='bad',s.parent_id=s.parent_id", "self_ref", "name", true},
		{"self fk default control", "update constraint_test.self_ref s join tpch.nation n on s.id=n.n_nationkey set s.name=default,s.parent_id=s.parent_id", "self_ref", "name", false},
		{"no fk invalid then default", "update tpch.nation n join tpch.region r on n.n_regionkey=r.r_regionkey set n.n_name='bad',n.n_name=default,n.n_comment=n.n_comment", "nation", "n_name", true},
		{"no fk default then invalid", "update tpch.nation n join tpch.region r on n.n_regionkey=r.r_regionkey set n.n_name=default,n.n_name='bad',n.n_comment=n.n_comment", "nation", "n_name", true},
		{"no fk repeated default", "update tpch.nation n join tpch.region r on n.n_regionkey=r.r_regionkey set n.n_name=default,n.n_name=default,n.n_comment=n.n_comment", "nation", "n_name", false},
		{"single target invalid then default", "update constraint_test.self_ref set name='bad',name=default,parent_id=parent_id", "self_ref", "name", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			table := mock.ctxt.tables[tc.table]
			for _, col := range table.Cols {
				if col.Name == tc.column {
					col.GeneratedCol = &planpb.GeneratedCol{IsStored: true, Expr: &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "generated"}}}}, OriginString: "'generated'"}
				}
			}
			_, err := runOneStmt(mock, t, tc.sql)
			if tc.invalid {
				require.ErrorContains(t, err, "the value specified for generated column")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUpdateGeneratedValidationKeepsSequentialErrorOrder(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	table := mock.ctxt.tables["self_ref"]
	for _, col := range table.Cols {
		switch col.Name {
		case "id":
			col.Typ.AutoIncr = true
		case "name":
			col.GeneratedCol = &planpb.GeneratedCol{IsStored: true}
		}
	}
	_, err := runOneStmt(mock, t, "update constraint_test.self_ref set id=null,name='bad'")
	require.ErrorContains(t, err, "Column 'id' cannot be null")
}
