// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
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

func TestTypedCollationAdmissionMatchesOriginalOwnerBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Plan)
	}{
		{"legacy", func(*Plan) {}},
		{"column future revision", func(p *Plan) { p.GetQuery().Nodes[0].TableDef.Cols[0].Typ.CollationVersion = 1 }},
		{"column unknown identity", func(p *Plan) { p.GetQuery().Nodes[0].TableDef.Cols[0].Typ.Charset = 259 }},
		{"default expression", func(p *Plan) { p.GetQuery().Nodes[0].TableDef.Cols[0].Default.Expr.Typ.Charset = 4 }},
		{"projection", func(p *Plan) { p.GetQuery().Nodes[0].ProjectList[1].Typ.Charset = 4 }},
		{"table key", func(p *Plan) { p.GetQuery().Nodes[0].TableDef.KeyFormat = 1 }},
		{"table default", func(p *Plan) { p.GetQuery().Nodes[0].TableDef.DefaultCharset = 4 }},
		{"index key", func(p *Plan) { p.GetQuery().Nodes[0].TableDef.Indexes[0].KeyFormat = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := collationAdmissionBenchmarkPlan(2)
			tc.mutate(p)
			before, after := legacyReflectCollationAdmission(p), RequireLegacyCollations(p)
			require.Equal(t, before == nil, after == nil)
			if tc.name != "legacy" {
				require.Error(t, after)
			} else {
				require.NoError(t, after)
			}
		})
	}
}

func TestTypedCollationAdmissionCyclesAndSharedExpressions(t *testing.T) {
	expr := &Expr{Typ: Type{Id: 61, Charset: 3}}
	expr.Expr = &Expr_List{List: &ExprList{List: []*Expr{expr, expr}}}
	require.NoError(t, RequireLegacyCollations(expr))
	require.NoError(t, RequireLegacyCollations([]*Expr{nil, expr, expr}))
	expr.Typ.CollationVersion = 1
	require.Error(t, RequireLegacyCollations(expr))
	require.Error(t, RequireLegacyCollations([]*Expr{nil, expr, expr}))
}

func TestTypedCollationAdmissionSchemaOnlyDDL(t *testing.T) {
	table := &TableDef{DefaultCharset: 3, Cols: []*ColDef{{Name: "c", Typ: Type{Id: 61, Charset: 3}}}}
	owner := &Plan{Plan: &Plan_Ddl{Ddl: &DataDefinition{
		Definition: &DataDefinition_CreateTable{CreateTable: &CreateTable{TableDef: table}},
	}}}
	require.NoError(t, RequireLegacyCollations(owner))
	table.KeyFormat = 1
	require.Error(t, RequireLegacyCollations(owner))
	table.KeyFormat = 0
	table.Cols[0].Typ.Charset = 4
	require.Error(t, RequireLegacyCollations(owner))
}

func TestCollationAdmissionPointPlanAllocationBudget(t *testing.T) {
	owner := collationAdmissionBenchmarkPlan(8)
	allocs := testing.AllocsPerRun(10, func() {
		if err := RequireLegacyCollations(owner); err != nil {
			panic(err)
		}
	})
	// A normal point plan fits the inline cycle set. No scalar boxing,
	// metadata sidecar, or heap map is needed at either admission boundary.
	require.Zero(t, allocs)
}
