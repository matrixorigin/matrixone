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

package engine

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPlanDefToCstrDefKeepsChecksOutOfLegacyConstraintStream(t *testing.T) {
	userProperty := &plan.Property{
		Key:   "__mo_check_constraints",
		Value: "mo_check_constraints_v1:<user-value>",
	}
	tableDef := &plan.TableDef{
		Checks: []*plan.CheckDef{{Name: "chk"}},
		Defs: []*plan.TableDef_DefType{{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{Properties: []*plan.Property{userProperty}},
			},
		}},
	}

	cstr, err := PlanDefToCstrDef(tableDef)
	require.NoError(t, err)
	require.Len(t, cstr.Cts, 1)
	require.Equal(t, userProperty, cstr.Cts[0].(*StreamConfigsDef).Configs[0])

	// This is the exact legacy binary reader. New writers must emit only tags it
	// already understands so rolling upgrades and rollback cannot desynchronize.
	data, err := cstr.MarshalBinary()
	require.NoError(t, err)
	decoded := &ConstraintDef{}
	require.NotPanics(t, func() {
		require.NoError(t, decoded.UnmarshalBinary(data))
	})
	require.Len(t, decoded.Cts, 1)
	require.Equal(t, userProperty, decoded.Cts[0].(*StreamConfigsDef).Configs[0])
}

func TestPlanDefsToExeDefsPersistsChecksInSchemaExtra(t *testing.T) {
	check := &plan.CheckDef{
		Name:      "t_chk_1",
		OriginSql: "`a` > 0",
		Check: &plan.Expr{
			Typ: plan.Type{Id: int32(10)},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}},
			},
		},
	}
	tableDef := &plan.TableDef{Checks: []*plan.CheckDef{check}}

	_, extra, err := PlanDefsToExeDefs(tableDef)
	require.NoError(t, err)
	require.Len(t, extra.Checks, 1)
	require.Equal(t, check.Name, extra.Checks[0].Name)
	require.Equal(t, check.OriginSql, extra.Checks[0].OriginSql)

	data, err := extra.Marshal()
	require.NoError(t, err)
	roundTripped := &api.SchemaExtra{}
	require.NoError(t, roundTripped.Unmarshal(data))
	require.Equal(t, extra.Checks, roundTripped.Checks)
}
