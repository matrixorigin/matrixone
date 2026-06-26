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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPlanDefToCstrDefPersistsCheckConstraints(t *testing.T) {
	check := &plan.CheckDef{
		Name: "chk_v_positive",
		Check: &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_Bval{Bval: true},
				},
			},
		},
	}

	cstr, err := PlanDefToCstrDef(&plan.TableDef{Checks: []*plan.CheckDef{check}})
	require.NoError(t, err)
	require.Len(t, cstr.Cts, 1)

	configs := cstr.Cts[0].(*StreamConfigsDef).Configs
	visibleConfigs, checks, err := SplitCheckConstraintsFromConfigs(configs)
	require.NoError(t, err)
	require.Empty(t, visibleConfigs)
	require.Len(t, checks, 1)
	require.Equal(t, check.Name, checks[0].Name)
	require.NotNil(t, checks[0].Check)
}

func TestSplitCheckConstraintsFromConfigsKeepsVisibleConfigsOnDecodeError(t *testing.T) {
	check := &plan.CheckDef{
		Name: "chk_v_positive",
		Check: &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_Bval{Bval: true},
				},
			},
		},
	}
	cstr, err := PlanDefToCstrDef(&plan.TableDef{Checks: []*plan.CheckDef{check}})
	require.NoError(t, err)
	checkConfigs := cstr.Cts[0].(*StreamConfigsDef).Configs

	visibleConfigs, checks, err := SplitCheckConstraintsFromConfigs([]*plan.Property{
		{
			Key:   "visible-before",
			Value: "before",
		},
		checkConfigs[0],
		{
			Key:   CheckConstraintsConfigKey,
			Value: "not-base64",
		},
		{
			Key:   "visible-after",
			Value: "after",
		},
	})
	require.Error(t, err)
	require.Len(t, visibleConfigs, 2)
	require.Equal(t, "visible-before", visibleConfigs[0].Key)
	require.Equal(t, "visible-after", visibleConfigs[1].Key)
	require.Nil(t, checks)
}
