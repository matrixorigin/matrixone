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

package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestPlanColsToExeColsPreservesCharset(t *testing.T) {
	defs := PlanColsToExeCols([]*plan.ColDef{
		{
			Name:       "name",
			OriginName: "name",
			Typ: plan.Type{
				Id:      int32(types.T_varchar),
				Width:   32,
				Charset: uint32(types.CharsetBinary),
			},
		},
		{
			Name:       "payload",
			OriginName: "payload",
			Typ: plan.Type{
				Id:    int32(types.T_binary),
				Width: 8,
			},
		},
	})
	require.Len(t, defs, 2)
	require.Equal(t, types.CharsetBinary, defs[0].(*AttributeDef).Attr.Type.Charset)
	require.Equal(t, types.CharsetBinary, defs[1].(*AttributeDef).Attr.Type.Charset)
}

func TestPlanDefsToExeDefsPersistsChecksInSchemaExtra(t *testing.T) {
	check := &plan.CheckDef{
		Name: "t_chk_1",
		Check: &plan.Expr{
			Typ: plan.Type{Id: 10},
		},
	}
	_, extra, err := PlanDefsToExeDefs(&plan.TableDef{
		Name:           "t",
		Checks:         []*plan.CheckDef{check},
		DefaultCharset: uint32(types.CharsetBinary),
	})
	require.NoError(t, err)
	require.Equal(t, []*plan.CheckDef{check}, extra.Checks)
	require.Equal(t, uint32(types.CharsetBinary), extra.DefaultCharset)

	roundTrip := api.MustUnmarshalTblExtra(api.MustMarshalTblExtra(extra))
	require.Equal(t, extra.Checks, roundTrip.Checks)
	require.Equal(t, extra.DefaultCharset, roundTrip.DefaultCharset)

	clone := api.CloneExtra(extra)
	require.Equal(t, extra.Checks, clone.Checks)
	require.Equal(t, extra.DefaultCharset, clone.DefaultCharset)
	require.NotSame(t, extra.Checks[0], clone.Checks[0])
}
