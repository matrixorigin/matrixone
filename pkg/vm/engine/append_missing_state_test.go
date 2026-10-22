// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestAppendMissingPlanToCatalogExtra(t *testing.T) {
	key := &plan.ColDef{Name: "__mo_cpkey_col", ColId: 3, Seqnum: 2, Hidden: true, Primary: true,
		Typ: plan.Type{Id: int32(types.T_varchar)}}
	table := &plan.TableDef{Version: 1, Pkey: &plan.PrimaryKeyDef{CompPkeyCol: key},
		Cols: []*plan.ColDef{
			{Name: "a", ColId: 1, Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "b", ColId: 2, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int32)}},
			key,
			{Name: "flag", ColId: 4, Seqnum: 3, Typ: plan.Type{Id: int32(types.T_bool)}, Default: &plan.Default{NullAbility: true}},
			{Name: "__mo_rowid", ColId: 5, Seqnum: 65535, Typ: plan.Type{Id: int32(types.T_Rowid)}},
		}, AppendMissingState: &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"),
			BaseWidth: 3, PkSeqnum: 2, Columns: []*plan.ColumnBirthFill{{Seqnum: 3, BirthAppendOrdinal: 1,
				Type: plan.Type{Id: int32(types.T_bool)}, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1}}}}
	_, extra, err := PlanDefsToExeDefs(table)
	require.NoError(t, err)
	require.Equal(t, uint32(4), extra.NextColSeqnum)
	require.Equal(t, table.AppendMissingState, api.MustUnmarshalTblExtra(api.MustMarshalTblExtra(extra)).AppendMissingState)
	extra.AppendMissingState.LayoutId[0] = 'x'
	require.Equal(t, byte('0'), table.AppendMissingState.LayoutId[0])
	table.Version = 0
	_, _, err = PlanDefsToExeDefs(table)
	require.Error(t, err)
	table.Version = 1
	table.Cols[3].Typ.Id = int32(types.T_int32)
	_, _, err = PlanDefsToExeDefs(table)
	require.Error(t, err)
}
