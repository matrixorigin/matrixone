//go:build mo_m0

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestM0BirthCNProposalPreservesLegacyNullHistory(t *testing.T) {
	table := m0TestAppendTableDef()
	// This source has one existing nullable M0 suffix.
	table.Cols[3].Typ = plan.Type{Id: plan.AppendMissingBoolOID}
	for i, c := range table.Cols {
		c.ColId = uint64(i + 1)
	}
	table.Cols[4].Hidden = true
	table.Pkey.CompPkeyCol = table.Cols[2]
	added := &plan.ColDef{Name: "flag", ColId: 5, Seqnum: 4, Typ: plan.Type{Id: plan.AppendMissingBoolOID}}
	added.Default = &plan.Default{NullAbility: true, OriginString: "false", Expr: &plan.Expr{Typ: added.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}}
	state, err := proposeM0BirthState(table, added)
	require.NoError(t, err)
	require.Len(t, state.LayoutId, 16)
	require.Len(t, state.Columns, 2)
	require.Equal(t, plan.MissingFillKind_MISSING_FILL_NULL_V1, state.Columns[0].Kind)
	require.Equal(t, plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1, state.Columns[1].Kind)
	require.Nil(t, table.AppendMissingState, "proposal cannot mutate the source catalog")
	old := plan.CloneAppendMissingState(state)
	table.AppendMissingState = state
	table.Version = 2
	rowid := table.Cols[4]
	rowid.Seqnum = 5
	rowid.ColId = 6
	table.Cols = append(table.Cols[:4], added, rowid)
	next := *added
	next.Name = "second"
	next.ColId = 6
	next.Seqnum = 5
	proposed, err := proposeM0BirthState(table, &next)
	require.NoError(t, err)
	require.Len(t, proposed.Columns, 3)
	require.Equal(t, old, state)
	require.Equal(t, state.LayoutId, proposed.LayoutId)
	proposed.LayoutId[0] ^= 1
	require.Equal(t, old, state)
}
