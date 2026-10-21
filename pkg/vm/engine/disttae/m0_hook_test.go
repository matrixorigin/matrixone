//go:build mo_m0

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

package disttae

import (
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func m0TestAppendTableDef() *plan.TableDef {
	return &plan.TableDef{
		Version: 1,
		Cols: []*plan.ColDef{
			{Name: "a", Seqnum: 0},
			{Name: "b", Seqnum: 1},
			{Name: pkgcatalog.CPrimaryKeyColName, Seqnum: 2, Hidden: true, Primary: true},
			{Name: "c", Seqnum: 3, Default: &plan.Default{NullAbility: true}},
			{Name: pkgcatalog.Row_ID, Seqnum: 4},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: pkgcatalog.CPrimaryKeyColName,
			CompPkeyCol: &plan.ColDef{
				Name: pkgcatalog.CPrimaryKeyColName, Seqnum: 2, Hidden: true, Primary: true,
			},
		},
	}
}

func TestM0AppendBaseValidatesCompletePlanDomain(t *testing.T) {
	table := m0TestAppendTableDef()
	previous := activeM0Enabled
	activeM0Enabled = true
	t.Cleanup(func() { activeM0Enabled = previous })

	base, ok := m0AppendBaseFromPlan(table)
	require.True(t, ok)
	require.Equal(t, uint32(3), base)

	table.Cols[3].Default.NullAbility = false
	_, ok = m0AppendBaseFromPlan(table)
	require.False(t, ok)

	table.Cols[3].Default.NullAbility = true
	table.Cols[3].Seqnum = 2
	_, ok = m0AppendBaseFromPlan(table)
	require.False(t, ok)

	table.Cols[3].Seqnum = 3
	table.Cols[3].Default = nil
	_, ok = m0AppendBaseFromPlan(table)
	require.False(t, ok)

	table.Cols[3].Default = &plan.Default{NullAbility: true}
	table.RefChildTbls = []uint64{42}
	_, ok = m0AppendBaseFromPlan(table)
	require.False(t, ok)

	activeM0Enabled = false
	_, ok = m0AppendBaseFromPlan(table)
	require.False(t, ok)
}
