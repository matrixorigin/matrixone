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

package lifecycle

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBindingSchemaDigestIsStableAndFencesPhysicalSchemaIdentity(t *testing.T) {
	require.Equal(t, [32]byte{}, BindingSchemaDigest(nil))

	table := &plan.TableDef{
		TblId:     42,
		LogicalId: 7,
		Version:   3,
		DbName:    "analytics",
		Name:      "events",
		Cols: []*plan.ColDef{
			{
				ColId:   11,
				Name:    "event_id",
				Seqnum:  0,
				NotNull: true,
				Typ: plan.Type{
					Id:          int32(types.T_uint64),
					NotNullable: true,
					AutoIncr:    true,
				},
				Default: &plan.Default{OriginString: "42"},
			},
			nil,
			{
				ColId:  12,
				Name:   "payload",
				Seqnum: 2,
				Hidden: true,
				Typ: plan.Type{
					Id:         int32(types.T_varchar),
					Width:      128,
					Enumvalues: "",
				},
			},
		},
	}

	want := BindingSchemaDigest(table)
	require.NotEqual(t, [32]byte{}, want)
	require.Equal(t, want, BindingSchemaDigest(cloneBindingSchemaTable(table)))

	for _, mutate := range []func(*plan.TableDef){
		func(value *plan.TableDef) { value.TblId++ },
		func(value *plan.TableDef) { value.LogicalId++ },
		func(value *plan.TableDef) { value.Version++ },
		func(value *plan.TableDef) { value.DbName = "other" },
		func(value *plan.TableDef) { value.Name = "other_events" },
		func(value *plan.TableDef) { value.Cols[0].Typ.AutoIncr = false },
		func(value *plan.TableDef) { value.Cols[2].Typ.Width++ },
		func(value *plan.TableDef) { value.Cols[0].Default.OriginString = "43" },
	} {
		changed := cloneBindingSchemaTable(table)
		mutate(changed)
		require.NotEqual(t, want, BindingSchemaDigest(changed))
	}
}

func cloneBindingSchemaTable(source *plan.TableDef) *plan.TableDef {
	cloned := *source
	cloned.Cols = make([]*plan.ColDef, len(source.Cols))
	for index, column := range source.Cols {
		if column == nil {
			continue
		}
		columnClone := *column
		if column.Default != nil {
			defaultClone := *column.Default
			columnClone.Default = &defaultClone
		}
		cloned.Cols[index] = &columnClone
	}
	return &cloned
}
