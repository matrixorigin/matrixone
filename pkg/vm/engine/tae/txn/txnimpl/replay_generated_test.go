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

package txnimpl

import (
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestReplayGeneratedColumnsRequiresMatchingCatalogRows(t *testing.T) {
	tests := []struct {
		name           string
		mutate         func(*pkgcatalog.Table, []pkgcatalog.Column) []pkgcatalog.Column
		omitTable      bool
		duplicateTable bool
		omitAttribute  string
		want           bool
	}{
		{name: "complete", want: true},
		{name: "missing_table_row", omitTable: true},
		{name: "duplicate_table_row", duplicateTable: true},
		{name: "missing_marker", omitAttribute: pkgcatalog.SystemColAttr_HasGenerated},
		{name: "missing_expression", omitAttribute: pkgcatalog.SystemColAttr_Generated},
		{name: "wrong_version", mutate: func(table *pkgcatalog.Table, cols []pkgcatalog.Column) []pkgcatalog.Column {
			table.Version++
			return cols
		}},
		{name: "wrong_tenant", mutate: func(_ *pkgcatalog.Table, cols []pkgcatalog.Column) []pkgcatalog.Column {
			cols[0].AccountId++
			return cols
		}},
		{name: "wrong_relation", mutate: func(_ *pkgcatalog.Table, cols []pkgcatalog.Column) []pkgcatalog.Column {
			cols[0].TableId++
			return cols
		}},
		{name: "missing_column", mutate: func(_ *pkgcatalog.Table, cols []pkgcatalog.Column) []pkgcatalog.Column { return cols[1:] }},
		{name: "duplicate_column", mutate: func(_ *pkgcatalog.Table, cols []pkgcatalog.Column) []pkgcatalog.Column { return append(cols, cols[0]) }},
		{name: "wrong_default", mutate: func(_ *pkgcatalog.Table, cols []pkgcatalog.Column) []pkgcatalog.Column {
			cols[0].DefaultExpr = []byte("different")
			return cols
		}},
		{name: "default_marker_without_expression", mutate: func(_ *pkgcatalog.Table, cols []pkgcatalog.Column) []pkgcatalog.Column {
			cols[0].HasDef = 1
			return cols
		}},
		{name: "update_marker_without_expression", mutate: func(_ *pkgcatalog.Table, cols []pkgcatalog.Column) []pkgcatalog.Column {
			cols[0].HasUpdate = 1
			return cols
		}},
		{name: "invalid_hidden_marker", mutate: func(_ *pkgcatalog.Table, cols []pkgcatalog.Column) []pkgcatalog.Column {
			cols[0].IsHidden = 2
			return cols
		}},
		{name: "malformed_generated_expression", mutate: func(_ *pkgcatalog.Table, cols []pkgcatalog.Column) []pkgcatalog.Column {
			cols[0].GeneratedExpr = []byte{0xff}
			return cols
		}},
		{name: "wrong_generated_flag", mutate: func(_ *pkgcatalog.Table, cols []pkgcatalog.Column) []pkgcatalog.Column {
			cols[0].HasGenerated = 0
			return cols
		}},
		{name: "wrong_type_width", mutate: func(_ *pkgcatalog.Table, cols []pkgcatalog.Column) []pkgcatalog.Column {
			var typ types.Type
			if err := types.Decode(cols[0].Typ, &typ); err != nil {
				panic(err)
			}
			typ.Width++
			var err error
			cols[0].Typ, err = types.Encode(&typ)
			if err != nil {
				panic(err)
			}
			return cols
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			packer := types.NewPacker()
			defer packer.Close()
			schema := catalog.MockSchemaAll(3, 1)
			schema.Name = "generated_table"
			schema.Version = 3
			generated, err := types.Encode(&plan.GeneratedCol{OriginString: "a + 1", IsStored: true})
			require.NoError(t, err)
			schema.ColDefs[0].GeneratedCol = generated
			secondGenerated, err := types.Encode(&plan.GeneratedCol{OriginString: "b + 2", IsStored: true})
			require.NoError(t, err)
			schema.ColDefs[1].GeneratedCol = secondGenerated
			defs, err := catalog.SchemaToDefs(schema)
			require.NoError(t, err)
			cols, err := pkgcatalog.GenColumnsFromDefs(0, schema.Name, "db", 42, 9, defs)
			require.NoError(t, err)
			// MockSchemaAll marks the primary key NOT NULL without a Default;
			// reproduce the catalog row written by the TN schema fixture.
			for i := range cols[:len(cols)-1] {
				if !schema.ColDefs[i].NullAbility {
					cols[i].NotNull = 1
				}
			}
			table := pkgcatalog.Table{TableId: 42, DatabaseId: 9, TableName: schema.Name, DatabaseName: "db", Version: schema.Version}
			if tc.mutate != nil {
				cols = tc.mutate(&table, cols)
			}
			tableCN, err := pkgcatalog.GenCreateTableTuple(table, mp, packer)
			require.NoError(t, err)
			tableTN := containers.ToTNBatch(tableCN, mp)
			defer tableTN.Close()
			colsCN, err := pkgcatalog.GenCreateColumnTuples(cols, mp, packer)
			require.NoError(t, err)
			colsTN := containers.ToTNBatch(colsCN, mp)
			defer colsTN.Close()
			if tc.omitAttribute != "" {
				delete(colsTN.Nameidx, tc.omitAttribute)
			}
			schema.ColDefs[0].GeneratedCol = nil // the existing WAL schema codec omits it
			schema.ColDefs[1].GeneratedCol = nil
			key := replayGeneratedKey{9, 42}
			targets := map[replayGeneratedKey]*replayGeneratedTarget{key: {schema: schema}}
			if !tc.omitTable {
				replayGeneratedTableRow(targets, tableTN, 0)
			}
			if tc.duplicateTable {
				replayGeneratedTableRow(targets, tableTN, 0)
			}
			for i := range cols {
				replayGeneratedColumnRow(targets, colsTN, i)
			}
			applyReplayGeneratedTargets(targets)
			if tc.want {
				require.Equal(t, generated, schema.ColDefs[0].GeneratedCol)
				require.Equal(t, secondGenerated, schema.ColDefs[1].GeneratedCol)
				colsTN.GetVectorByName(pkgcatalog.SystemColAttr_Generated).GetDownstreamVector().GetBytesAt(0)[0] ^= 0xff
				require.Equal(t, generated, schema.ColDefs[0].GeneratedCol)
			} else {
				require.Empty(t, schema.ColDefs[0].GeneratedCol)
				require.Empty(t, schema.ColDefs[1].GeneratedCol, "an inconsistent catalog row must not partially hydrate the schema")
			}
		})
	}
}
