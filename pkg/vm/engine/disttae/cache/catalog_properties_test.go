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

package cache

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestGetTableDefCatalogPropertiesRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name, comment, createSQL string
		legacy                   bool
	}{
		{name: "new table", comment: "current comment", createSQL: "create table t(id int primary key)"},
		{name: "historical duplicates", comment: "current comment", createSQL: "create table t(id int primary key)", legacy: true},
		{name: "empty catalog fields", legacy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pkey := &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}
			indexes := []*plan.IndexDef{{IndexName: "idx_id", Parts: []string{"id"}, Unique: true}}
			fkeys := []*plan.ForeignKeyDef{{Name: "fk_parent", Cols: []uint64{1}, ForeignTbl: 42, ForeignCols: []uint64{1}}}
			children := []uint64{43}
			constraint := &engine.ConstraintDef{Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{Pkey: pkey}, &engine.IndexDef{Indexes: indexes},
				&engine.ForeignKeyDef{Fkeys: fkeys}, &engine.RefChildTableDef{Tables: children},
			}}
			custom := []*plan.Property{{Key: "connector", Value: "kafka"}, {Key: "topic", Value: "first"}, {Key: "topic", Value: "second"}}
			if tc.legacy {
				constraint.Cts = append(constraint.Cts,
					&engine.StreamConfigsDef{Configs: []*plan.Property{
						{Key: catalog.SystemRelAttr_Kind, Value: "stale"}, custom[0],
						{Key: "REL_COMMENT", Value: "stale comment"}, custom[1],
						{Key: catalog.SystemRelAttr_CreateSQL, Value: "stale SQL"},
					}},
					&engine.StreamConfigsDef{Configs: []*plan.Property{
						{Key: "RELKIND", Value: "older"}, custom[2],
						{Key: catalog.SystemRelAttr_Comment, Value: "older comment"},
						{Key: "REL_CREATESQL", Value: "older SQL"},
					}},
				)
			}
			wire, err := constraint.MarshalBinary()
			require.NoError(t, err)
			item := &TableItem{
				Name: "t", Kind: catalog.SystemOrdinaryRel, Comment: tc.comment,
				CreateSql: tc.createSQL, Constraint: wire, ExtraInfo: &api.SchemaExtra{},
			}
			var want []*plan.Property
			if tc.comment != "" {
				want = append(want, &plan.Property{Key: catalog.SystemRelAttr_Comment, Value: tc.comment})
			}
			if tc.legacy {
				want = append(want, custom...)
			}
			want = append(want, &plan.Property{Key: catalog.SystemRelAttr_Kind, Value: item.Kind})
			if tc.createSQL != "" {
				want = append(want, &plan.Property{Key: catalog.SystemRelAttr_CreateSQL, Value: tc.createSQL})
			}

			var stableWire []byte
			for round := 0; round < 3; round++ {
				before := bytes.Clone(item.Constraint)
				decoded, exeDefs := getTableDef(item, nil)
				require.NotNil(t, decoded)
				require.Equal(t, before, item.Constraint, "reader must not mutate persisted input")
				require.Equal(t, item.Kind, decoded.TableType)
				require.Equal(t, tc.createSQL, decoded.Createsql)
				var properties []*plan.Property
				for _, def := range decoded.Defs {
					properties = append(properties, def.GetProperties().GetProperties()...)
				}
				require.Equal(t, want, properties, "round %d", round)
				require.Equal(t, pkey, decoded.Pkey)
				require.Equal(t, indexes, decoded.Indexes)
				require.Equal(t, fkeys, decoded.Fkeys)
				require.Equal(t, children, decoded.RefChildTbls)
				constraintCount := 0
				for _, def := range exeDefs {
					if c, ok := def.(*engine.ConstraintDef); ok {
						constraintCount++
						originalWire, err := c.MarshalBinary()
						require.NoError(t, err)
						require.Equal(t, before, originalWire, "engine constraint remains wire compatible")
					}
				}
				require.Equal(t, 1, constraintCount)

				copied := plan2.DeepCopyTableDef(decoded, true)
				converted := engine.PlanDefToCstrDef(copied)
				if round == 1 {
					defs, _, err := engine.PlanDefsToExeDefs(copied)
					require.NoError(t, err)
					constraintCount = 0
					for _, def := range defs {
						if c, ok := def.(*engine.ConstraintDef); ok {
							constraintCount++
							converted = c
						}
					}
					require.Equal(t, 1, constraintCount)
				}
				item.Constraint, err = converted.MarshalBinary()
				require.NoError(t, err)
				if round == 0 {
					stableWire = bytes.Clone(item.Constraint)
				}
				require.Equal(t, stableWire, item.Constraint, "repeated schema rewrites must be idempotent")
			}
		})
	}
}
