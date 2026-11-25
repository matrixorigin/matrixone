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

package disttae

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestTxnTableGetTableDefCatalogProperties(t *testing.T) {
	for _, populated := range []bool{false, true} {
		name := "empty catalog fields"
		if populated {
			name = "current catalog fields"
		}
		t.Run(name, func(t *testing.T) {
			custom := []*plan.Property{{Key: "topic", Value: "first"}, {Key: "topic", Value: "second"}}
			constraint := &engine.ConstraintDef{Cts: []engine.Constraint{
				&engine.StreamConfigsDef{Configs: []*plan.Property{
					{Key: "RELKIND", Value: "stale"}, custom[0],
					{Key: "REL_COMMENT", Value: "stale comment"},
				}},
				&engine.StreamConfigsDef{Configs: []*plan.Property{
					{Key: catalog.SystemRelAttr_CreateSQL, Value: "stale SQL"}, custom[1],
					{Key: catalog.SystemRelAttr_Kind, Value: "older"},
				}},
			}}
			wire, err := constraint.MarshalBinary()
			require.NoError(t, err)
			table := &txnTable{db: &txnDatabase{}, relKind: catalog.SystemOrdinaryRel, constraint: wire}
			var want []*plan.Property
			if populated {
				table.comment, table.createSql = "current comment", "create table t(id int)"
				want = append(want, &plan.Property{Key: catalog.SystemRelAttr_Comment, Value: table.comment})
			}
			want = append(want, custom...)
			want = append(want, &plan.Property{Key: catalog.SystemRelAttr_Kind, Value: table.relKind})
			if populated {
				want = append(want, &plan.Property{Key: catalog.SystemRelAttr_CreateSQL, Value: table.createSql})
			}
			var stableWire []byte
			for round := 0; round < 3; round++ {
				before := bytes.Clone(table.constraint)
				decoded := table.GetTableDef(context.Background())
				require.NotNil(t, decoded)
				require.Equal(t, before, table.constraint)
				require.Equal(t, table.relKind, decoded.TableType)
				require.Equal(t, table.createSql, decoded.Createsql)
				var properties []*plan.Property
				for _, def := range decoded.Defs {
					properties = append(properties, def.GetProperties().GetProperties()...)
				}
				require.Equal(t, want, properties, "round %d", round)
				table.constraint, err = engine.PlanDefToCstrDef(decoded).MarshalBinary()
				require.NoError(t, err)
				if round == 0 {
					stableWire = bytes.Clone(table.constraint)
				}
				require.Equal(t, stableWire, table.constraint)
				table.tableDef = nil
			}
		})
	}
}
