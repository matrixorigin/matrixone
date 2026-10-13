// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package catalog

import (
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewDefinitionPersistence(t *testing.T) {
	def := MVDefinition{FormatVersion: 1, Query: "select k,sum(v) from db.t group by k", Sources: []MVSource{{AccountID: 7, DatabaseID: 3, TableID: 8, LogicalID: 9, Version: 2, Database: "db", Table: "t"}}, SQLMode: "NO_UNSIGNED_SUBTRACTION", TimeZone: "Asia/Shanghai", OwnerUser: 10, OwnerRole: 11, OwnerName: "owner"}
	data, err := json.Marshal(def)
	require.NoError(t, err)
	for _, props := range []bool{true, false} {
		table := &plan.TableDef{TableType: SystemMaterializedViewRel}
		if props {
			table.Props = []*plan.PropertyDef{{Key: MaterializedViewProperty, Value: string(data)}}
		} else {
			table.Defs = []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: []*plan.Property{{Key: MaterializedViewProperty, Value: string(data)}}}}}}
		}
		decoded, err := MaterializedViewDefinition(table)
		require.NoError(t, err)
		require.Equal(t, def, *decoded)
	}
	for _, raw := range []string{"", "invalid", `{"format_version":2,"query":"select * from t","sources":[{}]}`, `{"format_version":1,"query":"select 1"}`} {
		_, err := MaterializedViewDefinition(&plan.TableDef{TableType: SystemMaterializedViewRel, Props: []*plan.PropertyDef{{Key: MaterializedViewProperty, Value: raw}}})
		require.Error(t, err)
	}
	decoded, err := MaterializedViewDefinition(&plan.TableDef{TableType: SystemOrdinaryRel})
	require.NoError(t, err)
	require.Nil(t, decoded)
}
