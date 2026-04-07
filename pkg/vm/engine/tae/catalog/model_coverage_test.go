// Copyright 2024 Matrix Origin
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

package catalog

import (
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoverage_DefsToSchema_FromPublicationProperty(t *testing.T) {
	defs := []engine.TableDef{
		&engine.AttributeDef{
			Attr: engine.Attribute{
				Name: "id",
				Type: types.T_int32.ToType(),
			},
		},
		&engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						PkeyColName: "id",
						Names:       []string{"id"},
					},
				},
			},
		},
		&engine.PropertiesDef{
			Properties: []engine.Property{
				{
					Key:   pkgcatalog.PropFromPublication,
					Value: "true",
				},
			},
		},
	}

	schema, err := DefsToSchema("test_pub_table", defs)
	require.NoError(t, err)
	assert.True(t, schema.FromPublication, "FromPublication should be true when property is set")
}

func TestCoverage_DefsToSchema_FromPublicationPropertyFalse(t *testing.T) {
	defs := []engine.TableDef{
		&engine.AttributeDef{
			Attr: engine.Attribute{
				Name: "id",
				Type: types.T_int32.ToType(),
			},
		},
		&engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						PkeyColName: "id",
						Names:       []string{"id"},
					},
				},
			},
		},
		&engine.PropertiesDef{
			Properties: []engine.Property{
				{
					Key:   pkgcatalog.PropFromPublication,
					Value: "false",
				},
			},
		},
	}

	schema, err := DefsToSchema("test_table", defs)
	require.NoError(t, err)
	assert.False(t, schema.FromPublication, "FromPublication should be false when value is not 'true'")
}

func TestCoverage_DefsToSchema_FromPublicationExtra(t *testing.T) {
	extra := &api.SchemaExtra{
		FromPublication: true,
	}
	extraBytes := api.MustMarshalTblExtra(extra)

	defs := []engine.TableDef{
		&engine.AttributeDef{
			Attr: engine.Attribute{
				Name: "id",
				Type: types.T_int32.ToType(),
			},
		},
		&engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						PkeyColName: "id",
						Names:       []string{"id"},
					},
				},
			},
		},
		&engine.PropertiesDef{
			Properties: []engine.Property{
				{
					Key:   pkgcatalog.PropSchemaExtra,
					Value: string(extraBytes),
				},
			},
		},
	}

	schema, err := DefsToSchema("test_extra_pub", defs)
	require.NoError(t, err)
	assert.True(t, schema.FromPublication, "FromPublication should be true when set via Extra")
}

func TestCoverage_DefsToSchema_WithPartition(t *testing.T) {
	defs := []engine.TableDef{
		&engine.AttributeDef{
			Attr: engine.Attribute{
				Name: "id",
				Type: types.T_int32.ToType(),
			},
		},
		&engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						PkeyColName: "id",
						Names:       []string{"id"},
					},
				},
			},
		},
		&engine.PartitionDef{
			Partitioned: 1,
			Partition:   "partition by range(id)",
		},
	}

	schema, err := DefsToSchema("test_partitioned", defs)
	require.NoError(t, err)
	assert.Equal(t, int8(1), schema.Partitioned)
	assert.Equal(t, "partition by range(id)", schema.Partition)
}

func TestCoverage_DefsToSchema_WithView(t *testing.T) {
	defs := []engine.TableDef{
		&engine.AttributeDef{
			Attr: engine.Attribute{
				Name: "id",
				Type: types.T_int32.ToType(),
			},
		},
		&engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						PkeyColName: "id",
						Names:       []string{"id"},
					},
				},
			},
		},
		&engine.ViewDef{
			View: "SELECT 1",
		},
	}

	schema, err := DefsToSchema("test_view", defs)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1", schema.View)
}

func TestCoverage_DefsToSchema_WithComment(t *testing.T) {
	defs := []engine.TableDef{
		&engine.AttributeDef{
			Attr: engine.Attribute{
				Name: "id",
				Type: types.T_int32.ToType(),
			},
		},
		&engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						PkeyColName: "id",
						Names:       []string{"id"},
					},
				},
			},
		},
		&engine.CommentDef{
			Comment: "test comment",
		},
	}

	schema, err := DefsToSchema("test_comment", defs)
	require.NoError(t, err)
	assert.Equal(t, "test comment", schema.Comment)
}

func TestCoverage_DefsToSchema_WithPropertiesComment(t *testing.T) {
	defs := []engine.TableDef{
		&engine.AttributeDef{
			Attr: engine.Attribute{
				Name: "id",
				Type: types.T_int32.ToType(),
			},
		},
		&engine.ConstraintDef{
			Cts: []engine.Constraint{
				&engine.PrimaryKeyDef{
					Pkey: &plan.PrimaryKeyDef{
						PkeyColName: "id",
						Names:       []string{"id"},
					},
				},
			},
		},
		&engine.PropertiesDef{
			Properties: []engine.Property{
				{Key: pkgcatalog.SystemRelAttr_Comment, Value: "property comment"},
				{Key: pkgcatalog.SystemRelAttr_Kind, Value: "r"},
				{Key: pkgcatalog.SystemRelAttr_CreateSQL, Value: "CREATE TABLE t(id int)"},
			},
		},
	}

	schema, err := DefsToSchema("test_props", defs)
	require.NoError(t, err)
	assert.Equal(t, "property comment", schema.Comment)
	assert.Equal(t, "r", schema.Relkind)
	assert.Equal(t, "CREATE TABLE t(id int)", schema.Createsql)
}

func TestCoverage_DefsToSchema_ClusterBy(t *testing.T) {
	defs := []engine.TableDef{
		&engine.AttributeDef{
			Attr: engine.Attribute{
				Name:      "id",
				Type:      types.T_int32.ToType(),
				ClusterBy: true,
			},
		},
		&engine.ConstraintDef{},
	}

	schema, err := DefsToSchema("test_cluster", defs)
	require.NoError(t, err)
	require.True(t, len(schema.ColDefs) > 0)
}

func TestCoverage_SchemaToDefs_WithFromPublication(t *testing.T) {
	schema := NewEmptySchema("test_pub")
	schema.FromPublication = true
	require.NoError(t, schema.AppendPKCol("id", types.T_int32.ToType(), 0))
	require.NoError(t, schema.Finalize(false))

	defs, err := SchemaToDefs(schema)
	require.NoError(t, err)

	// Find the PropertiesDef and check for from_publication
	found := false
	for _, def := range defs {
		if propDef, ok := def.(*engine.PropertiesDef); ok {
			for _, prop := range propDef.Properties {
				if prop.Key == pkgcatalog.PropFromPublication && prop.Value == "true" {
					found = true
				}
			}
		}
	}
	assert.True(t, found, "FromPublication property should be in SchemaToDefs output")
}

func TestCoverage_SchemaToDefs_WithoutFromPublication(t *testing.T) {
	schema := NewEmptySchema("test_no_pub")
	schema.FromPublication = false
	require.NoError(t, schema.AppendPKCol("id", types.T_int32.ToType(), 0))
	require.NoError(t, schema.Finalize(false))

	defs, err := SchemaToDefs(schema)
	require.NoError(t, err)

	// Check that from_publication property is NOT present
	for _, def := range defs {
		if propDef, ok := def.(*engine.PropertiesDef); ok {
			for _, prop := range propDef.Properties {
				assert.NotEqual(t, pkgcatalog.PropFromPublication, prop.Key,
					"FromPublication property should not be in SchemaToDefs output when false")
			}
		}
	}
}

func TestCoverage_SchemaToDefs_WithComment(t *testing.T) {
	schema := NewEmptySchema("test_comment")
	schema.Comment = "my comment"
	require.NoError(t, schema.AppendPKCol("id", types.T_int32.ToType(), 0))
	require.NoError(t, schema.Finalize(false))

	defs, err := SchemaToDefs(schema)
	require.NoError(t, err)

	found := false
	for _, def := range defs {
		if commentDef, ok := def.(*engine.CommentDef); ok {
			assert.Equal(t, "my comment", commentDef.Comment)
			found = true
		}
	}
	assert.True(t, found, "CommentDef should be in output")
}

func TestCoverage_SchemaToDefs_WithPartition(t *testing.T) {
	schema := NewEmptySchema("test_partition")
	schema.Partitioned = 1
	schema.Partition = "partition by range(id)"
	require.NoError(t, schema.AppendPKCol("id", types.T_int32.ToType(), 0))
	require.NoError(t, schema.Finalize(false))

	defs, err := SchemaToDefs(schema)
	require.NoError(t, err)

	found := false
	for _, def := range defs {
		if partDef, ok := def.(*engine.PartitionDef); ok {
			assert.Equal(t, int8(1), partDef.Partitioned)
			assert.Equal(t, "partition by range(id)", partDef.Partition)
			found = true
		}
	}
	assert.True(t, found, "PartitionDef should be in output")
}

func TestCoverage_SchemaToDefs_WithView(t *testing.T) {
	schema := NewEmptySchema("test_view")
	schema.View = "SELECT 1"
	require.NoError(t, schema.AppendPKCol("id", types.T_int32.ToType(), 0))
	require.NoError(t, schema.Finalize(false))

	defs, err := SchemaToDefs(schema)
	require.NoError(t, err)

	found := false
	for _, def := range defs {
		if viewDef, ok := def.(*engine.ViewDef); ok {
			assert.Equal(t, "SELECT 1", viewDef.View)
			found = true
		}
	}
	assert.True(t, found, "ViewDef should be in output")
}

func TestCoverage_SchemaToDefs_WithCreatesql(t *testing.T) {
	schema := NewEmptySchema("test_createsql")
	schema.Createsql = "CREATE TABLE t(id int)"
	require.NoError(t, schema.AppendPKCol("id", types.T_int32.ToType(), 0))
	require.NoError(t, schema.Finalize(false))

	defs, err := SchemaToDefs(schema)
	require.NoError(t, err)

	found := false
	for _, def := range defs {
		if propDef, ok := def.(*engine.PropertiesDef); ok {
			for _, prop := range propDef.Properties {
				if prop.Key == pkgcatalog.SystemRelAttr_CreateSQL {
					assert.Equal(t, "CREATE TABLE t(id int)", prop.Value)
					found = true
				}
			}
		}
	}
	assert.True(t, found, "Createsql property should be in output")
}

func TestCoverage_AttrFromColDef_Basic(t *testing.T) {
	col := &ColDef{
		Name:    "test_col",
		Type:    types.T_int32.ToType(),
		SortIdx: 0,
		SortKey: true,
	}

	attr, err := AttrFromColDef(col)
	require.NoError(t, err)
	assert.Equal(t, "test_col", attr.Name)
	assert.False(t, attr.IsHidden)
}

func TestCoverage_AttrFromColDef_WithDefault(t *testing.T) {
	defaultVal := &plan.Default{
		NullAbility:  true,
		OriginString: "42",
	}
	encoded, err := types.Encode(defaultVal)
	require.NoError(t, err)

	col := &ColDef{
		Name:    "col_with_default",
		Type:    types.T_int32.ToType(),
		Default: encoded,
	}

	attr, err := AttrFromColDef(col)
	require.NoError(t, err)
	assert.NotNil(t, attr.Default)
	assert.Equal(t, "42", attr.Default.OriginString)
}

func TestCoverage_AttrFromColDef_NoDefault(t *testing.T) {
	col := &ColDef{
		Name: "col_no_default",
		Type: types.T_varchar.ToType(),
	}

	attr, err := AttrFromColDef(col)
	require.NoError(t, err)
	assert.Nil(t, attr.Default)
	assert.Nil(t, attr.OnUpdate)
	assert.Nil(t, attr.GeneratedCol)
}

func TestCoverage_DefsToSchema_RoundTrip(t *testing.T) {
	// Create schema -> convert to defs -> should have consistent properties
	schema := NewEmptySchema("roundtrip_test")
	schema.Comment = "test"
	schema.FromPublication = true
	schema.View = "SELECT 1"
	require.NoError(t, schema.AppendPKCol("id", types.T_int32.ToType(), 0))
	require.NoError(t, schema.Finalize(false))

	defs, err := SchemaToDefs(schema)
	require.NoError(t, err)
	require.True(t, len(defs) > 0)

	// Re-create schema from defs
	schema2, err := DefsToSchema("roundtrip_test", defs)
	require.NoError(t, err)
	assert.Equal(t, "test", schema2.Comment)
	assert.Equal(t, "SELECT 1", schema2.View)
	assert.True(t, schema2.FromPublication)
}

func TestCoverage_SystemSchemas_Init(t *testing.T) {
	// Verify system schemas were initialized correctly
	assert.NotNil(t, SystemDBSchema)
	assert.NotNil(t, SystemTableSchema)
	assert.NotNil(t, SystemColumnSchema)
	assert.NotNil(t, SystemIndexTableSchema)
}

func TestCoverage_Constants(t *testing.T) {
	assert.Equal(t, uint32(0), TenantSysID)
	assert.Equal(t, "_ModelSchema", ModelSchemaName)
}
