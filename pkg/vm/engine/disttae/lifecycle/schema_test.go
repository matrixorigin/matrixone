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

package lifecycle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func TestBuildSchemaDescriptorPreservesRestoreSemantics(t *testing.T) {
	table := &plan.TableDef{
		TblId:   42,
		Name:    "events",
		DbName:  "db",
		Version: 7,
		Cols: []*plan.ColDef{
			{
				ColId:   11,
				Name:    "id",
				NotNull: true,
				Typ: plan.Type{
					Id:          int32(types.T_uint64),
					NotNullable: true,
					AutoIncr:    true,
				},
			},
			{
				ColId: 12,
				Name:  "event_time",
				Typ: plan.Type{
					Id:    int32(types.T_timestamp),
					Scale: 6,
				},
				Default: &plan.Default{OriginString: "current_timestamp"},
			},
		},
	}

	descriptor, digest, err := BuildSchemaDescriptor(context.Background(), table)
	require.NoError(t, err)
	require.Equal(t, uint16(1), descriptor.FormatVersion)
	require.Equal(t, "events", descriptor.SourceTableName)
	require.Len(t, descriptor.Columns, 2)
	require.Equal(t, uint64(11), descriptor.Columns[0].SourceColumnID)
	require.True(t, descriptor.Columns[0].AutoIncrement)
	require.Equal(t, int32(6), descriptor.Columns[1].Scale)
	require.Equal(t, "current_timestamp", descriptor.Columns[1].DefaultExpression)
	require.NotEqual(t, [32]byte{}, digest)

	again, againDigest, err := BuildSchemaDescriptor(context.Background(), table)
	require.NoError(t, err)
	require.Equal(t, descriptor, again)
	require.Equal(t, digest, againDigest)
}

func TestBuildSchemaDescriptorRejectsUnsupportedColumn(t *testing.T) {
	table := &plan.TableDef{
		Name: "unsupported",
		Cols: []*plan.ColDef{{
			ColId: 1,
			Name:  "embedding",
			Typ:   plan.Type{Id: int32(types.T_array_float32)},
		}},
	}
	_, _, err := BuildSchemaDescriptor(context.Background(), table)
	require.Error(t, err)
}

func TestBuildSchemaDescriptorRejectsOnUpdateColumn(t *testing.T) {
	table := &plan.TableDef{
		Name: "on_update",
		Cols: []*plan.ColDef{{
			ColId:    1,
			Name:     "updated_at",
			Typ:      plan.Type{Id: int32(types.T_timestamp)},
			OnUpdate: &plan.OnUpdate{OriginString: "current_timestamp"},
		}},
	}
	_, _, err := BuildSchemaDescriptor(context.Background(), table)
	require.ErrorContains(t, err, "ON UPDATE")
}

func TestBuildSchemaDescriptorRejectsEncodedSQLTypes(t *testing.T) {
	for _, test := range []struct {
		name string
		typ  plan.Type
	}{
		{
			name: "enum",
			typ: plan.Type{
				Id:         int32(types.T_enum),
				Enumvalues: "red,green",
			},
		},
		{
			name: "set encoded as uint64",
			typ: plan.Type{
				Id:         int32(types.T_uint64),
				Enumvalues: "read,write",
			},
		},
		{
			name: "typed array encoded as json",
			typ: plan.Type{
				Id:         int32(types.T_json),
				Enumvalues: "array(varchar(20))",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			table := &plan.TableDef{
				Name: "encoded_type",
				Cols: []*plan.ColDef{{
					ColId: 1,
					Name:  "value",
					Typ:   test.typ,
				}},
			}
			_, _, err := BuildSchemaDescriptor(context.Background(), table)
			require.ErrorContains(t, err, "encoded SQL type")
		})
	}
}

func TestSchemaDescriptorRestoreDDLDoesNotReuseSourceColumnIDs(t *testing.T) {
	descriptor := SchemaDescriptor{
		FormatVersion:   1,
		SourceTableName: "events",
		Columns: []SchemaColumn{
			{
				Ordinal:        0,
				SourceColumnID: 99,
				Name:           "id",
				TypeID:         int32(types.T_int64),
				NotNull:        true,
			},
		},
	}
	ddl, err := descriptor.BuildRestoreCreateTableSQL(
		context.Background(),
		"db",
		"__mo_lifecycle_restore_1",
	)
	require.NoError(t, err)
	require.Contains(t, ddl, "`id` BIGINT NOT NULL")
	require.NotContains(t, ddl, "99")
}
