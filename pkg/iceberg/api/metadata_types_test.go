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

package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTableMetadataSchemaAndSpecLookup(t *testing.T) {
	meta := TableMetadata{
		CurrentSchemaID: 7,
		Schemas: []Schema{
			{SchemaID: 1},
			{SchemaID: 7, Fields: []SchemaField{{ID: 10, Name: "id", Type: IcebergType{Kind: TypeLong}}}},
		},
		DefaultSpecID: 3,
		PartitionSpecs: []PartitionSpec{
			{SpecID: 2},
			{SpecID: 3, Fields: []PartitionField{{SourceID: 10, FieldID: 1000, Name: "id_bucket", Transform: "bucket[16]"}}},
		},
	}
	schema, ok := meta.CurrentSchema()
	require.True(t, ok)
	require.Equal(t, 7, schema.SchemaID)
	spec, ok := meta.DefaultSpec()
	require.True(t, ok)
	require.Equal(t, 3, spec.SpecID)

	meta.CurrentSchemaID = 99
	_, ok = meta.CurrentSchema()
	require.False(t, ok)
	meta.DefaultSpecID = 99
	_, ok = meta.DefaultSpec()
	require.False(t, ok)
}

func TestParseIcebergTypeStringAndString(t *testing.T) {
	cases := []struct {
		raw  string
		kind IcebergTypeKind
		text string
	}{
		{" Boolean ", TypeBoolean, "boolean"},
		{"decimal( 12 , 2 )", TypeDecimal, "decimal(12,2)"},
		{"fixed[ 16 ]", TypeFixed, "fixed[16]"},
		{"timestamp-ns", TypeTimestampNS, "timestamp-ns"},
	}
	for _, tc := range cases {
		typ, err := ParseIcebergTypeString(tc.raw)
		require.NoError(t, err)
		require.Equal(t, tc.kind, typ.Kind)
		require.Equal(t, tc.text, typ.String())
	}
	require.Equal(t, "unknown", (IcebergType{}).String())
	require.Equal(t, "DECIMAL(38,6)", MOType{Name: "DECIMAL", Width: 38, Scale: 6}.String())
	require.Equal(t, "VARCHAR", MOType{Name: "VARCHAR"}.String())
	require.Equal(t, "", MOType{}.String())
}

func TestParseIcebergTypeStringRejectsInvalidShapes(t *testing.T) {
	for _, raw := range []string{"decimal(12)", "decimal(x,2)", "decimal(12,x)", "fixed[x]", "unsupported_type"} {
		_, err := ParseIcebergTypeString(raw)
		require.Error(t, err, raw)
	}
}

func TestIcebergTypeUnmarshalJSONPrimitiveAndNestedObjects(t *testing.T) {
	var primitive IcebergType
	require.NoError(t, json.Unmarshal([]byte(`"long"`), &primitive))
	require.Equal(t, TypeLong, primitive.Kind)

	var strct IcebergType
	require.NoError(t, json.Unmarshal([]byte(`{"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]}`), &strct))
	require.Equal(t, TypeStruct, strct.Kind)
	require.Len(t, strct.Fields, 1)
	require.True(t, strct.Fields[0].Required)

	var list IcebergType
	require.NoError(t, json.Unmarshal([]byte(`{"type":"list","element-id":2,"element-required":true,"element":"string"}`), &list))
	require.Equal(t, TypeList, list.Kind)
	require.Equal(t, 2, list.ElementID)
	require.True(t, list.ElementRequired)
	require.NotNil(t, list.Element)
	require.Equal(t, TypeString, list.Element.Kind)

	var mp IcebergType
	require.NoError(t, json.Unmarshal([]byte(`{"type":"map","key-id":3,"key":"string","value-id":4,"value-required":true,"value":"int"}`), &mp))
	require.Equal(t, TypeMap, mp.Kind)
	require.Equal(t, 3, mp.KeyID)
	require.Equal(t, 4, mp.ValueID)
	require.True(t, mp.ValueRequired)
	require.Equal(t, TypeString, mp.Key.Kind)
	require.Equal(t, TypeInt, mp.Value.Kind)

	var fixed IcebergType
	require.NoError(t, json.Unmarshal([]byte(`{"type":"fixed","length":32}`), &fixed))
	require.Equal(t, TypeFixed, fixed.Kind)
	require.Equal(t, 32, fixed.Length)
}

func TestIcebergTypeUnmarshalJSONRejectsInvalidObjects(t *testing.T) {
	for _, raw := range []string{
		`42`,
		`{"type":""}`,
		`{"type":"struct","fields":"bad"}`,
		`{"type":"list"}`,
		`{"type":"map","key":"string"}`,
	} {
		var typ IcebergType
		require.Error(t, json.Unmarshal([]byte(raw), &typ), raw)
	}
}
