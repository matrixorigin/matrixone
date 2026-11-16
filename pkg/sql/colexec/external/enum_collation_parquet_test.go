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

package external

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

type collatedEnumInput struct {
	name, domain, input string
	identity            uint32
	want                types.Enum
	invalid             bool
}

func collatedEnumInputs() []collatedEnumInput {
	return []collatedEnumInput{
		{name: "binary upper member", domain: "a,A", input: "A", identity: types.EnumCollationUTF8Bin, want: 2},
		{name: "binary lower member", domain: "a,A", input: "a", identity: types.EnumCollationUTF8Bin, want: 1},
		{name: "binary mismatched case", domain: "a", input: "A", identity: types.EnumCollationUTF8Bin, invalid: true},
		{name: "binary trailing space", domain: "a", input: "a ", identity: types.EnumCollationUTF8Bin, want: 1},
		{name: "binary physical padded member", domain: "a ,a", input: "a ", identity: types.EnumCollationUTF8Bin, want: 1},
		{name: "binary physical leading space", domain: " a,a", input: " a", identity: types.EnumCollationUTF8Bin, want: 1},
		{name: "binary leading space invalid", domain: "a", input: " a", identity: types.EnumCollationUTF8Bin, invalid: true},
		{name: "binary NBSP invalid", domain: "a", input: "a\u00a0", identity: types.EnumCollationUTF8Bin, invalid: true},
		{name: "general accent", domain: "a,b", input: "Á", identity: types.EnumCollationGeneralCI, want: 1},
		{name: "general NBSP invalid", domain: "a", input: "a\u00a0", identity: types.EnumCollationGeneralCI, invalid: true},
		{name: "empty member", domain: ",a", input: "", identity: types.EnumCollationUTF8Bin, want: 1},
	}
}

// Actual in-memory Parquet pages exercise the same mapper used by the reader.
func TestParquetEnumCollationPreservesInputWhitespace(t *testing.T) {
	cases := append(collatedEnumInputs(), collatedEnumInput{name: "legacy trim", domain: "a", input: " a\u00a0", identity: types.EnumCollationLegacy, want: 1})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			var data bytes.Buffer
			schema := parquet.NewSchema("enum", parquet.Group{"e": parquet.Leaf(parquet.ByteArrayType)})
			writer := parquet.NewWriter(&data, schema)
			_, err := writer.WriteRows([]parquet.Row{{parquet.ByteArrayValue([]byte(tc.input))}})
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			file, err := parquet.OpenFile(bytes.NewReader(data.Bytes()), int64(data.Len()))
			require.NoError(t, err)
			pages := file.RowGroups()[0].ColumnChunks()[0].Pages()
			defer pages.Close()
			page, err := pages.ReadPage()
			require.NoError(t, err)
			typ := plan.Type{Id: int32(types.T_enum), Enumvalues: tc.domain, EnumCollation: tc.identity}
			var handler ParquetHandler
			mapper := handler.getMapper(file.Root().Column("e"), typ)
			require.NotNil(t, mapper)
			out := vector.NewVec(types.T_enum.ToType())
			defer out.Free(proc.Mp())
			err = mapper.mapping(page, proc, out)
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[types.Enum](out, 0))
		})
	}
}

func TestParquetEnumOrdinalAndNull(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   parquet.Value
		null    bool
		invalid bool
		want    types.Enum
	}{
		{name: "ordinal two", value: parquet.Int32Value(2).Level(0, 1, 0), want: 2},
		{name: "ordinal zero invalid", value: parquet.Int32Value(0).Level(0, 1, 0), invalid: true},
		{name: "null", value: parquet.NullValue().Level(0, 0, 0), null: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			var data bytes.Buffer
			schema := parquet.NewSchema("enum", parquet.Group{"e": parquet.Optional(parquet.Leaf(parquet.Int32Type))})
			writer := parquet.NewWriter(&data, schema)
			_, err := writer.WriteRows([]parquet.Row{{tc.value}})
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			file, err := parquet.OpenFile(bytes.NewReader(data.Bytes()), int64(data.Len()))
			require.NoError(t, err)
			pages := file.RowGroups()[0].ColumnChunks()[0].Pages()
			defer pages.Close()
			page, err := pages.ReadPage()
			require.NoError(t, err)
			var handler ParquetHandler
			mapper := handler.getMapper(file.Root().Column("e"), plan.Type{Id: int32(types.T_enum), Enumvalues: "a,A", EnumCollation: types.EnumCollationUTF8Bin})
			require.NotNil(t, mapper)
			out := vector.NewVec(types.T_enum.ToType())
			defer out.Free(proc.Mp())
			err = mapper.mapping(page, proc, out)
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.null, out.GetNulls().Contains(0))
			if !tc.null {
				require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[types.Enum](out, 0))
			}
		})
	}
}

func TestParquetDictionaryEnumCollation(t *testing.T) {
	for _, tc := range []collatedEnumInput{
		{name: "padded member", domain: "a ,a", input: "a ", identity: types.EnumCollationUTF8Bin, want: 1},
		{name: "invalid NBSP", domain: "a", input: "a\u00a0", identity: types.EnumCollationUTF8Bin, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			var data bytes.Buffer
			node := parquet.Encoded(parquet.Leaf(parquet.ByteArrayType), &parquet.RLEDictionary)
			schema := parquet.NewSchema("enum", parquet.Group{"e": node})
			writer := parquet.NewWriter(&data, schema)
			_, err := writer.WriteRows([]parquet.Row{{parquet.ByteArrayValue([]byte(tc.input))}})
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			file, err := parquet.OpenFile(bytes.NewReader(data.Bytes()), int64(data.Len()))
			require.NoError(t, err)
			pages := file.RowGroups()[0].ColumnChunks()[0].Pages()
			defer pages.Close()
			page, err := pages.ReadPage()
			require.NoError(t, err)
			require.NotNil(t, page.Dictionary())
			var handler ParquetHandler
			mapper := handler.getMapper(file.Root().Column("e"), plan.Type{Id: int32(types.T_enum), Enumvalues: tc.domain, EnumCollation: tc.identity})
			require.NotNil(t, mapper)
			out := vector.NewVec(types.T_enum.ToType())
			defer out.Free(proc.Mp())
			err = mapper.mapping(page, proc, out)
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[types.Enum](out, 0))
		})
	}
}

func TestHivePartitionEnumCollation(t *testing.T) {
	for _, tc := range collatedEnumInputs() {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			typ := plan.Type{Id: int32(types.T_enum), Enumvalues: tc.domain, EnumCollation: tc.identity}
			partition := tree.HivePartColType{Id: typ.Id, Enumvalues: typ.Enumvalues, EnumCollation: typ.EnumCollation, NullAbility: true}
			col := &plan.ColDef{Name: "e", Typ: typ, Default: &plan.Default{NullAbility: true}}
			out := vector.NewVec(types.T_enum.ToType())
			defer out.Free(proc.Mp())
			validationErr := validatePartitionValue(tc.input, "e", partition, proc, "e=value/part.parquet")
			fillErr := fillConstantVector(out, tc.input, col, 2, proc, "e=value/part.parquet")
			if tc.invalid {
				require.Error(t, validationErr)
				require.Error(t, fillErr)
				return
			}
			require.NoError(t, validationErr)
			require.NoError(t, fillErr)
			require.True(t, out.IsConst())
			require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[types.Enum](out, 0))
		})
	}
}
