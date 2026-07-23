// Copyright 2025 Matrix Origin
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

package frontend

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDataBranchUserVisibleColumn(t *testing.T) {
	require.True(t, isDataBranchUserVisibleColumn(&plan.ColDef{Name: "tenant"}))
	require.False(t, isDataBranchUserVisibleColumn(&plan.ColDef{Name: catalog.FakePrimaryKeyColName, Hidden: true}))
	require.False(t, isDataBranchUserVisibleColumn(&plan.ColDef{Name: catalog.CPrimaryKeyColName, Hidden: true}))
	require.False(t, isDataBranchUserVisibleColumn(&plan.ColDef{Name: "__mo_cbkey_006tenant003seq", Hidden: true}))
	require.False(t, isDataBranchUserVisibleColumn(&plan.ColDef{Name: catalog.Row_ID, Hidden: true}))
}

func TestBranchQuotaUsageSQLExcludesRootAlterLineage(t *testing.T) {
	require.Equal(t,
		"select count(*) from mo_catalog.mo_branch_metadata where creator = 7 and table_deleted = false and level != 'alter'",
		branchQuotaUsageSQL(7),
	)
}

func TestDataBranchFakePKColIdxesUseOnlyVisibleColumns(t *testing.T) {
	tblDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: catalog.Row_ID, Hidden: true},
			{Name: "tenant"},
			{Name: "__mo_cbkey_006tenant003seq", Hidden: true},
			{Name: "payload"},
			{Name: catalog.FakePrimaryKeyColName, Hidden: true},
		},
	}
	require.Equal(t, []int{0, 2}, dataBranchFakePKColIdxes(tblDef))
}

func TestDataBranchSchemaEquivalentRequiresCompleteLogicalTypes(t *testing.T) {
	newTableDef := func() *plan.TableDef {
		return &plan.TableDef{Cols: []*plan.ColDef{
			{ColId: 1, Name: "id", Primary: true, NotNull: true, Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
			{ColId: 2, Name: "payload", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
			{ColId: 3, Name: "amount", Seqnum: 2, Typ: plan.Type{Id: int32(types.T_decimal128), Width: 12, Scale: 2}},
			{ColId: 4, Name: "color", Seqnum: 3, Typ: plan.Type{Id: int32(types.T_enum), Enumvalues: "red,blue"}},
		}}
	}

	t.Run("equal schemas", func(t *testing.T) {
		require.True(t, isSchemaEquivalent(newTableDef(), newTableDef()))
	})

	for _, tc := range []struct {
		name   string
		mutate func(*plan.TableDef)
	}{
		{
			name: "varchar width",
			mutate: func(def *plan.TableDef) {
				def.Cols[1].Typ.Width = 80
			},
		},
		{
			name: "decimal scale",
			mutate: func(def *plan.TableDef) {
				def.Cols[2].Typ.Scale = 4
			},
		},
		{
			name: "enum definition",
			mutate: func(def *plan.TableDef) {
				def.Cols[3].Typ.Enumvalues = "red,green"
			},
		},
		{
			name: "type nullability",
			mutate: func(def *plan.TableDef) {
				def.Cols[1].Typ.NotNullable = true
			},
		},
		{
			name: "auto increment",
			mutate: func(def *plan.TableDef) {
				def.Cols[1].Typ.AutoIncr = true
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left, right := newTableDef(), newTableDef()
			tc.mutate(right)
			require.False(t, isSchemaEquivalent(left, right))
		})
	}
}

func TestFormatValIntoString_StringEscaping(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val := "a'b\"c\\\n\t\r\x1a\x00"
	require.NoError(t, formatValIntoString(ses, val, types.New(types.T_varchar, 0, 0), &buf))
	require.Equal(t, `'a\'b"c\\\n\t\r\Z\0'`, buf.String())
}

func TestFormatValIntoString_ControlByteRoundTrip(t *testing.T) {
	value := make([]byte, 0x21)
	for controlByte := byte(0); controlByte < 0x20; controlByte++ {
		value = append(value, controlByte)
	}
	value = append(value, 0x7f)

	var literal bytes.Buffer
	require.NoError(t, formatValIntoString(&Session{}, value, types.T_varchar.ToType(), &literal))
	require.NotContains(t, literal.String(), `\x`)

	scanner := mysql.NewScanner(dialect.MYSQL, literal.String())
	defer mysql.PutScanner(scanner)
	token, got := scanner.Scan()
	require.Equal(t, mysql.STRING, token)
	require.Equal(t, value, []byte(got))
}

func TestFormatValIntoString_BinaryHexLiteral(t *testing.T) {
	val := []byte{'x', 0x00, '\\', 0x07, '\''}
	for _, oid := range []types.T{types.T_binary, types.T_varbinary, types.T_blob} {
		t.Run(oid.String(), func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, formatValIntoString(&Session{}, val, types.New(oid, 0, 0), &buf))
			require.Equal(t, `x'78005c0727'`, buf.String())

			buf.Reset()
			require.NoError(t, formatValIntoString(&Session{}, []byte{}, types.New(oid, 0, 0), &buf))
			require.Equal(t, `x''`, buf.String())

			buf.Reset()
			require.NoError(t, formatValIntoString(&Session{}, "x\x00", types.New(oid, 0, 0), &buf))
			require.Equal(t, `x'7800'`, buf.String())
		})
	}
}

func TestFormatValIntoString_Time(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val, err := types.ParseTime("12:34:56.123456", 6)
	require.NoError(t, err)

	require.NoError(t, formatValIntoString(ses, val, types.New(types.T_time, 0, 6), &buf))
	require.Equal(t, `'12:34:56.123456'`, buf.String())
}

func TestFormatValIntoString_JSONEscaping(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	val := `{"k":"` + string([]byte{0x01, '\n'}) + `"}`
	require.NoError(t, formatValIntoString(ses, val, types.New(types.T_json, 0, 0), &buf))
	require.Equal(t, `'{"k":"\\u0001\\u000a"}'`, buf.String())
}

func TestFormatValIntoString_JSONByteJson(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	bj, err := types.ParseStringToByteJson(`{"a":1}`)
	require.NoError(t, err)

	require.NoError(t, formatValIntoString(ses, bj, types.New(types.T_json, 0, 0), &buf))
	require.Equal(t, `'{"a": 1}'`, buf.String())
}

func TestFormatValIntoString_Nil(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	require.NoError(t, formatValIntoString(ses, nil, types.New(types.T_varchar, 0, 0), &buf))
	require.Equal(t, "NULL", buf.String())
}

func TestFormatValIntoString_DataBranchSpecialTypes(t *testing.T) {
	tests := []struct {
		name string
		val  any
		typ  types.Type
		want string
	}{
		{"bit", uint64(7), types.New(types.T_bit, 10, 0), "7"},
		{"uuid string", "12345678-1234-1234-1234-123456789012", types.T_uuid.ToType(), "'12345678-1234-1234-1234-123456789012'"},
		{"uuid value", types.Uuid{0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x90, 0x12}, types.T_uuid.ToType(), "'12345678-1234-1234-1234-123456789012'"},
		{"enum", types.Enum(2), types.T_enum.ToType(), "2"},
		{"datalink", []byte("file:///tmp/a.csv"), types.T_datalink.ToType(), "cast('file:///tmp/a.csv' as datalink)"},
		{"geometry", []byte("POINT(1 2)"), types.T_geometry.ToType(), "st_geomfromtext('POINT(1 2)')"},
		{"geometry32", []byte("POINT(1 2)"), types.T_geometry32.ToType(), "st_geomfromtext('POINT(1 2)')"},
		{"geometry SRID", []byte("POINT(1 2)"), types.New(types.T_geometry, 4327, 0), "st_geomfromtext('POINT(1 2)', 4326)"},
		{"geometry32 SRID", []byte("POINT(1 2)"), types.New(types.T_geometry32, 4327, 0), "st_geomfromtext('POINT(1 2)', 4326)"},
		{"geometry SRID zero", []byte("POINT(1 2)"), types.New(types.T_geometry, 1, 0), "st_geomfromtext('POINT(1 2)', 0)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, formatValIntoString(&Session{}, tt.val, tt.typ, &buf))
			require.Equal(t, tt.want, buf.String())
		})
	}
}

func TestFormatValIntoString_UnsupportedType(t *testing.T) {
	var buf bytes.Buffer
	ses := &Session{}

	err := formatValIntoString(ses, true, types.New(types.T_Rowid, 0, 0), &buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not support type")
}

func TestShouldUseLCAReaderFallback(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "stale read",
			err:  moerr.NewErrStaleReadNoCtx("10-0", "9-0"),
			want: true,
		},
		{
			name: "file not found",
			err:  moerr.NewFileNotFoundNoCtx("obj"),
			want: true,
		},
		{
			name: "unknown database",
			err:  moerr.NewBadDB(ctx, "test"),
			want: true,
		},
		{
			name: "unknown table",
			err:  moerr.NewNoSuchTable(ctx, "test", "t0"),
			want: true,
		},
		{
			name: "other error",
			err:  moerr.NewInvalidInput(ctx, "other"),
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldUseLCAReaderFallback(tc.err))
		})
	}
}

func TestExtractDataBranchSQLRowValueDecimal256(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	typ := types.New(types.T_decimal256, 39, 4)
	vec := vector.NewVec(typ)
	defer vec.Free(mp)

	val, err := types.ParseDecimal256("12345678901234567890123456789012345.6789", typ.Width, typ.Scale)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(vec, val, false, mp))

	row := make([]any, 1)
	require.NoError(t, extractDataBranchSQLRowValue(context.Background(), nil, vec, 0, row, 0))
	require.Equal(t, val, row[0])

	var buf bytes.Buffer
	require.NoError(t, formatValIntoString(nil, row[0], typ, &buf))
	require.Equal(t, "12345678901234567890123456789012345.6789", buf.String())
}

func TestAppendTupleValueToVector_VarlenaAndNull(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	varcharVec := vector.NewVec(types.New(types.T_varchar, 64, 0))
	require.NoError(t, appendTupleValueToVector(varcharVec, []byte("hello"), mp))
	require.Equal(t, 1, varcharVec.Length())
	require.Equal(t, "hello", string(varcharVec.GetBytesAt(0)))

	require.NoError(t, appendTupleValueToVector(varcharVec, nil, mp))
	require.Equal(t, 2, varcharVec.Length())
	require.True(t, varcharVec.GetNulls().Contains(1))

	datetimeVec := vector.NewVec(types.New(types.T_datetime, 0, 6))
	err := appendTupleValueToVector(datetimeVec, []byte("not-raw-fixed"), mp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected byte slice for fixed-width column")

	decimalTyp := types.New(types.T_decimal256, 39, 4)
	decimalVec := vector.NewVec(decimalTyp)
	decimalVal, err := types.ParseDecimal256("12345678901234567890123456789012344.1234", decimalTyp.Width, decimalTyp.Scale)
	require.NoError(t, err)
	require.NoError(t, appendTupleValueToVector(decimalVec, types.EncodeDecimal256(&decimalVal), mp))
	require.Equal(t, 1, decimalVec.Length())
	require.Equal(t, decimalVal, vector.GetFixedAtNoTypeCheck[types.Decimal256](decimalVec, 0))

	yearVec := vector.NewVec(types.T_year.ToType())
	yearVal := types.MoYear(2024)
	require.NoError(t, appendTupleValueToVector(yearVec, types.EncodeValue(yearVal, types.T_year), mp))
	require.Equal(t, 1, yearVec.Length())
	require.Equal(t, yearVal, vector.GetFixedAtNoTypeCheck[types.MoYear](yearVec, 0))
}

func TestAppendTupleValueToVector_BranchHashmapYear(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	src := vector.NewVec(types.T_year.ToType())
	defer src.Free(mp)

	yearVal := types.MoYear(2024)
	require.NoError(t, vector.AppendFixed(src, yearVal, false, mp))

	bh, err := databranchutils.NewBranchHashmap()
	require.NoError(t, err)
	defer func() { require.NoError(t, bh.Close()) }()

	require.NoError(t, bh.PutByVectors([]*vector.Vector{src}, []int{0}))
	results, err := bh.GetByVectors([]*vector.Vector{src})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.True(t, results[0].Exists)
	require.Len(t, results[0].Rows, 1)

	tuple, _, err := bh.DecodeRow(results[0].Rows[0])
	require.NoError(t, err)
	require.Equal(t, yearVal, tuple[0])

	dst := vector.NewVec(types.T_year.ToType())
	defer dst.Free(mp)

	require.NoError(t, appendTupleValueToVector(dst, tuple[0], mp))
	require.Equal(t, 1, dst.Length())
	require.Equal(t, yearVal, vector.GetFixedAtNoTypeCheck[types.MoYear](dst, 0))
}

func TestCompareSingleValInVector_AllTypes(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	cases := []struct {
		name  string
		build func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int)
	}{
		{
			name: "json",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_json.ToType()
				leftVec := vector.NewVec(typ)
				rightVec := vector.NewVec(typ)
				leftVal, err := types.ParseStringToByteJson(`{"k":1}`)
				require.NoError(t, err)
				rightVal, err := types.ParseStringToByteJson(`{"k":2}`)
				require.NoError(t, err)
				leftBytes, err := leftVal.Marshal()
				require.NoError(t, err)
				rightBytes, err := rightVal.Marshal()
				require.NoError(t, err)
				require.NoError(t, vector.AppendBytes(leftVec, leftBytes, false, mp))
				require.NoError(t, vector.AppendBytes(rightVec, rightBytes, false, mp))
				expected := types.CompareValue(types.DecodeJson(leftBytes), types.DecodeJson(rightBytes))
				return leftVec, rightVec, expected
			},
		},
		{
			name: "bool",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_bool.ToType()
				leftVal, rightVal := false, true
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "bit",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_bit.ToType()
				leftVal, rightVal := uint64(1), uint64(3)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "int8",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_int8.ToType()
				leftVal, rightVal := int8(1), int8(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uint8",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uint8.ToType()
				leftVal, rightVal := uint8(1), uint8(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "int16",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_int16.ToType()
				leftVal, rightVal := int16(1), int16(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uint16",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uint16.ToType()
				leftVal, rightVal := uint16(1), uint16(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "int32",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_int32.ToType()
				leftVal, rightVal := int32(1), int32(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uint32",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uint32.ToType()
				leftVal, rightVal := uint32(1), uint32(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "int64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_int64.ToType()
				leftVal, rightVal := int64(1), int64(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uint64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uint64.ToType()
				leftVal, rightVal := uint64(1), uint64(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "float32",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_float32.ToType()
				leftVal, rightVal := float32(1.1), float32(2.2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "float64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_float64.ToType()
				leftVal, rightVal := float64(1.1), float64(2.2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "char",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_char, 8, 0)
				leftVal, rightVal := []byte("a"), []byte("b")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "varchar",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_varchar, 16, 0)
				leftVal, rightVal := []byte("alpha"), []byte("beta")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "blob",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_blob.ToType()
				leftVal, rightVal := []byte("blob-a"), []byte("blob-b")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "text",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_text.ToType()
				leftVal, rightVal := []byte("text-a"), []byte("text-b")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "binary",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_binary, 4, 0)
				leftVal, rightVal := []byte{0x00, 0x01}, []byte{0x00, 0x02}
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "varbinary",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_varbinary, 8, 0)
				leftVal, rightVal := []byte{0x01, 0x02}, []byte{0x02, 0x03}
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "datalink",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_datalink.ToType()
				leftVal, rightVal := []byte("link-a"), []byte("link-b")
				leftVec := buildBytesVector(t, mp, typ, leftVal)
				rightVec := buildBytesVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "array_float32",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_array_float32, 2, 0)
				leftVal := []float32{1, 2}
				rightVal := []float32{1, 3}
				leftVec := buildArrayVector(t, mp, typ, [][]float32{leftVal})
				rightVec := buildArrayVector(t, mp, typ, [][]float32{rightVal})
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "array_float64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_array_float64, 2, 0)
				leftVal := []float64{1, 2}
				rightVal := []float64{2, 3}
				leftVec := buildArrayVector(t, mp, typ, [][]float64{leftVal})
				rightVec := buildArrayVector(t, mp, typ, [][]float64{rightVal})
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "date",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_date.ToType()
				leftVal, rightVal := types.Date(1), types.Date(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "datetime",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_datetime.ToType()
				leftVal, rightVal := types.Datetime(1), types.Datetime(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "time",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_time.ToType()
				leftVal, rightVal := types.Time(1), types.Time(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "timestamp",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_timestamp.ToType()
				leftVal, rightVal := types.Timestamp(1), types.Timestamp(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "decimal64",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_decimal64, 18, 2)
				leftVal, err := types.Decimal64FromFloat64(12.34, 18, 2)
				require.NoError(t, err)
				rightVal, err := types.Decimal64FromFloat64(23.45, 18, 2)
				require.NoError(t, err)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "decimal128",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_decimal128, 38, 4)
				leftVal, err := types.Decimal128FromFloat64(12.34, 38, 4)
				require.NoError(t, err)
				rightVal, err := types.Decimal128FromFloat64(23.45, 38, 4)
				require.NoError(t, err)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "decimal256",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.New(types.T_decimal256, 39, 4)
				leftVal, err := types.ParseDecimal256("12345678901234567890123456789012344.1234", typ.Width, typ.Scale)
				require.NoError(t, err)
				rightVal, err := types.ParseDecimal256("12345678901234567890123456789012345.1234", typ.Width, typ.Scale)
				require.NoError(t, err)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "uuid",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_uuid.ToType()
				var leftVal, rightVal types.Uuid
				copy(leftVal[:], []byte("aaaaaaaaaaaaaaaa"))
				copy(rightVal[:], []byte("bbbbbbbbbbbbbbbb"))
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "rowid",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_Rowid.ToType()
				var blk types.Blockid
				copy(blk[:], []byte("block-identifier"))
				leftVal := types.NewRowid(&blk, 1)
				rightVal := types.NewRowid(&blk, 2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "blockid",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_Blockid.ToType()
				var leftVal, rightVal types.Blockid
				copy(leftVal[:], []byte("block-00000000001"))
				copy(rightVal[:], []byte("block-00000000002"))
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "ts",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_TS.ToType()
				leftVal := types.BuildTS(1, 0)
				rightVal := types.BuildTS(2, 0)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
		{
			name: "enum",
			build: func(t *testing.T, mp *mpool.MPool) (*vector.Vector, *vector.Vector, int) {
				typ := types.T_enum.ToType()
				leftVal, rightVal := types.Enum(1), types.Enum(2)
				leftVec := buildFixedVector(t, mp, typ, leftVal)
				rightVec := buildFixedVector(t, mp, typ, rightVal)
				return leftVec, rightVec, types.CompareValue(leftVal, rightVal)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			leftVec, rightVec, expected := tc.build(t, mp)
			defer leftVec.Free(mp)
			defer rightVec.Free(mp)

			cmp, err := compareSingleValInVector(ctx, ses, 0, 0, leftVec, rightVec)
			require.NoError(t, err)
			require.Equal(t, expected, cmp)
		})
	}
}

func TestCompareSingleValInVector_ConstVectors(t *testing.T) {
	ctx := context.Background()
	ses := &Session{}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	typ := types.T_int32.ToType()
	leftVec := buildFixedVector(t, mp, typ, int32(5))
	rightVec, err := vector.NewConstFixed[int32](typ, int32(7), 3, mp)
	require.NoError(t, err)
	defer leftVec.Free(mp)
	defer rightVec.Free(mp)

	cmp, err := compareSingleValInVector(ctx, ses, 0, 2, leftVec, rightVec)
	require.NoError(t, err)
	require.Equal(t, types.CompareValue(int32(5), int32(7)), cmp)
}

func TestCompareTupleValueWithVectorDecimal256(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	typ := types.New(types.T_decimal256, 39, 4)
	leftVal, err := types.ParseDecimal256("12345678901234567890123456789012344.1234", typ.Width, typ.Scale)
	require.NoError(t, err)
	rightVal, err := types.ParseDecimal256("12345678901234567890123456789012345.1234", typ.Width, typ.Scale)
	require.NoError(t, err)

	vec := buildFixedVector(t, mp, typ, rightVal)
	defer vec.Free(mp)

	cmp, err := compareTupleValueWithVector(leftVal, vec, 0)
	require.NoError(t, err)
	require.Equal(t, types.CompareValue(leftVal, rightVal), cmp)

	cmp, err = compareTupleValueWithVector(rightVal, vec, 0)
	require.NoError(t, err)
	require.Equal(t, 0, cmp)
}

func buildFixedVector[T any](t *testing.T, mp *mpool.MPool, typ types.Type, vals ...T) *vector.Vector {
	vec := vector.NewVec(typ)
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	return vec
}

func buildBytesVector(t *testing.T, mp *mpool.MPool, typ types.Type, vals ...[]byte) *vector.Vector {
	vec := vector.NewVec(typ)
	for _, v := range vals {
		require.NoError(t, vector.AppendBytes(vec, v, false, mp))
	}
	return vec
}

func buildArrayVector[T types.RealNumbers](t *testing.T, mp *mpool.MPool, typ types.Type, vals [][]T) *vector.Vector {
	vec := vector.NewVec(typ)
	require.NoError(t, vector.AppendArrayList(vec, vals, nil, mp))
	return vec
}

func newValidateSession(t *testing.T) *Session {
	t.Helper()

	proc := testutil.NewProcess(t)
	service := "validate-output-dir"

	InitServerLevelVars(service)
	setPu(service, &config.ParameterUnit{
		SV:          &config.FrontendParameters{},
		FileService: proc.Base.FileService,
	})

	return &Session{
		feSessionImpl: feSessionImpl{service: service},
		proc:          proc,
	}
}

func TestValidateOutputDirPath(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)

	t.Run("empty path", func(t *testing.T) {
		require.NoError(t, validateOutputDirPath(ctx, ses, ""))
	})

	t.Run("local directory exists", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, validateOutputDirPath(ctx, ses, dir))
	})

	t.Run("local directory missing", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "missing")
		err := validateOutputDirPath(ctx, ses, dir)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "does not exist")
	})

	t.Run("local path is file", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "file")
		require.NoError(t, err)
		require.NoError(t, f.Close())

		err = validateOutputDirPath(ctx, ses, f.Name())
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "not a directory")
	})

	t.Run("stage path through cache", func(t *testing.T) {
		stageDir := t.TempDir()
		stageURL, err := url.Parse("file://" + stageDir)
		require.NoError(t, err)
		stageName := "stage_local"

		cache := ses.proc.GetStageCache()
		cache.Set(stageName, stage.StageDef{
			Name: stageName,
			Url:  stageURL,
		})

		err = validateOutputDirPath(ctx, ses, fmt.Sprintf("stage://%s", stageName))
		require.NoError(t, err)
	})

	t.Run("shared fileservice directory exists", func(t *testing.T) {
		fs := ses.proc.Base.FileService
		dirPath := fmt.Sprintf("%s:/exists", defines.SharedFileServiceName)
		filePath := fmt.Sprintf("%s/file.txt", dirPath)

		write := fileservice.IOVector{
			FilePath: filePath,
			Entries: []fileservice.IOEntry{{
				Offset: 0,
				Size:   int64(len("x")),
				Data:   []byte("x"),
			}},
		}
		require.NoError(t, fs.Write(ctx, write))

		require.NoError(t, validateOutputDirPath(ctx, ses, dirPath))
	})

	t.Run("shared fileservice missing directory", func(t *testing.T) {
		dirPath := fmt.Sprintf("%s:/not-exist", defines.SharedFileServiceName)

		err := validateOutputDirPath(ctx, ses, dirPath)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	})

	t.Run("shared fileservice path is file", func(t *testing.T) {
		fs := ses.proc.Base.FileService
		filePath := fmt.Sprintf("%s:/just-file", defines.SharedFileServiceName)

		write := fileservice.IOVector{
			FilePath: filePath,
			Entries: []fileservice.IOEntry{{
				Offset: 0,
				Size:   int64(len("abc")),
				Data:   []byte("abc"),
			}},
		}
		require.NoError(t, fs.Write(ctx, write))

		err := validateOutputDirPath(ctx, ses, filePath)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "not a directory")
	})

	t.Run("invalid path format", func(t *testing.T) {
		err := validateOutputDirPath(ctx, ses, string([]byte{0x00, ':'}))
		require.Error(t, err)
	})

	t.Run("service argument error", func(t *testing.T) {
		err := validateOutputDirPath(ctx, ses, "s3,bad:/bucket")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	})

	t.Run("service root unreachable", func(t *testing.T) {
		err := validateOutputDirPath(ctx, ses, "s3-opts,endpoint=http://127.0.0.1:65535,region=us-east-1,bucket=b,key=k,secret=s,prefix=tmp:")
		require.Error(t, err)
	})
}

func TestCheckSchemaCompatibility_Identical(t *testing.T) {
	tarDef := &plan.TableDef{
		Name: "target",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	baseDef := &plan.TableDef{
		Name: "base",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}

	commonIdxes, commonVisibleIdxes, tarOnlyIdxes, err := checkSchemaCompatibility(tarDef, baseDef)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1}, commonIdxes)
	require.Equal(t, []int{0, 1}, commonVisibleIdxes)
	require.Empty(t, tarOnlyIdxes)
}

func TestCheckSchemaCompatibility_ExtraColumnOnTarget(t *testing.T) {
	tarDef := &plan.TableDef{
		Name: "target",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "c", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	baseDef := &plan.TableDef{
		Name: "base",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}

	commonIdxes, commonVisibleIdxes, tarOnlyIdxes, err := checkSchemaCompatibility(tarDef, baseDef)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1}, commonIdxes)
	require.Equal(t, []int{0, 1}, commonVisibleIdxes)
	require.Equal(t, []int{2}, tarOnlyIdxes)
}

func TestCheckSchemaCompatibility_ReturnsDataBatchIndexes(t *testing.T) {
	tarDef := &plan.TableDef{
		Name: "target",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: catalog.Row_ID, Hidden: true},
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "c", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	baseDef := &plan.TableDef{
		Name: "base",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: catalog.Row_ID, Hidden: true},
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}

	commonIdxes, commonVisibleIdxes, tarOnlyIdxes, err := checkSchemaCompatibility(tarDef, baseDef)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1}, commonIdxes)
	require.Equal(t, []int{0, 1}, commonVisibleIdxes)
	require.Equal(t, []int{2}, tarOnlyIdxes)
}

func TestCheckSchemaCompatibility_SeparatesPhysicalAndVisibleCommonIndexes(t *testing.T) {
	tarDef := &plan.TableDef{
		Name: "target",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: catalog.Row_ID, Hidden: true},
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "__mo_cbkey_001a", Typ: plan.Type{Id: int32(types.T_varchar)}, Hidden: true},
			{Name: "c", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	baseDef := &plan.TableDef{
		Name: "base",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: catalog.Row_ID, Hidden: true},
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "__mo_cbkey_001a", Typ: plan.Type{Id: int32(types.T_varchar)}, Hidden: true},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}

	commonIdxes, commonVisibleIdxes, tarOnlyIdxes, err := checkSchemaCompatibility(tarDef, baseDef)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1, 3}, commonIdxes)
	require.Equal(t, []int{0, 3}, commonVisibleIdxes)
	require.Equal(t, []int{2}, tarOnlyIdxes)
}

func TestCheckSchemaCompatibility_HiddenColumnsAreNotOutputColumns(t *testing.T) {
	tarDef := &plan.TableDef{Pkey: &plan.PrimaryKeyDef{PkeyColName: "a"}, Cols: []*plan.ColDef{
		{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "hidden", Hidden: true, Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
	}}
	baseDef := &plan.TableDef{Pkey: &plan.PrimaryKeyDef{PkeyColName: "a"}, Cols: []*plan.ColDef{
		{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "hidden", Hidden: true, Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
	}}

	commonIdxes, commonVisibleIdxes, tarOnlyIdxes, err := checkSchemaCompatibility(tarDef, baseDef)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1, 2}, commonIdxes)
	require.Equal(t, []int{0, 2}, commonVisibleIdxes)
	require.Empty(t, tarOnlyIdxes)
}

func TestCheckSchemaCompatibility_RejectsBaseOnlyVisibleColumn(t *testing.T) {
	tarDef := &plan.TableDef{Pkey: &plan.PrimaryKeyDef{PkeyColName: "a"}, Cols: []*plan.ColDef{{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}}}}
	baseDef := &plan.TableDef{Pkey: &plan.PrimaryKeyDef{PkeyColName: "a"}, Cols: []*plan.ColDef{
		{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "removed", Typ: plan.Type{Id: int32(types.T_int64)}},
	}}

	_, _, _, err := checkSchemaCompatibility(tarDef, baseDef)
	require.ErrorContains(t, err, "base column 'removed' is not present in target schema")
}

func TestCheckSchemaCompatibility_PKChanged(t *testing.T) {
	// base has PK on column "a" which is also in target, so it passes.
	// We need a case where the BASE's PK column does NOT exist in target.
	// Target has different columns entirely, so base PK "a" is missing from common.
	tarDef := &plan.TableDef{
		Name: "target",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"x"},
			PkeyColName: "x",
		},
		Cols: []*plan.ColDef{
			{Name: "x", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "y", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	baseDef := &plan.TableDef{
		Name: "base",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}

	_, _, _, err := checkSchemaCompatibility(tarDef, baseDef)
	require.Error(t, err)
	require.Contains(t, err.Error(), "primary key column")
}

func TestCheckSchemaCompatibility_RejectsChangedPrimaryKeyWithCommonColumns(t *testing.T) {
	tarDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"b"}, PkeyColName: "b"},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	baseDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"a"}, PkeyColName: "a"},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}

	_, _, _, err := checkSchemaCompatibility(tarDef, baseDef)
	require.ErrorContains(t, err, "primary key columns")
}

func TestCheckSchemaCompatibility_TypeMismatch(t *testing.T) {
	tarDef := &plan.TableDef{
		Name: "target",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_varchar)}}, // varchar in target
		},
	}
	baseDef := &plan.TableDef{
		Name: "base",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}}, // int in base
		},
	}

	_, _, _, err := checkSchemaCompatibility(tarDef, baseDef)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has different types")
}

func TestCheckSchemaCompatibility_AllowsCopyAlterIdentityReassignment(t *testing.T) {
	baseDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"a"}, PkeyColName: "a"},
		Cols: []*plan.ColDef{
			{Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	targetDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"a"}, PkeyColName: "a"},
		Cols: []*plan.ColDef{
			{Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Seqnum: 2, Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}

	_, _, _, err := checkSchemaCompatibility(targetDef, baseDef)
	require.NoError(t, err)

	// Physical reordering preserves Seqnum and remains compatible.
	targetDef.Cols[1].Seqnum = 1
	targetDef.Cols[0], targetDef.Cols[1] = targetDef.Cols[1], targetDef.Cols[0]
	_, _, _, err = checkSchemaCompatibility(targetDef, baseDef)
	require.NoError(t, err)
}

func TestValidateDataBranchColumnLineage(t *testing.T) {
	tableDef := func(cols ...*plan.ColDef) *plan.TableDef {
		return &plan.TableDef{Cols: cols}
	}
	col := func(name string, id uint64, seq uint32) *plan.ColDef {
		return &plan.ColDef{
			Name: name, ColId: id, Seqnum: seq,
			Typ: plan.Type{Id: int32(types.T_int64)},
		}
	}

	t.Run("copy alter preserves same-name columns", func(t *testing.T) {
		tarDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0), col("b", 2, 1)),
			tableDef(col("a", 10, 1), col("c", 11, 0), col("b", 12, 2)),
		}
		baseDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0), col("b", 2, 1)),
			tableDef(col("a", 20, 0), col("b", 21, 1)),
		}
		require.NoError(t, validateDataBranchColumnLineage(
			tarDefs, []bool{false, true}, baseDefs, []bool{false, false},
		))
	})

	t.Run("drop and add same name is discontinuous", func(t *testing.T) {
		tarDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0), col("b", 2, 1)),
			tableDef(col("a", 10, 0)),
			tableDef(col("a", 20, 0), col("b", 21, 1)),
		}
		baseDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0), col("b", 2, 1)),
			tableDef(col("a", 30, 0), col("b", 31, 1)),
		}
		err := validateDataBranchColumnLineage(
			tarDefs, []bool{false, true, true}, baseDefs, []bool{false, false},
		)
		require.ErrorContains(t, err, "column 'b' has different identity")
	})

	t.Run("independent additions are compatible", func(t *testing.T) {
		tarDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0)),
			tableDef(col("a", 10, 0), col("c", 11, 1)),
		}
		baseDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0)),
			tableDef(col("a", 20, 0), col("c", 21, 1)),
		}
		require.NoError(t, validateDataBranchColumnLineage(
			tarDefs, []bool{false, true}, baseDefs, []bool{false, true},
		))
	})

	t.Run("added column cannot be dropped and recreated", func(t *testing.T) {
		tarDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0)),
			tableDef(col("a", 10, 0), col("c", 11, 1)),
			tableDef(col("a", 20, 0)),
			tableDef(col("a", 30, 0), col("c", 31, 1)),
		}
		baseDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0)),
			tableDef(col("a", 40, 0), col("c", 41, 1)),
		}
		err := validateDataBranchColumnLineage(
			tarDefs, []bool{false, true, true, true}, baseDefs, []bool{false, true},
		)
		require.ErrorContains(t, err, "column 'c' has different identity")
	})

	t.Run("rename across a clone edge preserves identity", func(t *testing.T) {
		tarDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0), col("b", 2, 1)),
			tableDef(col("a", 1, 0), &plan.ColDef{
				Name: "bb", OriginName: "b", ColId: 2, Seqnum: 1,
				Typ: plan.Type{Id: int32(types.T_int64)},
			}),
		}
		baseDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0), col("b", 2, 1)),
			tableDef(col("a", 1, 0), col("b", 2, 1)),
		}
		require.NoError(t, validateDataBranchColumnLineage(
			tarDefs, []bool{false, false}, baseDefs, []bool{false, false},
		))
	})

	t.Run("rename without origin name preserves stable identity", func(t *testing.T) {
		tarDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0), col("b", 2, 1)),
			tableDef(col("a", 1, 0), col("bb", 2, 1)),
		}
		baseDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0), col("b", 2, 1)),
			tableDef(col("a", 1, 0), col("b", 2, 1)),
		}
		require.NoError(t, validateDataBranchColumnLineage(
			tarDefs, []bool{false, false}, baseDefs, []bool{false, false},
		))
	})

	t.Run("replacement with colliding endpoint identity remains discontinuous", func(t *testing.T) {
		tarDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0), col("b", 2, 1)),
			tableDef(col("a", 10, 0)),
			tableDef(col("a", 20, 0), col("c", 2, 1)),
		}
		baseDefs := []*plan.TableDef{
			tableDef(col("a", 1, 0), col("b", 2, 1)),
			tableDef(col("a", 1, 0), col("b", 2, 1)),
		}
		err := validateDataBranchColumnLineage(
			tarDefs, []bool{false, true, true}, baseDefs, []bool{false, false},
		)
		require.ErrorContains(t, err, "column 'c' has different identity")
	})
}

func TestCheckSchemaCompatibility_AllowsStableIdentityRename(t *testing.T) {
	baseDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"a"}, PkeyColName: "a"},
		Cols: []*plan.ColDef{
			{Name: "a", ColId: 1, Seqnum: 0, Primary: true, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", ColId: 2, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	targetDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"a"}, PkeyColName: "a"},
		Cols: []*plan.ColDef{
			{Name: "a", ColId: 1, Seqnum: 0, Primary: true, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "bb", ColId: 2, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}

	common, visible, targetOnly, err := checkSchemaCompatibility(targetDef, baseDef)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1}, common)
	require.Equal(t, []int{0, 1}, visible)
	require.Empty(t, targetOnly)
}

func TestCheckSchemaCompatibility_RejectsDifferentTypeAttributes(t *testing.T) {
	tarDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"a"}, PkeyColName: "a"},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "amount", Typ: plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 2}},
		},
	}
	baseDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"a"}, PkeyColName: "a"},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "amount", Typ: plan.Type{Id: int32(types.T_decimal64), Width: 12, Scale: 0}},
		},
	}

	_, _, _, err := checkSchemaCompatibility(tarDef, baseDef)
	require.ErrorContains(t, err, "different type attributes")
}

func TestCheckSchemaCompatibility_RejectsDifferentColumnNullability(t *testing.T) {
	tarDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"a"}, PkeyColName: "a"},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}, NotNull: false},
		},
	}
	baseDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{Names: []string{"a"}, PkeyColName: "a"},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}, NotNull: true},
		},
	}

	_, _, _, err := checkSchemaCompatibility(tarDef, baseDef)
	require.ErrorContains(t, err, "different nullability")
}

func TestCheckSchemaCompatibility_BaseOnlyVisibleColumnRejected(t *testing.T) {
	tarDef := &plan.TableDef{
		Name: "target",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	baseDef := &plan.TableDef{
		Name: "base",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a"},
			PkeyColName: "a",
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}

	_, _, _, err := checkSchemaCompatibility(tarDef, baseDef)
	require.Error(t, err)
	require.Contains(t, err.Error(), "base column 'b' is not present in target schema")
}

func TestCheckSchemaCompatibility_CompositePK(t *testing.T) {
	tarDef := &plan.TableDef{
		Name: "target",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a", "b"},
			PkeyColName: "__cpkey__",
			CompPkeyCol: &plan.ColDef{Name: "__cpkey__"},
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "c", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	baseDef := &plan.TableDef{
		Name: "base",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"a", "b"},
			PkeyColName: "__cpkey__",
			CompPkeyCol: &plan.ColDef{Name: "__cpkey__"},
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}

	commonIdxes, commonVisibleIdxes, tarOnlyIdxes, err := checkSchemaCompatibility(tarDef, baseDef)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1}, commonIdxes)
	require.Equal(t, []int{0, 1}, commonVisibleIdxes)
	require.Equal(t, []int{2}, tarOnlyIdxes)
}

func TestCheckSchemaCompatibility_FakePKRejectsTargetOnlyColumns(t *testing.T) {
	tarDef := &plan.TableDef{
		Name: "target",
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.FakePrimaryKeyColName,
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "c", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	baseDef := &plan.TableDef{
		Name: "base",
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: catalog.FakePrimaryKeyColName,
		},
		Cols: []*plan.ColDef{
			{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}

	_, _, _, err := checkSchemaCompatibility(tarDef, baseDef)
	require.Error(t, err)
	require.Contains(t, err.Error(), "require an explicit primary key")
}

func TestDataBranchCollectRelationSnapshot(t *testing.T) {
	endpointSP := types.BuildTS(300, 7)
	transitionTS := types.BuildTS(200, 3)

	require.Equal(t, endpointSP, dataBranchCollectRelationSnapshot(endpointSP, transitionTS, true))
	require.Equal(t, transitionTS, dataBranchCollectRelationSnapshot(endpointSP, transitionTS, false))
}

func TestBranchMetaInfoLCASnapshotIgnoresAlterLineageEdges(t *testing.T) {
	alterTS := types.BuildTS(200, 0)
	forkTS := types.BuildTS(100, 0)
	tarSP := types.BuildTS(300, 0)
	baseSP := types.BuildTS(250, 0)
	info := branchMetaInfo{
		pathFromLCAToTar:             []uint64{1, 2},
		pathFromLCAToTarTS:           []types.TS{{}, alterTS},
		pathFromLCAToTarLineageOnly:  []bool{false, true},
		pathFromLCAToBase:            []uint64{1, 3},
		pathFromLCAToBaseTS:          []types.TS{{}, forkTS},
		pathFromLCAToBaseLineageOnly: []bool{false, false},
	}

	require.Equal(t, forkTS, info.tarLCASnapshot(baseSP))
	require.Equal(t, forkTS, info.baseLCASnapshot(tarSP))
}
