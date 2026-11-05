// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

// newParquetUTF8ResultSet builds the result-set metadata for a single column.
func newParquetUTF8ResultSet(name string, mysqlType defines.MysqlType, flag uint16) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	col := &MysqlColumn{}
	col.SetName(name)
	col.SetColumnType(mysqlType)
	col.SetFlag(flag)
	mrs.AddColumn(col)
	return mrs
}

func newBytesBatch(t *testing.T, mp *mpool.MPool, typ types.T, width int32, values [][]byte, nulls []bool) *batch.Batch {
	t.Helper()
	bat := batch.NewOffHeapWithSize(1)
	bat.Vecs[0] = vector.NewOffHeapVecWithType(types.New(typ, width, 0))
	for i, v := range values {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], v, nulls[i], mp))
	}
	bat.SetRowCount(len(values))
	return bat
}

// TestParquetExportRejectsInvalidUTF8: a VARCHAR-family value that is not
// valid UTF-8 cannot be represented in a Parquet String() node. The export
// must fail with the same error shape the loader reports (ERROR 20301,
// "invalid UTF-8 value for <TYPE>") instead of writing an unloadable file.
func TestParquetExportRejectsInvalidUTF8(t *testing.T) {
	ctx := context.Background()

	t.Run("varchar invalid utf-8 rejected with loader parity message", func(t *testing.T) {
		mrs := newParquetUTF8ResultSet("s", defines.MYSQL_TYPE_VARCHAR, 0)
		pw, err := NewParquetWriter(ctx, mrs)
		require.NoError(t, err)
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newBytesBatch(t, mp, types.T_varchar, 30,
			[][]byte{[]byte("plain"), {0xFF}}, []bool{false, false})
		defer bat.Clean(mp)
		err = pw.WriteBatch(bat, mp, nil)
		require.Error(t, err)
		// Same wording the loader reports for such a value (ERROR 20301).
		require.EqualError(t, err, "invalid input: invalid UTF-8 value for VARCHAR")
	})

	t.Run("rejection happens before any bytes are emitted", func(t *testing.T) {
		mrs := newParquetUTF8ResultSet("s", defines.MYSQL_TYPE_VARCHAR, 0)
		pw, err := NewParquetWriter(ctx, mrs)
		require.NoError(t, err)
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newBytesBatch(t, mp, types.T_varchar, 30,
			[][]byte{[]byte("good"), {0x61, 0xFF, 0x62}}, []bool{false, false})
		defer bat.Clean(mp)
		sizeBefore := pw.Size()
		err = pw.WriteBatch(bat, mp, nil)
		require.Error(t, err)
		// The failing row-group never reached the underlying writer: nothing
		// flushed, and no rows were committed to the current file.
		require.Zero(t, pw.bufferedRows)
		require.Equal(t, sizeBefore, pw.Size())
	})

	t.Run("text and char invalid utf-8 rejected", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			mysqlType defines.MysqlType
			engineT   types.T
			expectErr string
		}{
			{"text", defines.MYSQL_TYPE_TEXT, types.T_text, "invalid UTF-8 value for TEXT"},
			{"char", defines.MYSQL_TYPE_STRING, types.T_char, "invalid UTF-8 value for CHAR"},
			{"var_string varchar form", defines.MYSQL_TYPE_VAR_STRING, types.T_varchar, "invalid UTF-8 value for VARCHAR"},
			{"text-family blob", defines.MYSQL_TYPE_BLOB, types.T_blob, "invalid UTF-8 value for BLOB"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				mrs := newParquetUTF8ResultSet("s", tc.mysqlType, 0)
				pw, err := NewParquetWriter(ctx, mrs)
				require.NoError(t, err)
				mp := mpool.MustNewZero()
				defer func() { require.Zero(t, mp.CurrNB()) }()
				bat := newBytesBatch(t, mp, tc.engineT, 30,
					[][]byte{{0xC3, 0x28}}, []bool{false}) // invalid UTF-8 (orphaned lead byte)
				defer bat.Clean(mp)
				err = pw.WriteBatch(bat, mp, nil)
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErr)
			})
		}
	})

	t.Run("valid utf-8 varchar exports", func(t *testing.T) {
		mrs := newParquetUTF8ResultSet("s", defines.MYSQL_TYPE_VARCHAR, 0)
		pw, err := NewParquetWriter(ctx, mrs)
		require.NoError(t, err)
		mp := mpool.MustNewZero()
		defer func() { require.Zero(t, mp.CurrNB()) }()
		bat := newBytesBatch(t, mp, types.T_varchar, 30,
			[][]byte{[]byte("héllo"), []byte("世界"), nil, []byte("plain")},
			[]bool{false, false, true, false})
		defer bat.Clean(mp)
		require.NoError(t, pw.WriteBatch(bat, mp, nil))
		data, err := pw.Close()
		require.NoError(t, err)
		require.Equal(t, "PAR1", string(data[:4]))
		file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, err)
		require.Equal(t, int64(4), file.NumRows())
		require.NotNil(t, file.Metadata().Schema[1].LogicalType.UTF8)
	})

	t.Run("binary-flagged columns bypass validation and stay lossless", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			mysqlType defines.MysqlType
			engineT   types.T
		}{
			{"varbinary", defines.MYSQL_TYPE_VAR_STRING, types.T_varbinary},
			{"varchar binary metadata", defines.MYSQL_TYPE_VARCHAR, types.T_varbinary},
			{"binary", defines.MYSQL_TYPE_STRING, types.T_binary},
			{"text binary metadata", defines.MYSQL_TYPE_TEXT, types.T_blob},
			{"tiny blob", defines.MYSQL_TYPE_TINY_BLOB, types.T_blob},
			{"blob", defines.MYSQL_TYPE_BLOB, types.T_blob},
			{"medium blob", defines.MYSQL_TYPE_MEDIUM_BLOB, types.T_blob},
			{"long blob", defines.MYSQL_TYPE_LONG_BLOB, types.T_blob},
		} {
			t.Run(tc.name, func(t *testing.T) {
				mrs := newParquetUTF8ResultSet("s", tc.mysqlType, uint16(defines.BINARY_FLAG))
				pw, err := NewParquetWriter(ctx, mrs)
				require.NoError(t, err)
				require.Len(t, pw.utf8Cols, 1)
				require.False(t, pw.utf8Cols[0])
				mp := mpool.MustNewZero()
				defer func() { require.Zero(t, mp.CurrNB()) }()
				raw := [][]byte{{0xFF}, {0x61, 0xFF, 0x62}, []byte("plain")}
				bat := newBytesBatch(t, mp, tc.engineT, 30, raw, []bool{false, false, false})
				defer bat.Clean(mp)
				require.NoError(t, pw.WriteBatch(bat, mp, nil))
				data, err := pw.Close()
				require.NoError(t, err)
				require.Equal(t, "PAR1", string(data[:4]))
				// Inspect the serialized footer independently of MO's loader:
				// raw binary is BYTE_ARRAY with neither UTF8 nor STRING tags.
				file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
				require.NoError(t, err)
				require.Len(t, file.Metadata().Schema, 2)
				leaf := file.Metadata().Schema[1]
				require.Nil(t, leaf.LogicalType)
				require.Nil(t, leaf.ConvertedType)
				require.Equal(t, parquet.ByteArray, file.Schema().Fields()[0].Type().Kind())
				// Use the library reader, not vectorValueToParquet or MO LOAD.
				decoded, err := parquet.Read[struct {
					S []byte `parquet:"s"`
				}](bytes.NewReader(data), int64(len(data)))
				require.NoError(t, err)
				require.Len(t, decoded, len(raw))
				for i := range raw {
					require.Equal(t, raw[i], decoded[i].S)
				}
			})
		}
	})
}

// TestParquetUTF8ColumnPredicate pins the column classification itself.
func TestParquetUTF8ColumnPredicate(t *testing.T) {
	// Text-family without BINARY flag must be validated.
	for _, typ := range []defines.MysqlType{
		defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING,
		defines.MYSQL_TYPE_STRING, defines.MYSQL_TYPE_TEXT,
		defines.MYSQL_TYPE_TINY_BLOB, defines.MYSQL_TYPE_BLOB,
		defines.MYSQL_TYPE_MEDIUM_BLOB, defines.MYSQL_TYPE_LONG_BLOB,
	} {
		require.True(t, valueRequiresValidUTF8(typ, 0), "type %d", typ)
		// ...but never when the column is declared binary.
		require.False(t, valueRequiresValidUTF8(typ, uint16(defines.BINARY_FLAG)), "type %d with binary flag", typ)
	}
	// Non-text families never carry raw user bytes into the export.
	for _, typ := range []defines.MysqlType{
		defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_BIT,
		defines.MYSQL_TYPE_DATE, defines.MYSQL_TYPE_DATETIME, defines.MYSQL_TYPE_YEAR,
		defines.MYSQL_TYPE_DECIMAL, defines.MYSQL_TYPE_JSON, defines.MYSQL_TYPE_UUID,
		defines.MYSQL_TYPE_TIMESTAMP, defines.MYSQL_TYPE_DOUBLE, defines.MYSQL_TYPE_BOOL,
	} {
		require.False(t, valueRequiresValidUTF8(typ, 0), "type %d", typ)
	}
}
