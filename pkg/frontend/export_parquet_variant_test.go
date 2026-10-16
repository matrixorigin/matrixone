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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/parquetvariant"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetVariantExport(t *testing.T) {
	proc := testutil.NewProc(t)
	mrs := &MysqlResultSet{}
	col := new(MysqlColumn)
	col.SetName("payload")
	col.SetColumnType(defines.MYSQL_TYPE_JSON)
	mrs.AddColumn(col)
	for _, variant := range []bool{false, true} {
		writer, err := NewParquetWriterWithOptions(context.Background(), mrs, ParquetWriterOptions{JSONAsVariant: variant})
		require.NoError(t, err)
		vec := vector.NewVec(types.T_json.ToType())
		bj, err := bytejson.ParseFromStringExact(`{"cost":9007199254740993.001,"id":18446744073709551615}`)
		require.NoError(t, err)
		require.NoError(t, vector.AppendByteJson(vec, bj, false, proc.Mp()))
		require.NoError(t, vector.AppendByteJson(vec, bytejson.Null, false, proc.Mp()))
		require.NoError(t, vector.AppendByteJson(vec, bytejson.Null, true, proc.Mp()))
		bat := batch.New([]string{"payload"})
		bat.Vecs[0] = vec
		bat.SetRowCount(3)
		require.NoError(t, writer.WriteBatch(bat, proc.Mp(), time.UTC))
		bat.Clean(proc.Mp())
		data, err := writer.Close()
		require.NoError(t, err)
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, err)
		require.Equal(t, int64(3), f.NumRows())
		column := f.Root().Columns()[0]
		if variant {
			require.NotNil(t, column.Type().LogicalType().Variant)
		} else {
			require.NotNil(t, column.Type().LogicalType().UTF8)
		}
		reader := parquet.NewReader(f)
		rows := make([]parquet.Row, 3)
		n, err := reader.ReadRows(rows)
		require.True(t, err == nil || err == io.EOF)
		require.Equal(t, 3, n)
		require.NoError(t, reader.Close())
		if variant {
			got, err := parquetvariant.Decode(context.Background(), rows[0][0].ByteArray(), rows[0][1].ByteArray())
			require.NoError(t, err)
			require.Zero(t, bytejson.CompareByteJson(bj, got))
			got, err = parquetvariant.Decode(context.Background(), rows[1][0].ByteArray(), rows[1][1].ByteArray())
			require.NoError(t, err)
			require.True(t, got.IsNull())
		} else {
			require.Equal(t, bj.String(), string(rows[0][0].ByteArray()))
			require.Equal(t, "null", string(rows[1][0].ByteArray()))
		}
		require.True(t, rows[2][0].IsNull())
	}
}

func TestParquetVariantExportFormat(t *testing.T) {
	ep := &tree.ExportParam{ExportFormat: "parquet_variant", FilePath: "agent.parquet"}
	require.NoError(t, validateExportFormat(context.Background(), ep))
	require.Equal(t, "parquet_variant", ep.ExportFormat)
	ec := &ExportConfig{userConfig: ep}
	require.Equal(t, "parquet", ec.getExportFormat())
	ep.FilePath = "agent.csv"
	require.Error(t, validateExportFormat(context.Background(), ep))
}

func TestParquetVariantConstNullExport(t *testing.T) {
	proc := testutil.NewProc(t)
	mrs := &MysqlResultSet{}
	col := new(MysqlColumn)
	col.SetName("payload")
	col.SetColumnType(defines.MYSQL_TYPE_JSON)
	mrs.AddColumn(col)
	writer, err := NewParquetWriterWithOptions(context.Background(), mrs, ParquetWriterOptions{JSONAsVariant: true})
	require.NoError(t, err)
	bat := batch.New([]string{"payload"})
	bat.Vecs[0] = vector.NewConstNull(types.T_json.ToType(), 3, proc.Mp())
	bat.SetRowCount(3)
	require.NoError(t, writer.WriteBatch(bat, proc.Mp(), time.UTC))
	bat.Clean(proc.Mp())
	data, err := writer.Close()
	require.NoError(t, err)
	reader := parquet.NewReader(bytes.NewReader(data))
	defer reader.Close()
	rows := make([]parquet.Row, 3)
	n, err := reader.ReadRows(rows)
	require.True(t, err == nil || err == io.EOF)
	require.Equal(t, 3, n)
	for _, row := range rows {
		for _, value := range row {
			require.True(t, value.IsNull())
		}
	}
}

func TestParquetVariantExportTenThousandRowsAcrossGroups(t *testing.T) {
	for _, variant := range []bool{false, true} {
		t.Run(fmt.Sprint(variant), func(t *testing.T) {
			proc := testutil.NewProc(t)
			mrs := &MysqlResultSet{}
			col := new(MysqlColumn)
			col.SetName("payload")
			col.SetColumnType(defines.MYSQL_TYPE_JSON)
			mrs.AddColumn(col)
			writer, err := NewParquetWriterWithOptions(context.Background(), mrs, ParquetWriterOptions{JSONAsVariant: variant})
			require.NoError(t, err)
			// Force page and row-group boundaries inside a single WriteBatch call.
			writer.writer = parquet.NewGenericWriter[any](writer.buf, writer.schema, parquet.MaxRowsPerRowGroup(127), parquet.PageBufferSize(256))
			bat := batch.New([]string{"payload"})
			vec := vector.NewVec(types.T_json.ToType())
			bat.Vecs[0] = vec
			const n = 10000
			for i := 0; i < n; i++ {
				bj, err := bytejson.ParseFromStringExact(fmt.Sprintf(`{"id":%d,"cost":9007199254740993.001}`, i))
				require.NoError(t, err)
				require.NoError(t, vector.AppendByteJson(vec, bj, i%17 == 0, proc.Mp()))
			}
			bat.SetRowCount(n)
			require.NoError(t, writer.WriteBatch(bat, proc.Mp(), time.UTC))
			bat.Clean(proc.Mp())
			data, err := writer.Close()
			require.NoError(t, err)
			file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
			require.NoError(t, err)
			require.Equal(t, int64(n), file.NumRows())
			require.Len(t, file.RowGroups(), 79)
			for i, group := range file.RowGroups() {
				want := int64(127)
				if i == 78 {
					want = 94
				}
				require.Equal(t, want, group.NumRows())
			}
			reader := parquet.NewReader(file)
			defer reader.Close()
			rows := make([]parquet.Row, 31)
			seen := 0
			for {
				count, err := reader.ReadRows(rows)
				require.True(t, err == nil || err == io.EOF)
				for _, row := range rows[:count] {
					require.Equal(t, seen%17 == 0, row[0].IsNull(), "row %d", seen)
					if seen%17 != 0 {
						text := string(row[0].ByteArray())
						if variant {
							bj, e := parquetvariant.Decode(context.Background(), row[0].ByteArray(), row[1].ByteArray())
							require.NoError(t, e)
							text = bj.String()
						}
						want, e := bytejson.ParseFromStringExact(fmt.Sprintf(`{"id":%d,"cost":9007199254740993.001}`, seen))
						require.NoError(t, e)
						require.Equal(t, want.String(), text)
					}
					seen++
				}
				if err == io.EOF {
					break
				}
			}
			require.Equal(t, n, seen)
		})
	}
}

func TestParquetVariantSplitExportKeepsSchemaAndValues(t *testing.T) {
	for _, n := range []int{0, 32} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			proc := testutil.NewProc(t)
			mp := proc.Mp()
			mrs := &MysqlResultSet{}
			for _, spec := range []struct {
				name string
				typ  defines.MysqlType
			}{{"id", defines.MYSQL_TYPE_LONG}, {"payload", defines.MYSQL_TYPE_JSON}} {
				c := new(MysqlColumn)
				c.SetName(spec.name)
				c.SetColumnType(spec.typ)
				mrs.AddColumn(c)
			}
			dir := t.TempDir()
			ec := &ExportConfig{ctx: context.Background(), mrs: mrs, userConfig: &tree.ExportParam{FilePath: filepath.Join(dir, "out_%d.parquet"), ExportFormat: "parquet_variant", SplitSize: 8 << 10}}
			execCtx := &ExecCtx{reqCtx: ec.ctx, ses: &backSession{feSessionImpl: feSessionImpl{pool: mp}}}
			expected := make([]string, n)
			if n > 0 {
				bat := batch.NewWithSize(2)
				bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
				bat.Vecs[1] = vector.NewVec(types.T_json.ToType())
				for i := 0; i < n; i++ {
					require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(i), false, mp))
					text := fmt.Sprintf(`{"cost":9007199254740993.001,"padding":"%s"}`, strings.Repeat("x", 512))
					if i%7 == 0 {
						text = "null"
					}
					bj, err := bytejson.ParseFromStringExact(text)
					require.NoError(t, err)
					require.NoError(t, vector.AppendByteJson(bat.Vecs[1], bj, i%11 == 0, mp))
					if i%11 != 0 {
						expected[i] = bj.String()
					}
				}
				bat.SetRowCount(n)
				err := ec.Write(execCtx, nil, bat)
				bat.Clean(mp)
				require.NoError(t, err)
			}
			require.NoError(t, finalizeParquetExport(ec))
			require.Equal(t, uint64(n), ec.Rows)
			files, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
			require.NoError(t, err)
			if n == 0 {
				require.Len(t, files, 1)
			} else {
				require.Greater(t, len(files), 1)
			}
			seen := make(map[int]bool, n)
			for _, name := range files {
				data, err := os.ReadFile(name)
				require.NoError(t, err)
				f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
				require.NoError(t, err)
				require.NotNil(t, f.Root().Columns()[1].Type().LogicalType().Variant)
				if n == 0 {
					require.Zero(t, f.NumRows())
					continue
				}
				require.Positive(t, f.NumRows())
				r := parquet.NewReader(f)
				rows := make([]parquet.Row, 7)
				count := 0
				for {
					size, err := r.ReadRows(rows)
					require.True(t, err == nil || err == io.EOF)
					for _, row := range rows[:size] {
						id := int(row[0].Int32())
						require.GreaterOrEqual(t, id, 0)
						require.Less(t, id, n)
						require.False(t, seen[id])
						seen[id] = true
						require.Equal(t, expected[id] == "", row[1].IsNull())
						if expected[id] != "" {
							bj, e := parquetvariant.Decode(context.Background(), row[1].ByteArray(), row[2].ByteArray())
							require.NoError(t, e)
							require.Equal(t, expected[id], bj.String())
						}
						count++
					}
					if err == io.EOF {
						break
					}
				}
				require.NoError(t, r.Close())
				require.Equal(t, int64(count), f.NumRows())
			}
			require.Len(t, seen, n)
		})
	}
}
