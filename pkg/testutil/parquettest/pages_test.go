// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquettest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestWriterPageStatisticsAndFooter(t *testing.T) {
	type row struct {
		Value *string `parquet:"value,optional"`
	}
	z, e, empty := "z", "é", ""
	rows := []row{{&z}, {nil}, {&e}, {&empty}}
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			var out bytes.Buffer
			writer := parquet.NewGenericWriter[row](&out, parquet.DataPageVersion(version), parquet.DataPageStatistics(false), parquet.PageBufferSize(1024))
			for iteration := 0; iteration < 2; iteration++ {
				if iteration > 0 {
					out.Reset()
					writer.Reset(&out)
				}
				for i := 0; i < 100; i++ {
					_, err := writer.Write(rows)
					require.NoError(t, err)
				}
				require.NoError(t, writer.Close())
				data := out.Bytes()
				RequireNoDataPageStatistics(t, data)
				f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
				require.NoError(t, err)
				stat := f.Metadata().RowGroups[0].Columns[0].MetaData.Statistics
				require.Empty(t, stat.Min)
				require.Empty(t, stat.Max)
				require.Equal(t, int64(100), stat.NullCount)
				require.Equal(t, []byte(""), stat.MinValue)
				require.Equal(t, []byte("é"), stat.MaxValue)
				if version == 2 {
					got, err := parquet.Read[row](bytes.NewReader(data), int64(len(data)))
					require.NoError(t, err)
					require.Len(t, got, 400)
					require.Equal(t, rows, got[:4])
				}
			}
		})
	}
}

func TestWriterStatisticsBoundaries(t *testing.T) {
	type row struct {
		Null   *int32 `parquet:"null,optional"`
		Number int32  `parquet:"number"`
		Flag   bool   `parquet:"flag"`
		Binary []byte `parquet:"binary"`
	}
	rows := make([]row, 3000)
	for i := range rows {
		rows[i] = row{Number: int32(i%3) - 1, Flag: i%2 != 0, Binary: []byte{0x7a + byte(i%2)*6}}
	}
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			var out bytes.Buffer
			w := parquet.NewGenericWriter[row](&out, parquet.DataPageVersion(version), parquet.DataPageStatistics(false), parquet.PageBufferSize(128))
			_, err := w.Write(rows)
			require.NoError(t, err)
			require.NoError(t, w.Close())
			data := out.Bytes()
			RequireNoDataPageStatistics(t, data)
			f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
			require.NoError(t, err)
			require.Len(t, f.RowGroups(), 1)
			chunks := f.RowGroups()[0].ColumnChunks()
			require.Len(t, chunks, 4)
			for col, chunk := range chunks {
				index, err := chunk.ColumnIndex()
				require.NoError(t, err)
				require.Greater(t, index.NumPages(), 1, "column %d", col)
				var nulls int64
				for p := 0; p < index.NumPages(); p++ {
					nulls += index.NullCount(p)
					if col == 0 {
						require.True(t, index.NullPage(p))
						continue
					}
					require.False(t, index.NullPage(p))
					switch col {
					case 1:
						require.Equal(t, int32(-1), index.MinValue(p).Int32())
						require.Equal(t, int32(1), index.MaxValue(p).Int32())
					case 2:
						require.False(t, index.MinValue(p).Boolean())
						require.True(t, index.MaxValue(p).Boolean())
					case 3:
						require.Equal(t, []byte{0x7a}, index.MinValue(p).ByteArray())
						require.Equal(t, []byte{0x80}, index.MaxValue(p).ByteArray())
					}
				}
				if col == 0 {
					require.Equal(t, int64(len(rows)), nulls)
				} else {
					require.Zero(t, nulls)
				}
			}
			stats := f.Metadata().RowGroups[0].Columns
			require.Equal(t, int64(len(rows)), stats[0].MetaData.Statistics.NullCount)
			for _, col := range stats {
				require.Empty(t, col.MetaData.Statistics.Min)
				require.Empty(t, col.MetaData.Statistics.Max)
			}
			require.Empty(t, stats[0].MetaData.Statistics.MinValue)
			require.Empty(t, stats[0].MetaData.Statistics.MaxValue)
			require.Equal(t, uint32(0xffffffff), binary.LittleEndian.Uint32(stats[1].MetaData.Statistics.MinValue))
			require.Equal(t, uint32(1), binary.LittleEndian.Uint32(stats[1].MetaData.Statistics.MaxValue))
			require.Equal(t, []byte{0}, stats[2].MetaData.Statistics.MinValue)
			require.Equal(t, []byte{1}, stats[2].MetaData.Statistics.MaxValue)
			require.Equal(t, []byte{0x7a}, stats[3].MetaData.Statistics.MinValue)
			require.Equal(t, []byte{0x80}, stats[3].MetaData.Statistics.MaxValue)
			if version == 2 {
				got, err := parquet.Read[row](bytes.NewReader(data), int64(len(data)))
				require.NoError(t, err)
				require.Equal(t, rows, got)
			}
		})
	}
}

func TestWriterZeroOnlyStatistics(t *testing.T) {
	type row struct {
		Number int32  `parquet:"number"`
		Flag   bool   `parquet:"flag"`
		Binary []byte `parquet:"binary"`
	}
	rows := []row{{Binary: []byte{}}}
	var out bytes.Buffer
	w := parquet.NewGenericWriter[row](&out, parquet.DataPageStatistics(false))
	_, err := w.Write(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	data := out.Bytes()
	RequireNoDataPageStatistics(t, data)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	chunks := f.RowGroups()[0].ColumnChunks()
	require.Len(t, chunks, 3)
	for col, chunk := range chunks {
		index, err := chunk.ColumnIndex()
		require.NoError(t, err)
		require.Equal(t, 1, index.NumPages())
		require.False(t, index.NullPage(0))
		require.Zero(t, index.NullCount(0))
		switch col {
		case 0:
			require.Zero(t, index.MinValue(0).Int32())
			require.Zero(t, index.MaxValue(0).Int32())
		case 1:
			require.False(t, index.MinValue(0).Boolean())
			require.False(t, index.MaxValue(0).Boolean())
		case 2:
			require.Empty(t, index.MinValue(0).ByteArray())
			require.Empty(t, index.MaxValue(0).ByteArray())
		}
	}
	got, err := parquet.Read[row](bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	require.Equal(t, rows, got)
}
