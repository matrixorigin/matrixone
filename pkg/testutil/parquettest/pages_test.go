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
