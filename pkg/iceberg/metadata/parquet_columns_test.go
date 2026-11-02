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

package metadata

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetColumnIndexByName(t *testing.T) {
	_, ok := ParquetColumnIndexByName(nil, "pos")
	require.False(t, ok)

	var buf bytes.Buffer
	schema := parquet.NewSchema("delete", parquet.Group{
		"File_Path": parquet.String(),
		"POS":       parquet.Leaf(parquet.Int64Type),
	})
	writer := parquet.NewWriter(&buf, schema)
	require.NoError(t, writer.Close())
	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	columns := file.Root().Columns()
	require.Len(t, columns, 2)
	require.Equal(t, "File_Path", columns[0].Name())
	require.Equal(t, "POS", columns[1].Name())

	idx, ok := ParquetColumnIndexByName(file, "missing", "  pOs  ")
	require.True(t, ok)
	require.Equal(t, int(columns[1].Index()), idx)
	// File column order wins even if candidate names are in the reverse order.
	idx, ok = ParquetColumnIndexByName(file, "pos", "FILE_PATH")
	require.True(t, ok)
	require.Equal(t, int(columns[0].Index()), idx)
	_, ok = ParquetColumnIndexByName(file, "missing")
	require.False(t, ok)
	_, ok = ParquetColumnIndexByName(file)
	require.False(t, ok)
}
