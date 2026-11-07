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

// Package parquettest contains assertions for Parquet output fixtures.
package parquettest

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/stretchr/testify/require"
)

// Keep the statistics field optional so presence is distinguishable from a
// present, zero-valued statistics struct.
type dataPageHeader struct {
	Statistics *struct{} `thrift:"5,optional"`
}
type dataPageHeaderV2 struct {
	Statistics *struct{} `thrift:"8,optional"`
}
type pageHeader struct {
	CompressedPageSize int32             `thrift:"3,required"`
	DataPageHeader     *dataPageHeader   `thrift:"5,optional"`
	DataPageHeaderV2   *dataPageHeaderV2 `thrift:"8,optional"`
}

// RequireNoDataPageStatistics checks every data page of each column chunk.
func RequireNoDataPageStatistics(t *testing.T, data []byte) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	require.NotEmpty(t, f.Metadata().RowGroups)
	for _, group := range f.Metadata().RowGroups {
		for _, column := range group.Columns {
			meta := column.MetaData
			offset := meta.DataPageOffset
			if meta.DictionaryPageOffset > 0 && meta.DictionaryPageOffset < offset {
				offset = meta.DictionaryPageOffset
			}
			require.Greater(t, offset, int64(0))
			require.Less(t, offset, int64(len(data)))
			require.Positive(t, meta.TotalCompressedSize)
			require.LessOrEqual(t, meta.TotalCompressedSize, int64(len(data))-offset)
			end := offset + meta.TotalCompressedSize
			pages := 0
			for offset < end {
				input := bytes.NewReader(data[offset:end])
				var header pageHeader
				require.NoError(t, thrift.NewDecoder((&thrift.CompactProtocol{}).NewReader(input)).Decode(&header))
				headerSize := end - offset - int64(input.Len())
				require.Positive(t, headerSize)
				require.GreaterOrEqual(t, header.CompressedPageSize, int32(0))
				offset += headerSize + int64(header.CompressedPageSize)
				require.LessOrEqual(t, offset, end)
				if header.DataPageHeader != nil {
					pages++
					require.Nil(t, header.DataPageHeader.Statistics)
				}
				if header.DataPageHeaderV2 != nil {
					pages++
					require.Nil(t, header.DataPageHeaderV2.Statistics)
				}
			}
			require.Positive(t, pages)
		}
	}
}
