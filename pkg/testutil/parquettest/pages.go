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
	Statistics thrift.Null[struct{}] `thrift:"5,optional"`
}
type dataPageHeaderV2 struct {
	Statistics thrift.Null[struct{}] `thrift:"8,optional"`
}
type pageHeader struct {
	DataPageHeader   thrift.Null[dataPageHeader]   `thrift:"5,optional"`
	DataPageHeaderV2 thrift.Null[dataPageHeaderV2] `thrift:"8,optional"`
}

// RequireNoDataPageStatistics checks the first data page of each column chunk.
func RequireNoDataPageStatistics(t *testing.T, data []byte) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	require.NotEmpty(t, f.Metadata().RowGroups)
	for _, group := range f.Metadata().RowGroups {
		for _, column := range group.Columns {
			offset := column.MetaData.DataPageOffset
			require.Greater(t, offset, int64(0))
			require.Less(t, offset, int64(len(data)))
			protocol := &thrift.CompactProtocol{}
			var header pageHeader
			require.NoError(t, thrift.NewDecoder(protocol.NewReaderFromBytes(data[offset:])).Decode(&header))
			if header.DataPageHeader.Valid {
				require.False(t, header.DataPageHeader.V.Statistics.Valid)
			} else {
				require.True(t, header.DataPageHeaderV2.Valid)
				require.False(t, header.DataPageHeaderV2.V.Statistics.Valid)
			}
		}
	}
}
