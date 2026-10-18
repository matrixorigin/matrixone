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

package maintenance

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

// Benchmark the complete in-memory production compactor. Input encoding and
// metadata setup are excluded; each iteration reads and rewrites both files.
func BenchmarkRewriteDataFilesIdenticalSchema(b *testing.B) {
	const rowsPerFile = 2048
	const columns = 8
	meta := maintenanceTestTableMetadata("s3://warehouse/orders")
	meta.Schemas[0].Fields = make([]api.SchemaField, columns)
	group := make(parquet.Group, columns)
	for i := range meta.Schemas[0].Fields {
		name := fmt.Sprintf("c%02d", i)
		meta.Schemas[0].Fields[i] = api.SchemaField{ID: i + 1, Name: name, Required: true, Type: api.IcebergType{Kind: api.TypeLong}}
		group[name] = parquet.FieldID(parquet.Int(64), i+1)
	}
	rows := make([]parquet.Row, rowsPerFile)
	for r := range rows {
		rows[r] = make(parquet.Row, columns)
		for col := range rows[r] {
			rows[r][col] = parquet.Int64Value(int64(r*columns+col)).Level(0, 0, col)
		}
	}
	var input bytes.Buffer
	writer := parquet.NewWriter(&input, parquet.NewSchema("input", group))
	if _, err := writer.WriteRows(rows); err != nil {
		b.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	list, err := encodeMaintenanceTestManifestList(nil)
	if err != nil {
		b.Fatal(err)
	}
	const listPath = "s3://warehouse/orders/metadata/list.avro"
	reader := fakeRewriteManifestObjectReader{listPath: list}
	selection := RewriteDataFileGroup{}
	for i := 0; i < 2; i++ {
		path := fmt.Sprintf("s3://warehouse/orders/data/input-%d.parquet", i)
		reader[path] = input.Bytes()
		selection.Candidates = append(selection.Candidates, RewriteDataFileCandidate{File: api.DataFile{Content: api.DataFileContentData, FilePath: path, FileFormat: "parquet", RecordCount: rowsPerFile, FileSizeInBytes: int64(input.Len())}})
	}
	req := RewriteDataFilesCompactRequest{Metadata: meta, Snapshot: api.Snapshot{SnapshotID: 10, ManifestList: listPath}, Selection: RewriteDataFilesSelection{Groups: []RewriteDataFileGroup{selection}}}
	compactor := ParquetConcatRewriteDataFilesCompactor{ObjectReader: reader}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := compactor.CompactRewriteDataFiles(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Objects) != 1 || result.Rewrites[0].ReplacementFiles[0].RecordCount != 2*rowsPerFile {
			b.Fatal("incomplete compaction")
		}
	}
}
