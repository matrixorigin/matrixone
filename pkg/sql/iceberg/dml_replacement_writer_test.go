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

package iceberg

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
)

func TestWriteDMLReplacementDataFilesWritesStatementScopedParquet(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id", "created_at", "name"})
	idVec := vector.NewVec(types.T_int32.ToType())
	dateVec := vector.NewVec(types.T_date.ToType())
	nameVec := vector.NewVec(types.T_varchar.ToType())
	for idx, row := range []struct {
		id   int32
		date types.Date
		name string
	}{
		{id: 1, date: types.DateFromCalendar(2026, 6, 20), name: "alice"},
		{id: 2, date: types.DateFromCalendar(2026, 7, 1), name: "bob"},
	} {
		if err := vector.AppendFixed[int32](idVec, row.id, false, mp); err != nil {
			t.Fatalf("append id %d: %v", idx, err)
		}
		if err := vector.AppendFixed[types.Date](dateVec, row.date, false, mp); err != nil {
			t.Fatalf("append date %d: %v", idx, err)
		}
		if err := vector.AppendBytes(nameVec, []byte(row.name), false, mp); err != nil {
			t.Fatalf("append name %d: %v", idx, err)
		}
	}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = dateVec
	bat.Vecs[2] = nameVec
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	writer := &recordingDMLDeleteObjectWriter{}
	files, err := WriteDMLReplacementDataFiles(ctx, DMLReplacementDataFilesRequest{
		TableLocation: "s3://warehouse/gold/orders/",
		Operation:     dml.OperationMerge,
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"gold"},
			Table:          "orders",
			StatementID:    "merge into orders using sensitive_source",
			IdempotencyKey: "idem-merge",
		},
		SnapshotID: 77,
		Schema: api.Schema{SchemaID: 9, Fields: []api.SchemaField{
			{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeInt}},
			{ID: 2, Name: "created_at", Type: api.IcebergType{Kind: api.TypeDate}},
			{ID: 3, Name: "name", Type: api.IcebergType{Kind: api.TypeString}},
		}},
		PartitionSpec: api.PartitionSpec{SpecID: 7, Fields: []api.PartitionField{{
			SourceID:  2,
			FieldID:   1000,
			Name:      "created_month",
			Transform: "month",
		}}},
		Attrs:               bat.Attrs,
		Batch:               bat,
		TargetFileSizeBytes: 1,
		ObjectWriter:        writer,
	})
	if err != nil {
		t.Fatalf("write DML replacement data files: %v", err)
	}
	if len(files) < 2 || len(writer.objects) != len(files) {
		t.Fatalf("expected replacement files and matching objects, files=%d objects=%d", len(files), len(writer.objects))
	}
	for _, file := range files {
		if file.Content != api.DataFileContentData || file.FileFormat != "parquet" || file.RecordCount != 1 || file.FileSizeInBytes <= 0 {
			t.Fatalf("unexpected replacement data file: %+v", file)
		}
		if !strings.HasPrefix(file.FilePath, "s3://warehouse/gold/orders/data/mo-dml/merge/stmt-"+api.PathHash("merge into orders using sensitive_source")+"/replacement/snap-77/created_month=") {
			t.Fatalf("unexpected replacement file path: %s", file.FilePath)
		}
		if strings.Contains(file.FilePath, "sensitive_source") || strings.Contains(file.FilePath, "idem-merge") {
			t.Fatalf("replacement file path leaked raw statement/idempotency: %s", file.FilePath)
		}
		payload := writer.objects[file.FilePath]
		pf, err := parquet.OpenFile(bytes.NewReader(payload), int64(len(payload)))
		if err != nil {
			t.Fatalf("open replacement parquet %s: %v", file.FilePath, err)
		}
		if pf.Root().Column("id").ID() != 1 || pf.Root().Column("created_at").ID() != 2 || pf.Root().Column("name").ID() != 3 {
			t.Fatalf("replacement parquet field ids mismatch")
		}
		if file.Partition["created_month"] == nil || file.FilePathHash == "" || strings.Contains(file.FilePathRedacted, "warehouse") {
			t.Fatalf("replacement file metadata missing partition/hash/redaction: %+v", file)
		}
	}
}

func TestWriteDMLReplacementDataFilesRequiresOutput(t *testing.T) {
	_, err := WriteDMLReplacementDataFiles(context.Background(), DMLReplacementDataFilesRequest{
		TableLocation: "s3://warehouse/gold/orders",
		Operation:     dml.OperationUpdate,
		Base: dml.CommitBase{
			Namespace:      api.Namespace{"gold"},
			Table:          "orders",
			IdempotencyKey: "idem",
		},
		SnapshotID: 10,
	})
	if err == nil || !strings.Contains(err.Error(), "output factory or object writer") {
		t.Fatalf("expected missing output error, got %v", err)
	}
}

func TestBufferedDataFileObjectAbortDoesNotPublishPartialObject(t *testing.T) {
	objectWriter := &recordingDMLDeleteObjectWriter{}
	object := &bufferedDataFileObject{
		ctx:      context.Background(),
		location: "s3://warehouse/gold/orders/data/partial.parquet",
		writer:   objectWriter,
	}

	if _, err := object.Write([]byte("partial parquet payload")); err != nil {
		t.Fatalf("buffer partial object: %v", err)
	}
	if err := object.Abort(); err != nil {
		t.Fatalf("abort partial object: %v", err)
	}
	if err := object.Close(); err != nil {
		t.Fatalf("close aborted object: %v", err)
	}
	if len(objectWriter.objects) != 0 {
		t.Fatalf("abort published partial object: %+v", objectWriter.objects)
	}
}

func TestBufferedDataFileObjectSharesHardMemoryBudgetWithPublishCopy(t *testing.T) {
	objectWriter := &recordingDMLDeleteObjectWriter{}
	budget := newDMLMemoryBudget(100<<10, 0)
	object := &bufferedDataFileObject{
		ctx:      context.Background(),
		location: "s3://warehouse/gold/orders/data/bounded.parquet",
		writer:   objectWriter,
		budget:   budget,
	}
	requireWrite := bytes.Repeat([]byte{'x'}, 60<<10)
	if _, err := object.Write(requireWrite); err != nil {
		t.Fatalf("buffer bounded object: %v", err)
	}
	if err := object.Close(); err == nil || !strings.Contains(err.Error(), string(api.ErrPlanningLimitExceeded)) {
		t.Fatalf("expected publish-copy budget error, got %v", err)
	}
	if len(objectWriter.objects) != 0 {
		t.Fatalf("budget failure published object: %+v", objectWriter.objects)
	}
	if got := budget.usedBytes(); got != 0 {
		t.Fatalf("buffer reservation leaked after close: %d", got)
	}
}

func TestBufferedDataFileObjectChargesOldAndNewArraysDuringGrowth(t *testing.T) {
	budget := newDMLMemoryBudget(160<<10, 0)
	object := &bufferedDataFileObject{
		ctx:      context.Background(),
		location: "s3://warehouse/gold/orders/data/growth.parquet",
		writer:   &recordingDMLDeleteObjectWriter{},
		budget:   budget,
	}
	require.NoError(t, writeBufferedObjectForTest(object, 60<<10))
	// Growing 64 KiB -> 128 KiB needs both arrays live (192 KiB peak), even
	// though the final 128 KiB capacity alone fits under the 160 KiB limit.
	_, err := object.Write(bytes.Repeat([]byte{'y'}, 10<<10))
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrPlanningLimitExceeded))
	require.Equal(t, int64(64<<10), budget.usedBytes())
	require.NoError(t, object.Abort())
	require.Zero(t, budget.usedBytes())
}

func writeBufferedObjectForTest(object *bufferedDataFileObject, size int) error {
	_, err := object.Write(bytes.Repeat([]byte{'x'}, size))
	return err
}
