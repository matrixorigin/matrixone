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

package write

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestParquetDataWriterWritesFieldIDsMetricsAndPartition(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id", "name", "created_at"})
	idVec := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed[int32](idVec, 1, false, mp))
	require.NoError(t, vector.AppendFixed[int32](idVec, 2, false, mp))
	bat.Vecs[0] = idVec
	nameVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(nameVec, []byte("alice"), false, mp))
	require.NoError(t, vector.AppendBytes(nameVec, []byte("bob"), true, mp))
	bat.Vecs[1] = nameVec
	dateVec := vector.NewVec(types.T_date.ToType())
	require.NoError(t, vector.AppendFixed[types.Date](dateVec, types.DateFromCalendar(2026, 6, 10), false, mp))
	require.NoError(t, vector.AppendFixed[types.Date](dateVec, types.DateFromCalendar(2026, 6, 20), false, mp))
	bat.Vecs[2] = dateVec
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	schema := api.Schema{SchemaID: 3, Fields: []api.SchemaField{
		{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeInt}},
		{ID: 2, Name: "name", Type: api.IcebergType{Kind: api.TypeString}},
		{ID: 3, Name: "created_at", Type: api.IcebergType{Kind: api.TypeDate}},
	}}
	spec := api.PartitionSpec{SpecID: 7, Fields: []api.PartitionField{{
		SourceID:  3,
		FieldID:   1000,
		Name:      "created_month",
		Transform: "month",
	}}}
	var buf bytes.Buffer
	writer, err := NewParquetDataWriter(ctx, DataWriterConfig{
		Schema:        schema,
		PartitionSpec: spec,
		FilePath:      "s3://warehouse/sales/orders/data/created_month=677/part-1.parquet",
	}, &buf)
	require.NoError(t, err)
	require.NoError(t, writer.WriteBatch(ctx, bat.Attrs, bat))
	dataFile, err := writer.Close(ctx)
	require.NoError(t, err)

	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Equal(t, 1, file.Root().Column("id").ID())
	require.Equal(t, 2, file.Root().Column("name").ID())
	requireParquetColumnPlainEncoded(t, file, "name")
	require.Equal(t, int64(2), dataFile.RecordCount)
	require.Equal(t, int64(buf.Len()), dataFile.FileSizeInBytes)
	require.Equal(t, int64(2), dataFile.ValueCounts[1])
	require.Equal(t, int64(1), dataFile.NullValueCounts[2])
	require.Equal(t, int64(0), dataFile.NullValueCounts[1])
	require.Equal(t, int32((2026-1970)*12+5), dataFile.Partition["created_month"])
	require.Equal(t, "created_month=677", writer.PartitionPath())
	require.Equal(t, "parquet", dataFile.FileFormat)
	require.NotEmpty(t, dataFile.FilePathHash)
	require.False(t, strings.Contains(dataFile.FilePathRedacted, "warehouse"))
	require.Equal(t, int32(1), int32(binary.LittleEndian.Uint32(dataFile.LowerBounds[1])))
	require.Equal(t, int32(2), int32(binary.LittleEndian.Uint32(dataFile.UpperBounds[1])))
	require.Equal(t, []byte("alice"), dataFile.LowerBounds[2])
	require.Equal(t, []byte("alice"), dataFile.UpperBounds[2])
}

func TestParquetDataWriterWritesNullPartitionValue(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id", "region"})
	idVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](idVec, 1, false, mp))
	bat.Vecs[0] = idVec
	regionVec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(regionVec, nil, true, mp))
	bat.Vecs[1] = regionVec
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	var buf bytes.Buffer
	writer, err := NewParquetDataWriter(ctx, DataWriterConfig{
		Schema: api.Schema{SchemaID: 3, Fields: []api.SchemaField{
			{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
			{ID: 2, Name: "region", Type: api.IcebergType{Kind: api.TypeString}},
		}},
		PartitionSpec: api.PartitionSpec{SpecID: 7, Fields: []api.PartitionField{{
			SourceID: 2,
			FieldID:  1000,
			Name:     "region",
		}}},
		FilePath: "s3://warehouse/accounts/data/region=null/part-1.parquet",
	}, &buf)
	require.NoError(t, err)
	require.NoError(t, writer.WriteBatch(ctx, bat.Attrs, bat))
	dataFile, err := writer.Close(ctx)
	require.NoError(t, err)

	require.Contains(t, dataFile.Partition, "region")
	require.Nil(t, dataFile.Partition["region"])
	require.Equal(t, "region=null", writer.PartitionPath())
	require.Equal(t, int64(1), dataFile.NullValueCounts[2])
}

func TestParquetDataWriterFillsMissingNullableColumnWithNull(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id"})
	idVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](idVec, 1, false, mp))
	bat.Vecs[0] = idVec
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	var buf bytes.Buffer
	writer, err := NewParquetDataWriter(ctx, DataWriterConfig{
		Schema: api.Schema{SchemaID: 3, Fields: []api.SchemaField{
			{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
			{ID: 2, Name: "region", Type: api.IcebergType{Kind: api.TypeString}},
		}},
		PartitionSpec: api.PartitionSpec{SpecID: 7, Fields: []api.PartitionField{{
			SourceID: 2,
			FieldID:  1000,
			Name:     "region",
		}}},
		FilePath: "s3://warehouse/accounts/data/region=null/part-1.parquet",
	}, &buf)
	require.NoError(t, err)
	require.NoError(t, writer.WriteBatch(ctx, bat.Attrs, bat))
	dataFile, err := writer.Close(ctx)
	require.NoError(t, err)

	require.Contains(t, dataFile.Partition, "region")
	require.Nil(t, dataFile.Partition["region"])
	require.Equal(t, "region=null", writer.PartitionPath())
	require.Equal(t, int64(1), dataFile.NullValueCounts[2])
	require.Equal(t, int64(1), dataFile.ValueCounts[2])
}

func TestParquetDataWriterRejectsMissingRequiredColumn(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id"})
	idVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](idVec, 1, false, mp))
	bat.Vecs[0] = idVec
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	var buf bytes.Buffer
	writer, err := NewParquetDataWriter(ctx, DataWriterConfig{
		Schema: api.Schema{SchemaID: 3, Fields: []api.SchemaField{
			{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
			{ID: 2, Name: "region", Required: true, Type: api.IcebergType{Kind: api.TypeString}},
		}},
		PartitionSpec: api.PartitionSpec{SpecID: 7},
		FilePath:      "s3://warehouse/accounts/data/part-1.parquet",
	}, &buf)
	require.NoError(t, err)
	err = writer.WriteBatch(ctx, bat.Attrs, bat)
	require.Error(t, err)
	require.Contains(t, err.Error(), "required column is missing")
}

func TestParquetDataWriterRejectsMultiplePartitionsInOneFile(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"created_at"})
	dateVec := vector.NewVec(types.T_date.ToType())
	require.NoError(t, vector.AppendFixed[types.Date](dateVec, types.DateFromCalendar(2026, 6, 20), false, mp))
	require.NoError(t, vector.AppendFixed[types.Date](dateVec, types.DateFromCalendar(2026, 7, 1), false, mp))
	bat.Vecs[0] = dateVec
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	var buf bytes.Buffer
	writer, err := NewParquetDataWriter(ctx, DataWriterConfig{
		Schema: api.Schema{Fields: []api.SchemaField{{
			ID: 1, Name: "created_at", Type: api.IcebergType{Kind: api.TypeDate},
		}}},
		PartitionSpec: api.PartitionSpec{SpecID: 7, Fields: []api.PartitionField{{
			SourceID: 1, Name: "created_month", Transform: "month",
		}}},
		FilePath: "s3://warehouse/sales/orders/data/part-1.parquet",
	}, &buf)
	require.NoError(t, err)
	err = writer.WriteBatch(ctx, bat.Attrs, bat)
	require.Error(t, err)
	require.Contains(t, err.Error(), "multiple partition tuples")
}

func TestParquetDataWriterWritesTimestampBounds(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"event_time", "event_instant"})
	datetimeVec := vector.NewVec(types.T_datetime.ToType())
	dt1 := types.DatetimeFromClock(2026, 6, 10, 1, 2, 3, 123456)
	dt2 := types.DatetimeFromClock(2026, 6, 10, 2, 3, 4, 654321)
	require.NoError(t, vector.AppendFixed[types.Datetime](datetimeVec, dt2, false, mp))
	require.NoError(t, vector.AppendFixed[types.Datetime](datetimeVec, dt1, false, mp))
	bat.Vecs[0] = datetimeVec
	timestampVec := vector.NewVec(types.T_timestamp.ToType())
	ts1, err := types.ParseTimestamp(time.UTC, "2026-06-10 01:02:03.123456", 6)
	require.NoError(t, err)
	ts2, err := types.ParseTimestamp(time.UTC, "2026-06-10 02:03:04.654321", 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed[types.Timestamp](timestampVec, ts2, false, mp))
	require.NoError(t, vector.AppendFixed[types.Timestamp](timestampVec, ts1, false, mp))
	bat.Vecs[1] = timestampVec
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	var buf bytes.Buffer
	writer, err := NewParquetDataWriter(ctx, DataWriterConfig{
		Schema: api.Schema{SchemaID: 3, Fields: []api.SchemaField{
			{ID: 10, Name: "event_time", Required: true, Type: api.IcebergType{Kind: api.TypeTimestamp}},
			{ID: 11, Name: "event_instant", Required: true, Type: api.IcebergType{Kind: api.TypeTimestampTZ}},
		}},
		PartitionSpec: api.PartitionSpec{SpecID: 7},
		FilePath:      "s3://warehouse/sales/events/data/part-1.parquet",
	}, &buf)
	require.NoError(t, err)
	require.NoError(t, writer.WriteBatch(ctx, bat.Attrs, bat))
	dataFile, err := writer.Close(ctx)
	require.NoError(t, err)

	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Equal(t, 10, file.Root().Column("event_time").ID())
	require.Equal(t, 11, file.Root().Column("event_instant").ID())
	epoch := types.DatetimeFromClock(1970, 1, 1, 0, 0, 0, 0)
	require.Equal(t, int64(dt1-epoch), int64(binary.LittleEndian.Uint64(dataFile.LowerBounds[10])))
	require.Equal(t, int64(dt2-epoch), int64(binary.LittleEndian.Uint64(dataFile.UpperBounds[10])))
	require.Equal(t, int64(ts1)-int64(epoch), int64(binary.LittleEndian.Uint64(dataFile.LowerBounds[11])))
	require.Equal(t, int64(ts2)-int64(epoch), int64(binary.LittleEndian.Uint64(dataFile.UpperBounds[11])))
}

func TestParquetDataWriterWritesDecimalAndMOTimestamp(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"fare_amount", "pickup_time"})
	fareVec := vector.NewVec(types.New(types.T_decimal64, 12, 2))
	lowFare, err := types.ParseDecimal64("-5.67", 12, 2)
	require.NoError(t, err)
	highFare, err := types.ParseDecimal64("12.34", 12, 2)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed[types.Decimal64](fareVec, highFare, false, mp))
	require.NoError(t, vector.AppendFixed[types.Decimal64](fareVec, lowFare, false, mp))
	bat.Vecs[0] = fareVec
	pickupVec := vector.NewVec(types.T_timestamp.ToType())
	ts1, err := types.ParseTimestamp(time.UTC, "2024-01-15 00:00:00.123456", 6)
	require.NoError(t, err)
	ts2, err := types.ParseTimestamp(time.UTC, "2024-01-15 01:02:03.654321", 6)
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed[types.Timestamp](pickupVec, ts2, false, mp))
	require.NoError(t, vector.AppendFixed[types.Timestamp](pickupVec, ts1, false, mp))
	bat.Vecs[1] = pickupVec
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	var buf bytes.Buffer
	writer, err := NewParquetDataWriter(ctx, DataWriterConfig{
		Schema: api.Schema{SchemaID: 3, Fields: []api.SchemaField{
			{ID: 20, Name: "fare_amount", Type: api.IcebergType{Kind: api.TypeDecimal, Precision: 12, Scale: 2}},
			{ID: 21, Name: "pickup_time", Type: api.IcebergType{Kind: api.TypeTimestamp}},
		}},
		PartitionSpec: api.PartitionSpec{SpecID: 7},
		FilePath:      "s3://warehouse/tlc/export/data/part-1.parquet",
		TimeZone:      time.UTC,
	}, &buf)
	require.NoError(t, err)
	require.NoError(t, writer.WriteBatch(ctx, bat.Attrs, bat))
	dataFile, err := writer.Close(ctx)
	require.NoError(t, err)

	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Equal(t, 20, file.Root().Column("fare_amount").ID())
	require.Equal(t, 21, file.Root().Column("pickup_time").ID())
	require.Equal(t, int64(2), dataFile.RecordCount)
	require.Equal(t, decimalInt64BoundBytes(-567), dataFile.LowerBounds[20])
	require.Equal(t, decimalInt64BoundBytes(1234), dataFile.UpperBounds[20])
	epoch := types.DatetimeFromClock(1970, 1, 1, 0, 0, 0, 0)
	require.Equal(t, int64(ts1.ToDatetime(time.UTC)-epoch), int64(binary.LittleEndian.Uint64(dataFile.LowerBounds[21])))
	require.Equal(t, int64(ts2.ToDatetime(time.UTC)-epoch), int64(binary.LittleEndian.Uint64(dataFile.UpperBounds[21])))
}

func TestFanoutParquetDataWriterSplitsByPartitionAndTargetSize(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id", "created_at"})
	idVec := vector.NewVec(types.T_int32.ToType())
	dateVec := vector.NewVec(types.T_date.ToType())
	for i, date := range []types.Date{
		types.DateFromCalendar(2026, 6, 20),
		types.DateFromCalendar(2026, 6, 21),
		types.DateFromCalendar(2026, 7, 1),
		types.DateFromCalendar(2026, 7, 2),
	} {
		require.NoError(t, vector.AppendFixed[int32](idVec, int32(i+1), false, mp))
		require.NoError(t, vector.AppendFixed[types.Date](dateVec, date, false, mp))
	}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = dateVec
	bat.SetRowCount(4)
	defer bat.Clean(mp)

	factory := &memoryDataFileFactory{objects: make(map[string]*bytes.Buffer)}
	writer, err := NewFanoutParquetDataWriter(ctx, FanoutWriterConfig{
		Schema: api.Schema{Fields: []api.SchemaField{
			{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeInt}},
			{ID: 2, Name: "created_at", Type: api.IcebergType{Kind: api.TypeDate}},
		}},
		PartitionSpec: api.PartitionSpec{SpecID: 7, Fields: []api.PartitionField{{
			SourceID: 2, Name: "created_month", Transform: "month",
		}}},
		TableLocation:       "s3://warehouse/sales/orders",
		WriterID:            "stmt-1",
		TargetFileSizeBytes: 1,
	}, factory)
	require.NoError(t, err)
	require.NoError(t, writer.WriteBatch(ctx, bat.Attrs, bat))
	files, err := writer.Close(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(files), 2)

	seenPartitions := make(map[int32]bool)
	for _, file := range files {
		require.Equal(t, "parquet", file.FileFormat)
		require.Equal(t, int64(1), file.RecordCount)
		require.NotEmpty(t, factory.objects[file.FilePath])
		require.True(t, strings.HasPrefix(file.FilePath, "s3://warehouse/sales/orders/data/created_month="))
		seenPartitions[file.Partition["created_month"].(int32)] = true
		_, err := parquet.OpenFile(bytes.NewReader(factory.objects[file.FilePath].Bytes()), file.FileSizeInBytes)
		require.NoError(t, err)
	}
	require.True(t, seenPartitions[int32((2026-1970)*12+5)])
	require.True(t, seenPartitions[int32((2026-1970)*12+6)])
}

func TestFanoutParquetDataWriterWritesNullPartitionValue(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id", "region"})
	idVec := vector.NewVec(types.T_int64.ToType())
	regionVec := vector.NewVec(types.T_varchar.ToType())
	for _, id := range []int64{1, 2} {
		require.NoError(t, vector.AppendFixed[int64](idVec, id, false, mp))
		require.NoError(t, vector.AppendBytes(regionVec, nil, true, mp))
	}
	bat.Vecs[0] = idVec
	bat.Vecs[1] = regionVec
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	factory := &memoryDataFileFactory{objects: make(map[string]*bytes.Buffer)}
	writer, err := NewFanoutParquetDataWriter(ctx, FanoutWriterConfig{
		Schema: api.Schema{Fields: []api.SchemaField{
			{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
			{ID: 2, Name: "region", Type: api.IcebergType{Kind: api.TypeString}},
		}},
		PartitionSpec: api.PartitionSpec{SpecID: 7, Fields: []api.PartitionField{{
			SourceID: 2,
			FieldID:  1000,
			Name:     "region",
		}}},
		TableLocation:       "s3://warehouse/accounts",
		WriterID:            "stmt-1",
		TargetFileSizeBytes: 1024 * 1024,
	}, factory)
	require.NoError(t, err)
	require.NoError(t, writer.WriteBatch(ctx, bat.Attrs, bat))
	files, err := writer.Close(ctx)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Contains(t, files[0].Partition, "region")
	require.Nil(t, files[0].Partition["region"])
	require.Contains(t, files[0].FilePath, "/region=null/")
}

func TestFanoutParquetDataWriterFillsMissingNullablePartitionValue(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"id"})
	idVec := vector.NewVec(types.T_int64.ToType())
	for _, id := range []int64{1, 2} {
		require.NoError(t, vector.AppendFixed[int64](idVec, id, false, mp))
	}
	bat.Vecs[0] = idVec
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	factory := &memoryDataFileFactory{objects: make(map[string]*bytes.Buffer)}
	writer, err := NewFanoutParquetDataWriter(ctx, FanoutWriterConfig{
		Schema: api.Schema{Fields: []api.SchemaField{
			{ID: 1, Name: "id", Required: true, Type: api.IcebergType{Kind: api.TypeLong}},
			{ID: 2, Name: "region", Type: api.IcebergType{Kind: api.TypeString}},
		}},
		PartitionSpec: api.PartitionSpec{SpecID: 7, Fields: []api.PartitionField{{
			SourceID: 2,
			FieldID:  1000,
			Name:     "region",
		}}},
		TableLocation:       "s3://warehouse/accounts",
		WriterID:            "stmt-1",
		TargetFileSizeBytes: 1024 * 1024,
	}, factory)
	require.NoError(t, err)
	require.NoError(t, writer.WriteBatch(ctx, bat.Attrs, bat))
	files, err := writer.Close(ctx)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Contains(t, files[0].Partition, "region")
	require.Nil(t, files[0].Partition["region"])
	require.Contains(t, files[0].FilePath, "/region=null/")
	require.Equal(t, int64(2), files[0].NullValueCounts[2])
}

func requireParquetColumnPlainEncoded(t *testing.T, file *parquet.File, path string) {
	t.Helper()
	for _, rowGroup := range file.Metadata().RowGroups {
		for _, column := range rowGroup.Columns {
			if strings.Join(column.MetaData.PathInSchema, ".") != path {
				continue
			}
			encodings := make([]string, 0, len(column.MetaData.Encoding))
			for _, encoding := range column.MetaData.Encoding {
				encodings = append(encodings, encoding.String())
			}
			require.Contains(t, encodings, "PLAIN")
			require.NotContains(t, encodings, "DELTA_LENGTH_BYTE_ARRAY")
			return
		}
	}
	require.Failf(t, "column not found", "column path %s was not found in parquet metadata", path)
}

type memoryDataFileFactory struct {
	objects map[string]*bytes.Buffer
}

func (f *memoryDataFileFactory) CreateDataFile(ctx context.Context, location string) (io.WriteCloser, error) {
	buf := new(bytes.Buffer)
	f.objects[location] = buf
	return nopWriteCloser{Writer: buf}, nil
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error {
	return nil
}
