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
	"math"
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

func TestParquetWriterPrimitiveConversionsAndBounds(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	loc := time.FixedZone("KSA", 3*3600)

	boolVec := vector.NewVec(types.T_bool.ToType())
	defer boolVec.Free(mp)
	require.NoError(t, vector.AppendFixed(boolVec, true, false, mp))
	got, isNull, err := vectorValue(ctx, boolVec, 0, api.IcebergType{Kind: api.TypeBoolean}, loc)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, true, got)

	int8Vec := vector.NewVec(types.T_int8.ToType())
	defer int8Vec.Free(mp)
	require.NoError(t, vector.AppendFixed(int8Vec, int8(-8), false, mp))
	got, _, err = vectorValue(ctx, int8Vec, 0, api.IcebergType{Kind: api.TypeInt}, loc)
	require.NoError(t, err)
	require.Equal(t, int32(-8), got)
	got, _, err = vectorValue(ctx, int8Vec, 0, api.IcebergType{Kind: api.TypeLong}, loc)
	require.NoError(t, err)
	require.Equal(t, int64(-8), got)

	int16Vec := vector.NewVec(types.T_int16.ToType())
	defer int16Vec.Free(mp)
	require.NoError(t, vector.AppendFixed(int16Vec, int16(-16), false, mp))
	got, _, err = vectorValue(ctx, int16Vec, 0, api.IcebergType{Kind: api.TypeInt}, loc)
	require.NoError(t, err)
	require.Equal(t, int32(-16), got)
	got, _, err = vectorValue(ctx, int16Vec, 0, api.IcebergType{Kind: api.TypeLong}, loc)
	require.NoError(t, err)
	require.Equal(t, int64(-16), got)

	int32Vec := vector.NewVec(types.T_int32.ToType())
	defer int32Vec.Free(mp)
	require.NoError(t, vector.AppendFixed(int32Vec, int32(32), false, mp))
	got, _, err = vectorValue(ctx, int32Vec, 0, api.IcebergType{Kind: api.TypeLong}, loc)
	require.NoError(t, err)
	require.Equal(t, int64(32), got)

	floatVec := vector.NewVec(types.T_float32.ToType())
	defer floatVec.Free(mp)
	require.NoError(t, vector.AppendFixed(floatVec, float32(1.25), false, mp))
	got, _, err = vectorValue(ctx, floatVec, 0, api.IcebergType{Kind: api.TypeFloat}, loc)
	require.NoError(t, err)
	require.Equal(t, float32(1.25), got)

	doubleVec := vector.NewVec(types.T_float64.ToType())
	defer doubleVec.Free(mp)
	require.NoError(t, vector.AppendFixed(doubleVec, float64(2.5), false, mp))
	got, _, err = vectorValue(ctx, doubleVec, 0, api.IcebergType{Kind: api.TypeDouble}, loc)
	require.NoError(t, err)
	require.Equal(t, float64(2.5), got)

	textVec := vector.NewVec(types.T_text.ToType())
	defer textVec.Free(mp)
	require.NoError(t, vector.AppendBytes(textVec, []byte("ksa"), false, mp))
	got, _, err = vectorValue(ctx, textVec, 0, api.IcebergType{Kind: api.TypeString}, loc)
	require.NoError(t, err)
	require.Equal(t, "ksa", got)

	nullVec := vector.NewVec(types.T_int32.ToType())
	defer nullVec.Free(mp)
	require.NoError(t, vector.AppendFixed(nullVec, int32(0), true, mp))
	got, isNull, err = vectorValue(ctx, nullVec, 0, api.IcebergType{Kind: api.TypeInt}, loc)
	require.NoError(t, err)
	require.True(t, isNull)
	require.Nil(t, got)

	_, _, err = vectorValue(ctx, boolVec, 0, api.IcebergType{Kind: api.TypeInt}, loc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "type mismatch")
	_, _, err = vectorValue(ctx, nil, 0, api.IcebergType{Kind: api.TypeInt}, loc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil vector")

	boolBound, cmp, err := encodeBound(ctx, api.IcebergType{Kind: api.TypeBoolean}, true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, boolBound)
	require.Equal(t, true, cmp)
	floatBound, cmp, err := encodeBound(ctx, api.IcebergType{Kind: api.TypeFloat}, float32(1.5))
	require.NoError(t, err)
	require.Equal(t, float64(1.5), cmp)
	require.Equal(t, float32(1.5), math.Float32frombits(binary.LittleEndian.Uint32(floatBound)))
	_, _, err = encodeBound(ctx, api.IcebergType{Kind: api.TypeBinary}, []byte("x"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "ICEBERG_UNSUPPORTED_FEATURE")

	require.Equal(t, true, decodeMetricValue(api.IcebergType{Kind: api.TypeBoolean}, []byte{1}))
	require.Equal(t, int64(-7), decodeMetricValue(api.IcebergType{Kind: api.TypeDecimal}, decimalInt64BoundBytes(-7)))
	require.Equal(t, float64(1.5), decodeMetricValue(api.IcebergType{Kind: api.TypeFloat}, floatBound))
	require.Equal(t, "abc", decodeMetricValue(api.IcebergType{Kind: api.TypeString}, []byte("abc")))
	require.Nil(t, decodeMetricValue(api.IcebergType{Kind: api.TypeBinary}, []byte{1}))
	require.Equal(t, -1, compareMetricValue(false, true))
	require.Equal(t, 1, compareMetricValue(int64(2), int64(1)))
	require.Equal(t, -1, compareMetricValue(float64(1), float64(2)))
	require.Equal(t, 1, compareMetricValue("b", "a"))
	require.Equal(t, 0, compareMetricValue(struct{}{}, struct{}{}))

	require.True(t, isNaN(float32(math.NaN())))
	require.True(t, isNaN(math.NaN()))
	require.False(t, isNaN("nan"))
	require.Equal(t, int64(1), estimatedValueSize(nil))
	require.Equal(t, int64(4), estimatedValueSize(int32(1)))
	require.Equal(t, int64(8), estimatedValueSize(int64(1)))
	require.Equal(t, int64(3), estimatedValueSize("ksa"))
	require.Equal(t, int64(3), estimatedValueSize(struct{ A int }{A: 7}))
}

func TestParquetWriterPartitionTransformsAndFormatting(t *testing.T) {
	ctx := context.Background()
	dateValue := int32(types.DateFromCalendar(2026, 7, 6) - types.DateFromCalendar(1970, 1, 1))
	year, err := transformPartitionValue(ctx, api.IcebergType{Kind: api.TypeDate}, "year", dateValue)
	require.NoError(t, err)
	require.Equal(t, int32(56), year)
	month, err := transformPartitionValue(ctx, api.IcebergType{Kind: api.TypeDate}, "month", dateValue)
	require.NoError(t, err)
	require.Equal(t, int32(678), month)
	day, err := transformPartitionValue(ctx, api.IcebergType{Kind: api.TypeDate}, "day", dateValue)
	require.NoError(t, err)
	require.Equal(t, dateValue, day)
	hour, err := transformPartitionValue(ctx, api.IcebergType{Kind: api.TypeTimestamp}, "hour", time.Date(2026, 7, 6, 9, 0, 0, 0, time.UTC).UnixMicro())
	require.NoError(t, err)
	require.Equal(t, int32(time.Date(2026, 7, 6, 9, 0, 0, 0, time.UTC).Unix()/3600), hour)
	identity, err := transformPartitionValue(ctx, api.IcebergType{Kind: api.TypeString}, "identity", "ksa")
	require.NoError(t, err)
	require.Equal(t, "ksa", identity)
	nilValue, err := transformPartitionValue(ctx, api.IcebergType{Kind: api.TypeString}, "identity", nil)
	require.NoError(t, err)
	require.Nil(t, nilValue)
	_, err = transformPartitionValue(ctx, api.IcebergType{Kind: api.TypeString}, "bucket", "ksa")
	require.Error(t, err)
	_, err = transformTemporal(ctx, "bucket", time.Unix(0, 0))
	require.Error(t, err)

	require.Equal(t, "null", partitionValueString(nil))
	require.Equal(t, "7", partitionValueString(int(7)))
	require.Equal(t, "true", partitionValueString(true))
	require.Equal(t, "ksa", partitionValueString("ksa"))
	require.Equal(t, "{7}", partitionValueString(struct{ N int }{N: 7}))
	spec := api.PartitionSpec{Fields: []api.PartitionField{
		{Name: "region"},
		{Name: "bucket"},
	}}
	require.NotEmpty(t, partitionKey(map[string]any{"region": "ksa", "bucket": int32(7)}, spec))
	require.Equal(t, "", partitionKey(nil, api.PartitionSpec{}))
	require.NotEqual(t,
		partitionKey(map[string]any{"region": nil, "bucket": int32(7)}, spec),
		partitionKey(map[string]any{"region": "null", "bucket": int32(7)}, spec),
	)
	require.NotEqual(t,
		partitionKey(map[string]any{"region": "x/bucket=y", "bucket": "z"}, spec),
		partitionKey(map[string]any{"region": "x", "bucket": "y/bucket=z"}, spec),
	)
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

func TestFanoutParquetDataWriterAbortsSinkWhenWriterInitializationFails(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	bat := batch.New([]string{"unsupported"})
	vec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed[int64](vec, 1, false, mp))
	bat.Vecs[0] = vec
	bat.SetRowCount(1)
	defer bat.Clean(mp)

	sink := &abortTrackingDataFileSink{}
	writer, err := NewFanoutParquetDataWriter(ctx, FanoutWriterConfig{
		Schema: api.Schema{Fields: []api.SchemaField{{
			ID: 1, Name: "unsupported", Type: api.IcebergType{Kind: api.TypeStruct},
		}}},
		TableLocation: "s3://warehouse/accounts",
	}, DataFileOutputFactoryFunc(func(context.Context, string) (io.WriteCloser, error) {
		return sink, nil
	}))
	require.NoError(t, err)
	require.Error(t, writer.WriteBatch(ctx, bat.Attrs, bat))
	require.True(t, sink.aborted)
	require.False(t, sink.closed, "abort-capable sinks must not be finalized on initialization failure")
	require.Empty(t, writer.OrphanPaths(), "successful abort guarantees no object was published")
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

type abortTrackingDataFileSink struct {
	bytes.Buffer
	aborted bool
	closed  bool
}

func (s *abortTrackingDataFileSink) Close() error {
	s.closed = true
	return nil
}

func (s *abortTrackingDataFileSink) Abort() error {
	s.aborted = true
	s.Reset()
	return nil
}
