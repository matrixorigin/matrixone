// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package python

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestArrowFieldMetadataIsExact(t *testing.T) {
	descriptor, err := NewTypeDescriptor(types.T_varchar.ToType())
	require.NoError(t, err)
	field, err := descriptor.Field("result")
	require.NoError(t, err)
	require.Equal(t, []string{TypeMetadataKey, TypeFingerprintKey}, field.Metadata.Keys())
	require.NoError(t, descriptor.ValidateField(field))

	withExtra := field
	withExtra.Metadata = arrow.MetadataFrom(map[string]string{
		TypeMetadataKey:    field.Metadata.ToMap()[TypeMetadataKey],
		TypeFingerprintKey: field.Metadata.ToMap()[TypeFingerprintKey],
		"untrusted":        "must be rejected",
	})
	require.Error(t, descriptor.ValidateField(withExtra))
}

func TestEncodeRecordBatchUsesFlightPayloadFrames(t *testing.T) {
	descriptor, err := NewTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	field, err := descriptor.Field("arg_0")
	require.NoError(t, err)
	builder := array.NewInt64Builder(memory.NewGoAllocator())
	builder.AppendValues([]int64{1, 2, 3}, nil)
	values := builder.NewInt64Array()
	defer values.Release()
	record := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{values}, 3)
	defer record.Release()

	frames, err := EncodeRecordBatch(record, DefaultMaxBatchBytes)
	require.NoError(t, err)
	require.Len(t, frames, 2)
	// FlightData.DataHeader is the raw IPC FlatBuffer, without the stream
	// continuation prefix. DecodeRecordBatch adds that prefix back only for
	// the local bounded decoder.
	require.NotEqual(t, []byte{0xff, 0xff, 0xff, 0xff}, frames[0].Header[:4])
	decoded, err := DecodeRecordBatch(frames[0], frames[1], DefaultMaxBatchBytes)
	require.NoError(t, err)
	defer decoded.Release()
	require.Equal(t, int64(3), decoded.NumRows())
	decodedValues := decoded.Column(0).(*array.Int64)
	require.Equal(t, int64(1), decodedValues.Value(0))
	require.Equal(t, int64(2), decodedValues.Value(1))
	require.Equal(t, int64(3), decodedValues.Value(2))
}

func TestDecodeRecordBatchRequiresSchemaAndRecordHeaders(t *testing.T) {
	descriptor, err := NewTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	field, err := descriptor.Field("arg_0")
	require.NoError(t, err)
	builder := array.NewInt64Builder(memory.NewGoAllocator())
	builder.Append(1)
	values := builder.NewInt64Array()
	defer values.Release()
	record := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{values}, 1)
	defer record.Release()
	frames, err := EncodeRecordBatch(record, DefaultMaxBatchBytes)
	require.NoError(t, err)

	_, err = DecodeRecordBatch(ArrowFrame{Header: frames[1].Header}, frames[1], DefaultMaxBatchBytes)
	require.Error(t, err)
	_, err = DecodeRecordBatch(frames[0], frames[0], DefaultMaxBatchBytes)
	require.ErrorContains(t, err, "record frame header")
	_, err = DecodeRecordBatch(ArrowFrame{Header: frames[0].Header, Body: []byte{1}}, frames[1], DefaultMaxBatchBytes)
	require.ErrorContains(t, err, "schema frame body")
}

func TestAppendArrowResultRejectsMissingConsumer(t *testing.T) {
	builder := array.NewInt64Builder(memory.NewGoAllocator())
	builder.Append(1)
	values := builder.NewInt64Array()
	defer values.Release()
	descriptor, err := NewTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	require.ErrorContains(t, AppendArrowResult(descriptor, values, nil, nil), "missing result wrapper")
}

func TestAppendArrowResultValidatesEmptyArrayType(t *testing.T) {
	allocator := memory.NewGoAllocator()
	stringBuilder := array.NewStringBuilder(allocator)
	values := stringBuilder.NewStringArray()
	defer values.Release()

	mp := mpool.MustNewZeroNoFixed()
	result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
	defer func() {
		result.Free()
		mpool.DeleteMPool(mp)
	}()
	descriptor, err := NewTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)

	require.ErrorContains(t, AppendArrowResult(descriptor, values, result, mp), "Arrow type")
}

func TestZeroArgumentRecordPreservesRows(t *testing.T) {
	record := array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 3)
	defer record.Release()
	frames, err := EncodeRecordBatch(record, DefaultMaxBatchBytes)
	require.NoError(t, err)
	require.Len(t, frames, 2)
	decoded, err := DecodeRecordBatch(frames[0], frames[1], DefaultMaxBatchBytes)
	require.NoError(t, err)
	defer decoded.Release()
	require.Equal(t, int64(3), decoded.NumRows())
	require.Equal(t, int64(0), decoded.NumCols())
}

func TestArrowValueDomainRejectsWidthAndTimeOverflow(t *testing.T) {
	allocator := memory.NewGoAllocator()
	stringBuilder := array.NewStringBuilder(allocator)
	stringBuilder.Append("abcd")
	strings := stringBuilder.NewStringArray()
	defer strings.Release()
	stringDescriptor := TypeDescriptor{TypeID: int32(types.T_varchar), Width: 3, OffsetWidth: 32}
	require.ErrorContains(t, validateArrowValueDomain(stringDescriptor, strings), "exceeds width")

	timeBuilder := array.NewDurationBuilder(allocator, &arrow.DurationType{Unit: arrow.Microsecond})
	timeBuilder.Append(arrow.Duration(types.MySQLTimeMax + 1))
	times := timeBuilder.NewDurationArray()
	defer times.Release()
	timeDescriptor := TypeDescriptor{TypeID: int32(types.T_time), Scale: 6, OffsetWidth: 32}
	require.ErrorContains(t, validateArrowValueDomain(timeDescriptor, times), "outside the declared SQL domain")
}

func TestArrowTypeValidationIncludesFixedBinaryWidth(t *testing.T) {
	descriptor, err := NewTypeDescriptor(types.T_uuid.ToType())
	require.NoError(t, err)
	fingerprint, err := descriptor.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, "370e2939c178acee55aaae27a7bcf92937a203e2788e9e0140d84884be042780", fingerprint)
	builder := array.NewFixedSizeBinaryBuilder(memory.NewGoAllocator(), &arrow.FixedSizeBinaryType{ByteWidth: 1})
	builder.Append([]byte{0xff})
	values := builder.NewFixedSizeBinaryArray()
	defer values.Release()
	require.ErrorContains(t, validateArrayType(descriptor, values), "does not match")

	field, err := descriptor.Field("uuid")
	require.NoError(t, err)
	field.Type = &arrow.FixedSizeBinaryType{ByteWidth: 1}
	require.ErrorContains(t, descriptor.ValidateField(field), "does not match")
}

func TestArrowValueDomainRejectsDecimalOverflowAndNullableVectorChild(t *testing.T) {
	decimalType := &arrow.Decimal128Type{Precision: 3, Scale: 0}
	decimalBuilder := array.NewDecimal128Builder(memory.NewGoAllocator(), decimalType)
	decimalBuilder.Append(decimal128.FromI64(1000))
	decimals := decimalBuilder.NewDecimal128Array()
	defer decimals.Release()
	decimalDescriptor := TypeDescriptor{TypeID: int32(types.T_decimal128), Width: 3, OffsetWidth: 32}
	require.ErrorContains(t, validateArrowValueDomain(decimalDescriptor, decimals), "exceeds precision")

	listBuilder := array.NewFixedSizeListBuilder(memory.NewGoAllocator(), 2, arrow.PrimitiveTypes.Float32)
	listBuilder.Append(true)
	listBuilder.ValueBuilder().(*array.Float32Builder).Append(1)
	listBuilder.ValueBuilder().(*array.Float32Builder).AppendNull()
	lists := listBuilder.NewArray()
	defer lists.Release()
	vectorDescriptor := TypeDescriptor{TypeID: int32(types.T_array_float32), Width: 2}
	require.ErrorContains(t, validateArrowValueDomain(vectorDescriptor, lists), "vector child")
}

func TestArrowStringWidthCountsUnicodeCharacters(t *testing.T) {
	builder := array.NewStringBuilder(memory.NewGoAllocator())
	builder.Append("中")
	values := builder.NewStringArray()
	defer values.Release()
	descriptor := TypeDescriptor{TypeID: int32(types.T_varchar), Width: 1, OffsetWidth: 32}
	require.NoError(t, validateArrowValueDomain(descriptor, values))
}

func TestAppendArrowResultIgnoresNullDecimalPayload(t *testing.T) {
	decimalType := &arrow.Decimal128Type{Precision: 18, Scale: 0}
	validity := memory.NewBufferBytes([]byte{0})
	values := memory.NewBufferBytes(make([]byte, 16))
	// The physical coefficient of a NULL slot is unspecified. Make it a
	// value that would not fit Decimal64 to ensure conversion never observes
	// the payload after the validity check.
	binary.LittleEndian.PutUint64(values.Bytes()[8:], 1)
	data := array.NewData(decimalType, 1, []*memory.Buffer{validity, values}, nil, 1, 0)
	decimals := array.NewDecimal128Data(data)
	data.Release()
	defer decimals.Release()
	defer validity.Release()
	defer values.Release()

	mp := mpool.MustNewZeroNoFixed()
	result := vector.NewFunctionResultWrapper(types.T_decimal64.ToType(), mp)
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))

	descriptor := TypeDescriptor{TypeID: int32(types.T_decimal64), Width: 18, OffsetWidth: 32}
	require.NoError(t, AppendArrowResult(descriptor, decimals, result, mp))
	require.True(t, result.GetResultVector().IsNull(0))
}

func TestDecimal128ToDecimal64ChecksSignedRange(t *testing.T) {
	max, err := decimalFromArray(decimal128.New(0, math.MaxInt64), true)
	require.NoError(t, err)
	require.Equal(t, types.Decimal64(math.MaxInt64), max)

	min, err := decimalFromArray(decimal128.New(-1, uint64(1)<<63), true)
	require.NoError(t, err)
	require.Equal(t, types.Decimal64(uint64(1)<<63), min)

	_, err = decimalFromArray(decimal128.New(0, uint64(1)<<63), true)
	require.ErrorContains(t, err, "does not fit Decimal64")
	_, err = decimalFromArray(decimal128.New(-1, uint64(1)<<63-1), true)
	require.ErrorContains(t, err, "does not fit Decimal64")
}
