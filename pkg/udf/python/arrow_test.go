// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package python

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
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

func BenchmarkDecodeRecordBatch(b *testing.B) {
	descriptor, err := NewTypeDescriptor(types.T_int64.ToType())
	require.NoError(b, err)
	field, err := descriptor.Field("arg_0")
	require.NoError(b, err)
	builder := array.NewInt64Builder(memory.NewGoAllocator())
	values := make([]int64, 8192)
	for i := range values {
		values[i] = int64(i)
	}
	builder.AppendValues(values, nil)
	arrayValues := builder.NewInt64Array()
	defer arrayValues.Release()
	record := array.NewRecordBatch(
		arrow.NewSchema([]arrow.Field{field}, nil),
		[]arrow.Array{arrayValues},
		int64(len(values)),
	)
	defer record.Release()
	frames, err := EncodeRecordBatch(record, DefaultMaxBatchBytes)
	require.NoError(b, err)
	b.SetBytes(int64(len(frames[1].Body)))

	b.Run("copy_baseline", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			decoded, err := decodeRecordBatchCopyBaseline(frames[0], frames[1], DefaultMaxBatchBytes)
			if err != nil {
				b.Fatal(err)
			}
			decoded.Release()
		}
	})
	b.Run("snapshot_reader", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			decoded, err := DecodeRecordBatch(frames[0], frames[1], DefaultMaxBatchBytes)
			if err != nil {
				b.Fatal(err)
			}
			decoded.Release()
		}
	})
}

func BenchmarkAppendArrowResultFixedWidth(b *testing.B) {
	builder := array.NewInt64Builder(memory.NewGoAllocator())
	values := make([]int64, 8192)
	for i := range values {
		values[i] = int64(i)
	}
	builder.AppendValues(values, nil)
	input := builder.NewInt64Array()
	defer input.Release()
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)
	b.SetBytes(int64(len(values) * 8))

	bench := func(b *testing.B, appendResult func(vector.FunctionResultWrapper) error) {
		result := vector.NewFunctionResultWrapper(types.T_int64.ToType(), mp)
		defer result.Free()
		require.NoError(b, result.PreExtendAndReset(len(values)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := result.PreExtendAndReset(len(values)); err != nil {
				b.Fatal(err)
			}
			if err := appendResult(result); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("wrapper_each_row", func(b *testing.B) {
		bench(b, func(result vector.FunctionResultWrapper) error {
			for i := 0; i < input.Len(); i++ {
				if err := vector.MustFunctionResult[int64](result).Append(input.Value(i), input.IsNull(i)); err != nil {
					return err
				}
			}
			return nil
		})
	})
	b.Run("wrapper_once", func(b *testing.B) {
		bench(b, func(result vector.FunctionResultWrapper) error {
			return appendFixedArrowValues(input.Int64Values(), input, result)
		})
	})
}

func BenchmarkAppendArrowResultVariableWidth(b *testing.B) {
	builder := array.NewStringBuilder(memory.NewGoAllocator())
	values := make([]string, 8192)
	for i := range values {
		values[i] = fmt.Sprintf("value-%d", i)
	}
	builder.AppendValues(values, nil)
	input := builder.NewStringArray()
	defer input.Release()
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)
	b.SetBytes(int64(len(values) * 8))

	bench := func(b *testing.B, appendResult func(vector.FunctionResultWrapper) error) {
		result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), mp)
		defer result.Free()
		require.NoError(b, result.PreExtendAndReset(len(values)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := result.PreExtendAndReset(len(values)); err != nil {
				b.Fatal(err)
			}
			if err := appendResult(result); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("string_to_bytes_copy", func(b *testing.B) {
		bench(b, func(result vector.FunctionResultWrapper) error {
			output := vector.MustFunctionResult[types.Varlena](result)
			for i := 0; i < input.Len(); i++ {
				if err := output.AppendBytes([]byte(input.Value(i)), input.IsNull(i)); err != nil {
					return err
				}
			}
			return nil
		})
	})
	b.Run("borrowed_string_view", func(b *testing.B) {
		bench(b, func(result vector.FunctionResultWrapper) error {
			return appendStringArrowResult(input, result)
		})
	})
}

func decodeRecordBatchCopyBaseline(schemaFrame, batchFrame ArrowFrame, maxBytes int64) (arrow.RecordBatch, error) {
	stream := make([]byte, 0, len(schemaFrame.Header)+len(batchFrame.Header)+len(batchFrame.Body)+32)
	stream = appendIPCFrameCopyBaseline(stream, schemaFrame.Header, schemaFrame.Body)
	stream = appendIPCFrameCopyBaseline(stream, batchFrame.Header, batchFrame.Body)
	stream = append(stream, 0, 0, 0, 0, 0, 0, 0, 0)
	reader, err := ipc.NewReader(bytes.NewReader(stream), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	if !reader.Next() {
		return nil, reader.Err()
	}
	record := reader.RecordBatch()
	if record == nil {
		return nil, fmt.Errorf("missing record batch")
	}
	record.Retain()
	return record, nil
}

func appendIPCFrameCopyBaseline(dst, header, body []byte) []byte {
	metadataLength := (len(header) + 7) &^ 7
	var prefix [8]byte
	binary.LittleEndian.PutUint32(prefix[:4], math.MaxUint32)
	binary.LittleEndian.PutUint32(prefix[4:], uint32(metadataLength))
	dst = append(dst, prefix[:]...)
	dst = append(dst, header...)
	for len(dst)%8 != 0 {
		dst = append(dst, 0)
	}
	dst = append(dst, body...)
	for len(dst)%8 != 0 {
		dst = append(dst, 0)
	}
	return dst
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

func TestBuildInputRecordRejectsEmptyConstantVector(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)

	input := vector.NewConstNull(types.T_int64.ToType(), 0, mp)
	defer input.Free(mp)

	_, _, err := BuildInputRecordRange(
		[]*vector.Vector{input},
		[]types.Type{types.T_int64.ToType()},
		0,
		1,
	)
	require.ErrorContains(t, err, "shorter than batch range")

	textInput, err := vector.NewConstBytes(types.T_varchar.ToType(), nil, 0, mp)
	require.NoError(t, err)
	defer textInput.Free(mp)
	var record arrow.RecordBatch
	require.NotPanics(t, func() {
		record, _, err = BuildInputRecordRange(
			[]*vector.Vector{textInput},
			[]types.Type{types.T_varchar.ToType()},
			0,
			1,
		)
	})
	if record != nil {
		record.Release()
	}
	require.ErrorContains(t, err, "shorter than batch range")
}

func TestInputBatchEncoderRejectsMismatchedVectorType(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	input := vector.NewVec(types.T_int32.ToType())
	defer func() {
		input.Free(mp)
		mpool.DeleteMPool(mp)
	}()

	_, err := newInputBatchEncoder(
		[]*vector.Vector{input}, []types.Type{types.T_int64.ToType()},
	)
	require.ErrorContains(t, err, "does not match the frozen argument type")
}

func TestInputBatchEncoderReusesSchemaAndPreservesRange(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	input := vector.NewVec(types.T_int64.ToType())
	defer func() {
		input.Free(mp)
		mpool.DeleteMPool(mp)
	}()
	for _, value := range []int64{10, 20, 30} {
		require.NoError(t, vector.AppendFixed(input, value, false, mp))
	}
	encoder, err := newInputBatchEncoder(
		[]*vector.Vector{input}, []types.Type{types.T_int64.ToType()},
	)
	require.NoError(t, err)
	first, firstSchema, err := encoder.build(0, 2)
	require.NoError(t, err)
	defer first.Release()
	second, secondSchema, err := encoder.build(2, 1)
	require.NoError(t, err)
	defer second.Release()
	require.Same(t, firstSchema, secondSchema)
	firstValues := first.Column(0).(*array.Int64)
	secondValues := second.Column(0).(*array.Int64)
	require.Equal(t, int64(10), firstValues.Value(0))
	require.Equal(t, int64(20), firstValues.Value(1))
	require.Equal(t, int64(30), secondValues.Value(0))
}

func TestInputBatchEncoderCachesVariableWidthParameterAcrossRanges(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)

	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(mp)
	for index, value := range []string{"zero", "one", "two", "three"} {
		require.NoError(t, vector.AppendBytes(input, []byte(value), index == 2, mp))
	}
	encoder, err := newInputBatchEncoder(
		[]*vector.Vector{input}, []types.Type{types.T_varchar.ToType()},
	)
	require.NoError(t, err)

	first, _, err := encoder.build(0, 2)
	require.NoError(t, err)
	defer first.Release()
	second, _, err := encoder.build(2, 2)
	require.NoError(t, err)
	defer second.Release()

	firstValues := first.Column(0).(*array.String)
	require.Equal(t, "zero", firstValues.Value(0))
	require.Equal(t, "one", firstValues.Value(1))
	secondValues := second.Column(0).(*array.String)
	require.True(t, secondValues.IsNull(0))
	require.Equal(t, "three", secondValues.Value(1))
}

func TestInputBatchEncoderBulkPrimitivePreservesRangeNullsAndConstants(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)

	input := vector.NewVec(types.T_int64.ToType())
	defer input.Free(mp)
	for row := 0; row < 128; row++ {
		require.NoError(t, vector.AppendFixed(input, int64(row+100), row%19 == 0, mp))
	}
	encoder, err := newInputBatchEncoder(
		[]*vector.Vector{input}, []types.Type{types.T_int64.ToType()},
	)
	require.NoError(t, err)
	record, _, err := encoder.build(7, 100)
	require.NoError(t, err)
	defer record.Release()
	values := record.Column(0).(*array.Int64)
	for row := 0; row < 100; row++ {
		sourceRow := row + 7
		require.Equal(t, sourceRow%19 == 0, values.IsNull(row), "row %d", row)
		if !values.IsNull(row) {
			require.Equal(t, int64(sourceRow+100), values.Value(row), "row %d", row)
		}
	}

	constant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(42), 128, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	constantEncoder, err := newInputBatchEncoder(
		[]*vector.Vector{constant}, []types.Type{types.T_int64.ToType()},
	)
	require.NoError(t, err)
	constantRecord, _, err := constantEncoder.build(32, 96)
	require.NoError(t, err)
	defer constantRecord.Release()
	constantValues := constantRecord.Column(0).(*array.Int64)
	require.Equal(t, 96, int(constantRecord.NumRows()))
	for row := 0; row < 96; row++ {
		require.False(t, constantValues.IsNull(row))
		require.Equal(t, int64(42), constantValues.Value(row))
	}

	nullConstant := vector.NewConstNull(types.T_int64.ToType(), 128, mp)
	defer nullConstant.Free(mp)
	nullConstantEncoder, err := newInputBatchEncoder(
		[]*vector.Vector{nullConstant}, []types.Type{types.T_int64.ToType()},
	)
	require.NoError(t, err)
	nullConstantRecord, _, err := nullConstantEncoder.build(0, 96)
	require.NoError(t, err)
	defer nullConstantRecord.Release()
	nullConstantValues := nullConstantRecord.Column(0).(*array.Int64)
	for row := 0; row < 96; row++ {
		require.True(t, nullConstantValues.IsNull(row))
	}
}

func TestInputBatchEncoderVectorSizeProbeDoesNotUseVarlenaWrapper(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)

	for _, tc := range []struct {
		name string
		typ  types.Type
	}{
		{name: "vecf32", typ: types.New(types.T_array_float32, 3, 0)},
		{name: "vecf64", typ: types.New(types.T_array_float64, 3, 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := vector.NewVec(tc.typ)
			defer input.Free(mp)
			for row := 0; row < 64; row++ {
				var err error
				switch tc.typ.Oid {
				case types.T_array_float32:
					err = vector.AppendArray(input, []float32{float32(row), 1, 2}, row%11 == 0, mp)
				case types.T_array_float64:
					err = vector.AppendArray(input, []float64{float64(row), 1, 2}, row%11 == 0, mp)
				}
				require.NoError(t, err)
			}

			encoder, err := newInputBatchEncoder([]*vector.Vector{input}, []types.Type{tc.typ})
			require.NoError(t, err)
			defer func() { require.NoError(t, encoder.close()) }()

			var batch encodedRecordBatch
			require.NotPanics(t, func() {
				batch, err = encodeInputBatchWithEncoder(
					encoder, 0, 64, DefaultMaxBatchBytes, 64,
				)
			})
			require.NoError(t, err)
			require.Equal(t, int64(64), batch.Rows)
			require.Len(t, batch.Frames, 2)
		})
	}
}

func TestInputBatchWireEncoderReusesSchemaAcrossBatches(t *testing.T) {
	descriptor, err := NewTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	field, err := descriptor.Field("arg_0")
	require.NoError(t, err)
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	makeRecord := func(value int64) arrow.RecordBatch {
		builder := array.NewInt64Builder(memory.NewGoAllocator())
		builder.Append(value)
		values := builder.NewInt64Array()
		record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
		values.Release()
		return record
	}

	encoder := &inputBatchWireEncoder{schema: schema}
	first := makeRecord(1)
	second := makeRecord(2)
	defer first.Release()
	defer second.Release()
	defer func() { require.NoError(t, encoder.close()) }()

	firstFrames, err := encoder.encode(first, DefaultMaxBatchBytes)
	require.NoError(t, err)
	secondFrames, err := encoder.encode(second, DefaultMaxBatchBytes)
	require.NoError(t, err)
	require.Len(t, firstFrames, 2)
	require.Len(t, secondFrames, 2)
	require.Equal(t, firstFrames[0].Header, secondFrames[0].Header)
	require.Equal(t, firstFrames[0].Body, secondFrames[0].Body)
	require.NotEqual(t, firstFrames[1].Body, secondFrames[1].Body)
}

func TestInputBatchWireEncoderRecoversAfterOversizeCandidate(t *testing.T) {
	descriptor, err := NewTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	field, err := descriptor.Field("arg_0")
	require.NoError(t, err)
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	makeRecord := func(values ...int64) arrow.RecordBatch {
		builder := array.NewInt64Builder(memory.NewGoAllocator())
		builder.AppendValues(values, nil)
		arrayValues := builder.NewInt64Array()
		record := array.NewRecordBatch(schema, []arrow.Array{arrayValues}, int64(len(values)))
		arrayValues.Release()
		return record
	}

	one := makeRecord(1)
	defer one.Release()
	measure := &inputBatchWireEncoder{schema: schema}
	oneFrames, err := measure.encode(one, DefaultMaxBatchBytes)
	require.NoError(t, err)
	maxBytes := int64(len(oneFrames[0].Header) + len(oneFrames[0].Body) + len(oneFrames[1].Header) + len(oneFrames[1].Body))
	require.NoError(t, measure.close())

	encoder := &inputBatchWireEncoder{schema: schema}
	defer func() { require.NoError(t, encoder.close()) }()
	wider := makeRecord(1, 2)
	defer wider.Release()
	_, err = encoder.encode(wider, maxBytes)
	require.ErrorIs(t, err, errArrowBatchTooLarge)
	frames, err := encoder.encode(one, maxBytes)
	require.NoError(t, err)
	require.Len(t, frames, 2)
}

func TestInputBatchWireEncoderCloseIsIdempotent(t *testing.T) {
	descriptor, err := NewTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	field, err := descriptor.Field("arg_0")
	require.NoError(t, err)
	schema := arrow.NewSchema([]arrow.Field{field}, nil)
	builder := array.NewInt64Builder(memory.NewGoAllocator())
	builder.AppendValues([]int64{1}, nil)
	values := builder.NewInt64Array()
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 1)
	values.Release()
	defer record.Release()

	encoder := &inputBatchWireEncoder{schema: schema}
	_, err = encoder.encode(record, DefaultMaxBatchBytes)
	require.NoError(t, err)
	require.NoError(t, encoder.close())
	require.NoError(t, encoder.close())
	_, err = encoder.encode(record, DefaultMaxBatchBytes)
	require.ErrorContains(t, err, "invalid Arrow record batch encoder")
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

func TestTypeDescriptorRejectsNonCanonicalDomain(t *testing.T) {
	invalid := []TypeDescriptor{
		{TypeID: int32(types.T_varchar), Width: -1, OffsetWidth: 32},
		{TypeID: int32(types.T_int64), OffsetWidth: 64},
		{TypeID: int32(types.T_int64)},
		{TypeID: int32(types.T_decimal64), Width: 3, Scale: 4, OffsetWidth: 32},
		{TypeID: int32(types.T_json), OffsetWidth: 32},
		{TypeID: int32(types.T_date), OffsetWidth: 32},
		{TypeID: int32(types.T_array_float32), Width: types.MaxArrayDimension + 1},
		{TypeID: int32(types.T_varchar), Width: 16, Charset: types.CharsetBinary, OffsetWidth: 32},
	}
	for _, descriptor := range invalid {
		_, err := descriptor.Field("value")
		require.Error(t, err, "%+v", descriptor)
	}
}

func TestTypeDescriptorRejectsUnusedDomainFields(t *testing.T) {
	invalid := []TypeDescriptor{
		{TypeID: int32(types.T_date), Width: 1, OffsetWidth: 32, TemporalEncoding: "sql_zero_struct"},
		{TypeID: int32(types.T_datetime), Width: 1, OffsetWidth: 32, TemporalEncoding: "sql_zero_struct"},
		{TypeID: int32(types.T_time), Width: 1, Scale: 6, OffsetWidth: 32},
		{TypeID: int32(types.T_varchar), Width: 16, Scale: 1, Charset: types.CharsetUTF8, OffsetWidth: 32},
		{TypeID: int32(types.T_varbinary), Width: 16, Scale: 1, Charset: types.CharsetBinary, OffsetWidth: 32},
	}
	for _, descriptor := range invalid {
		_, err := descriptor.Field("value")
		require.Error(t, err, "%+v", descriptor)
	}
}

func TestFloatDescriptorsRoundTripThroughArrowFieldContract(t *testing.T) {
	for _, typ := range []types.T{types.T_float32, types.T_float64} {
		descriptor, err := NewTypeDescriptor(typ.ToType())
		require.NoError(t, err)
		field, err := descriptor.Field("value")
		require.NoError(t, err)
		require.NoError(t, descriptor.ValidateField(field))
		require.Equal(t, typ, types.T(descriptor.TypeID))
	}
}

func TestNewTypeDescriptorNormalizesPlannerOnlySentinels(t *testing.T) {
	integer := types.T_int32.ToTypeWithScale(-1)
	integer.Width = 32
	descriptor, err := NewTypeDescriptor(integer)
	require.NoError(t, err)
	require.Equal(t, int32(0), descriptor.Width)
	require.Equal(t, int32(0), descriptor.Scale)

	binaryType := types.T_binary.ToTypeWithScale(-1)
	binaryType.Width = 8
	descriptor, err = NewTypeDescriptor(binaryType)
	require.NoError(t, err)
	require.Equal(t, int32(8), descriptor.Width)
	require.Equal(t, int32(0), descriptor.Scale)

	temporal := types.T_datetime.ToTypeWithScale(-1)
	descriptor, err = NewTypeDescriptor(temporal)
	require.NoError(t, err)
	require.Equal(t, int32(0), descriptor.Width)
	require.Equal(t, int32(0), descriptor.Scale)
}

func TestBuildInputRecordCanonicalizesJSONText(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	input := vector.NewVec(types.T_json.ToType())
	defer input.Free(mp)
	bj, err := types.ParseStringToByteJson(`{"a":1,"b":[true,null,"中"]}`)
	require.NoError(t, err)
	raw, err := bj.Marshal()
	require.NoError(t, err)
	require.NoError(t, vector.AppendBytes(input, raw, false, mp))

	record, _, err := BuildInputRecord([]*vector.Vector{input}, []types.Type{types.T_json.ToType()}, 1)
	require.NoError(t, err)
	defer record.Release()
	values := record.Column(0).(*array.String)
	require.Equal(t, `{"a":1,"b":[true,null,"中"]}`, values.Value(0))
}

func TestArrowValueDomainRequiresCanonicalJSONText(t *testing.T) {
	descriptor, err := NewTypeDescriptor(types.T_json.ToType())
	require.NoError(t, err)

	for _, test := range []struct {
		name  string
		value string
		valid bool
	}{
		{name: "canonical", value: `{"a":1,"b":[true,null]}`, valid: true},
		{name: "whitespace", value: ` { "a": 1, "b": [true, null] } `},
		{name: "invalid", value: `{"a":}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder := array.NewStringBuilder(memory.NewGoAllocator())
			builder.Append(test.value)
			values := builder.NewStringArray()
			defer values.Release()

			err := validateArrowValueDomain(descriptor, values)
			if test.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "JSON row 0")
			}
		})
	}
}

func TestGoPythonTypeDescriptorFixtures(t *testing.T) {
	data, err := os.ReadFile("testdata/type_descriptors.json")
	require.NoError(t, err)
	var fixtures []struct {
		Name        string         `json:"name"`
		Descriptor  TypeDescriptor `json:"descriptor"`
		Fingerprint string         `json:"fingerprint"`
	}
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.NotEmpty(t, fixtures)
	for _, fixture := range fixtures {
		require.NoError(t, fixture.Descriptor.Validate(), fixture.Name)
		fingerprint, err := fixture.Descriptor.Fingerprint()
		require.NoError(t, err, fixture.Name)
		require.Equal(t, fixture.Fingerprint, fingerprint, fixture.Name)
		field, err := fixture.Descriptor.Field("value")
		require.NoError(t, err, fixture.Name)
		require.NoError(t, fixture.Descriptor.ValidateField(field), fixture.Name)
	}
}

func TestBuildInputRecordCoversEverySupportedType(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)

	jsonValue, err := types.ParseStringToByteJson(`{"a":1,"b":[true,null]}`)
	require.NoError(t, err)
	jsonBytes, err := jsonValue.Marshal()
	require.NoError(t, err)

	cases := []struct {
		name   string
		typ    types.Type
		append func(*vector.Vector) error
	}{
		{"bool", types.T_bool.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, true, false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, false, true, mp)
		}},
		{"int8", types.T_int8.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, int8(-1), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, int8(0), true, mp)
		}},
		{"int16", types.T_int16.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, int16(-1), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, int16(0), true, mp)
		}},
		{"int32", types.T_int32.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, int32(-1), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, int32(0), true, mp)
		}},
		{"int64", types.T_int64.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, int64(-1), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, int64(0), true, mp)
		}},
		{"uint8", types.T_uint8.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, uint8(1), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, uint8(0), true, mp)
		}},
		{"uint16", types.T_uint16.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, uint16(1), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, uint16(0), true, mp)
		}},
		{"uint32", types.T_uint32.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, uint32(1), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, uint32(0), true, mp)
		}},
		{"uint64", types.T_uint64.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, uint64(1), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, uint64(0), true, mp)
		}},
		{"float32", types.T_float32.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, float32(1.25), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, float32(0), true, mp)
		}},
		{"float64", types.T_float64.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, float64(1.25), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, float64(0), true, mp)
		}},
		{"decimal64", types.New(types.T_decimal64, 18, 6), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, types.Decimal64(123456), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, types.Decimal64(0), true, mp)
		}},
		{"decimal128", types.New(types.T_decimal128, 38, 10), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, types.Decimal128{B0_63: 123456}, false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, types.Decimal128{}, true, mp)
		}},
		{"date", types.T_date.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, types.Date(1), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, types.Date(0), true, mp)
		}},
		{"time", types.New(types.T_time, 0, 6), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, types.Time(123456), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, types.Time(0), true, mp)
		}},
		{"datetime", types.New(types.T_datetime, 0, 6), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, types.Datetime(types.GetUnixEpochSecs()+1), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, types.Datetime(0), true, mp)
		}},
		{"timestamp", types.New(types.T_timestamp, 0, 6), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, types.Timestamp(types.GetUnixEpochSecs()+1), false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, types.Timestamp(0), true, mp)
		}},
		{"char", types.New(types.T_char, 16, 0), func(v *vector.Vector) error {
			if err := vector.AppendBytes(v, []byte("中"), false, mp); err != nil {
				return err
			}
			return vector.AppendBytes(v, nil, true, mp)
		}},
		{"varchar", types.New(types.T_varchar, 64, 0), func(v *vector.Vector) error {
			if err := vector.AppendBytes(v, []byte("matrixone"), false, mp); err != nil {
				return err
			}
			return vector.AppendBytes(v, nil, true, mp)
		}},
		{"text", types.T_text.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendBytes(v, []byte("matrixone"), false, mp); err != nil {
				return err
			}
			return vector.AppendBytes(v, nil, true, mp)
		}},
		{"json", types.T_json.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendBytes(v, jsonBytes, false, mp); err != nil {
				return err
			}
			return vector.AppendBytes(v, nil, true, mp)
		}},
		{"binary", types.New(types.T_binary, 8, 0), func(v *vector.Vector) error {
			if err := vector.AppendBytes(v, []byte{1, 2}, false, mp); err != nil {
				return err
			}
			return vector.AppendBytes(v, nil, true, mp)
		}},
		{"varbinary", types.New(types.T_varbinary, 8, 0), func(v *vector.Vector) error {
			if err := vector.AppendBytes(v, []byte{1, 2}, false, mp); err != nil {
				return err
			}
			return vector.AppendBytes(v, nil, true, mp)
		}},
		{"blob", types.T_blob.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendBytes(v, []byte{1, 2}, false, mp); err != nil {
				return err
			}
			return vector.AppendBytes(v, nil, true, mp)
		}},
		{"uuid", types.T_uuid.ToType(), func(v *vector.Vector) error {
			if err := vector.AppendFixed(v, types.Uuid{1, 2, 3}, false, mp); err != nil {
				return err
			}
			return vector.AppendFixed(v, types.Uuid{}, true, mp)
		}},
		{"vecf32", types.New(types.T_array_float32, 3, 0), func(v *vector.Vector) error {
			if err := vector.AppendArray(v, []float32{1, 2, 3}, false, mp); err != nil {
				return err
			}
			return vector.AppendArray[float32](v, nil, true, mp)
		}},
		{"vecf64", types.New(types.T_array_float64, 3, 0), func(v *vector.Vector) error {
			if err := vector.AppendArray(v, []float64{1, 2, 3}, false, mp); err != nil {
				return err
			}
			return vector.AppendArray[float64](v, nil, true, mp)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := vector.NewVec(tc.typ)
			require.NoError(t, tc.append(input))
			record, _, err := BuildInputRecord([]*vector.Vector{input}, []types.Type{tc.typ}, 2)
			require.NoError(t, err)
			defer record.Release()
			input.Free(mp)

			descriptor, err := NewTypeDescriptor(tc.typ)
			require.NoError(t, err)
			require.NoError(t, descriptor.ValidateField(record.Schema().Field(0)))
			require.Equal(t, int64(2), record.NumRows())
			require.False(t, record.Column(0).IsNull(0))
			require.True(t, record.Column(0).IsNull(1))
		})
	}
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
