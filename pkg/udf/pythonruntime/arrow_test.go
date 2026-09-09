// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package pythonruntime

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
