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

package arrowio

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/matrixorigin/matrixone/pkg/container/arrowipc"
	"github.com/matrixorigin/matrixone/pkg/container/arrowipc/ipcflatbuf"
	"github.com/stretchr/testify/require"
)

const (
	maxArrowSchemaFields          = arrowipc.MaxSchemaFields
	maxArrowSchemaDepth           = arrowipc.MaxSchemaDepth
	maxArrowSchemaMetadataEntries = arrowipc.MaxSchemaMetadataEntries
	maxArrowSchemaFeatures        = arrowipc.MaxSchemaFeatures
	maxArrowUnionTypeIDsPerField  = arrowipc.MaxUnionTypeIDsPerField
)

func TestIPCSchemaVectorCountRejectedBeforeArrowGoForFileAndStream(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	for _, container := range []Container{ContainerFile, ContainerStream} {
		t.Run(containerName(container), func(t *testing.T) {
			payload := makeEmptyIPC(t, container, schema)
			schemaTable := ipcSchemaTable(t, payload, container)
			setFlatbufferVectorLength(
				t, schemaTable, 6, uint32(maxArrowSchemaFields+1),
			)

			admission := new(testAdmission)
			reader, err := Open(
				context.Background(), writeMemoryFile(t, "arrow-schema-vector", payload),
				"arrow-schema-vector", int64(len(payload)), container, admission, Options{},
			)
			if reader != nil {
				require.NoError(t, reader.Close())
			}
			require.ErrorContains(t, err, "schema field vector count")
			require.Zero(t, admission.pending.Load())
			require.Zero(t, admission.active.Load())
		})
	}
}

func TestIPCSchemaFieldAndDepthLimits(t *testing.T) {
	t.Run("total field boundary", func(t *testing.T) {
		fields := make([]arrow.Field, maxArrowSchemaFields)
		for index := range fields {
			fields[index] = arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Int8}
		}
		assertEmptyIPCSchemaOpen(t, arrow.NewSchema(fields, nil))

		fields = append(fields, arrow.Field{Name: "overflow", Type: arrow.PrimitiveTypes.Int8})
		assertEmptyIPCSchemaError(t, arrow.NewSchema(fields, nil), "schema field count exceeds limit 4096")
	})

	t.Run("nesting depth boundary", func(t *testing.T) {
		assertEmptyIPCSchemaOpen(t, nestedListSchema(maxArrowSchemaDepth))
		assertEmptyIPCSchemaError(
			t, nestedListSchema(maxArrowSchemaDepth+1), "schema nesting depth 65 exceeds limit 64",
		)
	})
}

func TestIPCSchemaMetadataAndUnionLimits(t *testing.T) {
	t.Run("custom metadata total boundary", func(t *testing.T) {
		schemaMetadata := repeatedArrowMetadata(maxArrowSchemaMetadataEntries / 2)
		fieldMetadata := repeatedArrowMetadata(maxArrowSchemaMetadataEntries / 2)
		field := arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Metadata: fieldMetadata}
		assertEmptyIPCSchemaOpen(t, arrow.NewSchema([]arrow.Field{field}, &schemaMetadata))

		fieldMetadata = repeatedArrowMetadata(maxArrowSchemaMetadataEntries/2 + 1)
		field.Metadata = fieldMetadata
		assertEmptyIPCSchemaError(
			t, arrow.NewSchema([]arrow.Field{field}, &schemaMetadata),
			"schema custom metadata entry count exceeds limit 4096",
		)
	})

	t.Run("timestamp timezone alias amplification", func(t *testing.T) {
		metadata := makeFlatbufferSchemaWithSharedTimezone(t, 128, strings.Repeat("x", 4096))
		var schema ipcflatbuf.Schema
		schema.Init(metadata, flatbuffers.GetUOffsetT(metadata))
		require.ErrorContains(t,
			arrowipc.ValidateSchemaMetadata(context.Background(), &schema, len(metadata)),
			"schema decoded string bytes exceed metadata size",
		)
	})

	t.Run("union type id boundary", func(t *testing.T) {
		children := make([]arrow.Field, maxArrowUnionTypeIDsPerField)
		codes := make([]arrow.UnionTypeCode, maxArrowUnionTypeIDsPerField)
		for index := range children {
			children[index] = arrow.Field{
				Name: fmt.Sprintf("member_%d", index), Type: arrow.PrimitiveTypes.Int8,
			}
			codes[index] = arrow.UnionTypeCode(index)
		}
		schema := arrow.NewSchema([]arrow.Field{{
			Name: "union", Type: arrow.DenseUnionOf(children, codes),
		}}, nil)
		assertEmptyIPCSchemaOpen(t, schema)

		payload := makeEmptyIPC(t, ContainerStream, schema)
		schemaTable := ipcSchemaTable(t, payload, ContainerStream)
		fieldTable := firstFlatbufferSchemaField(t, schemaTable)
		unionTable := flatbufferFieldType(t, fieldTable)
		setFlatbufferVectorLength(
			t, unionTable, 6, uint32(maxArrowUnionTypeIDsPerField+1),
		)
		admission := new(testAdmission)
		reader, err := Open(
			context.Background(), writeMemoryFile(t, "arrow-union-vector", payload),
			"arrow-union-vector", int64(len(payload)), ContainerStream, admission, Options{},
		)
		if reader != nil {
			require.NoError(t, reader.Close())
		}
		require.ErrorContains(t, err, "union type ID count 129 exceeds per-field limit 128")
		require.Zero(t, admission.pending.Load())
		require.Zero(t, admission.active.Load())
	})

	t.Run("feature boundary", func(t *testing.T) {
		metadata := makeFlatbufferSchemaWithFeatures(t, maxArrowSchemaFeatures)
		var schema ipcflatbuf.Schema
		schema.Init(metadata, flatbuffers.GetUOffsetT(metadata))
		require.NoError(t, arrowipc.ValidateSchemaMetadata(context.Background(), &schema, len(metadata)))

		metadata = makeFlatbufferSchemaWithFeatures(t, maxArrowSchemaFeatures+1)
		schema.Init(metadata, flatbuffers.GetUOffsetT(metadata))
		require.ErrorContains(t,
			arrowipc.ValidateSchemaMetadata(context.Background(), &schema, len(metadata)),
			"schema feature count 65 exceeds limit 64",
		)
	})
}

func assertEmptyIPCSchemaOpen(t *testing.T, schema *arrow.Schema) {
	t.Helper()
	payload := makeEmptyIPC(t, ContainerStream, schema)
	admission := new(testAdmission)
	reader, err := Open(
		context.Background(), writeMemoryFile(t, "arrow-schema-valid", payload),
		"arrow-schema-valid", int64(len(payload)), ContainerStream, admission, Options{},
	)
	require.NoError(t, err)
	require.NotNil(t, reader.Schema())
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
	require.NoError(t, reader.Close())
	require.Zero(t, admission.pending.Load())
	require.Zero(t, admission.active.Load())
}

func assertEmptyIPCSchemaError(t *testing.T, schema *arrow.Schema, errorText string) {
	t.Helper()
	payload := makeEmptyIPC(t, ContainerStream, schema)
	admission := new(testAdmission)
	reader, err := Open(
		context.Background(), writeMemoryFile(t, "arrow-schema-invalid", payload),
		"arrow-schema-invalid", int64(len(payload)), ContainerStream, admission, Options{},
	)
	if reader != nil {
		require.NoError(t, reader.Close())
	}
	require.ErrorContains(t, err, errorText)
	require.Zero(t, admission.pending.Load())
	require.Zero(t, admission.active.Load())
}

func makeEmptyIPC(t testing.TB, container Container, schema *arrow.Schema) []byte {
	t.Helper()
	var output bytes.Buffer
	if container == ContainerFile {
		writer, err := ipc.NewFileWriter(&output, ipc.WithSchema(schema))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
	} else {
		writer := ipc.NewWriter(&output, ipc.WithSchema(schema))
		require.NoError(t, writer.Close())
	}
	return output.Bytes()
}

func nestedListSchema(depth int) *arrow.Schema {
	var dataType arrow.DataType = arrow.PrimitiveTypes.Int8
	for current := 1; current < depth; current++ {
		dataType = arrow.ListOf(dataType)
	}
	return arrow.NewSchema([]arrow.Field{{Name: "nested", Type: dataType}}, nil)
}

func repeatedArrowMetadata(count int) arrow.Metadata {
	keys := make([]string, count)
	values := make([]string, count)
	for index := range keys {
		keys[index] = "k"
	}
	return arrow.NewMetadata(keys, values)
}

func ipcSchemaTable(t testing.TB, payload []byte, container Container) flatbuffers.Table {
	t.Helper()
	if container == ContainerFile {
		tailStart := len(payload) - 4 - len(ipc.Magic)
		require.GreaterOrEqual(t, tailStart, 0)
		footerLength := int(binary.LittleEndian.Uint32(payload[tailStart:]))
		footerStart := tailStart - footerLength
		require.GreaterOrEqual(t, footerStart, len(ipc.Magic))
		footerBytes := payload[footerStart:tailStart]
		footer := flatbuffers.Table{Bytes: footerBytes, Pos: flatbuffers.GetUOffsetT(footerBytes)}
		schemaOffset := flatbuffers.UOffsetT(footer.Offset(6))
		require.NotZero(t, schemaOffset)
		return flatbuffers.Table{
			Bytes: footerBytes,
			Pos:   footer.Indirect(schemaOffset + footer.Pos),
		}
	}

	cursor := 0
	require.GreaterOrEqual(t, len(payload), 4)
	metadataLength := binary.LittleEndian.Uint32(payload[cursor:])
	cursor += 4
	if metadataLength == ipcContinuationToken {
		require.GreaterOrEqual(t, len(payload)-cursor, 4)
		metadataLength = binary.LittleEndian.Uint32(payload[cursor:])
		cursor += 4
	}
	require.GreaterOrEqual(t, len(payload)-cursor, int(metadataLength))
	metadata := payload[cursor : cursor+int(metadataLength)]
	message := flatbuffers.Table{Bytes: metadata, Pos: flatbuffers.GetUOffsetT(metadata)}
	headerTypeOffset := flatbuffers.UOffsetT(message.Offset(6))
	require.NotZero(t, headerTypeOffset)
	require.Equal(t, byte(ipc.MessageSchema), message.GetByte(headerTypeOffset+message.Pos))
	headerOffset := flatbuffers.UOffsetT(message.Offset(8))
	require.NotZero(t, headerOffset)
	var schema flatbuffers.Table
	message.Union(&schema, headerOffset)
	return schema
}

func firstFlatbufferSchemaField(t testing.TB, schema flatbuffers.Table) flatbuffers.Table {
	t.Helper()
	fieldsOffset := flatbuffers.UOffsetT(schema.Offset(6))
	require.NotZero(t, fieldsOffset)
	require.Positive(t, schema.VectorLen(fieldsOffset))
	position := schema.Vector(fieldsOffset)
	return flatbuffers.Table{Bytes: schema.Bytes, Pos: schema.Indirect(position)}
}

func flatbufferFieldType(t testing.TB, field flatbuffers.Table) flatbuffers.Table {
	t.Helper()
	typeOffset := flatbuffers.UOffsetT(field.Offset(10))
	require.NotZero(t, typeOffset)
	var dataType flatbuffers.Table
	field.Union(&dataType, typeOffset)
	return dataType
}

func setFlatbufferVectorLength(
	t testing.TB,
	table flatbuffers.Table,
	vtableOffset flatbuffers.VOffsetT,
	count uint32,
) {
	t.Helper()
	offset := flatbuffers.UOffsetT(table.Offset(vtableOffset))
	require.NotZero(t, offset)
	vectorOffset := offset + table.Pos
	require.GreaterOrEqual(t, len(table.Bytes)-int(vectorOffset), 4)
	vector := vectorOffset + flatbuffers.GetUOffsetT(table.Bytes[vectorOffset:])
	require.GreaterOrEqual(t, len(table.Bytes)-int(vector), 4)
	binary.LittleEndian.PutUint32(table.Bytes[vector:], count)
}

func makeFlatbufferSchemaWithFeatures(t testing.TB, count int) []byte {
	t.Helper()
	builder := flatbuffers.NewBuilder(128 + count*8)
	builder.StartVector(8, count, 8)
	for index := count - 1; index >= 0; index-- {
		builder.PrependInt64(int64(index))
	}
	features := builder.EndVector(count)
	builder.StartObject(4)
	builder.PrependUOffsetTSlot(3, features, 0)
	schema := builder.EndObject()
	builder.Finish(schema)
	return append([]byte(nil), builder.FinishedBytes()...)
}

func makeFlatbufferSchemaWithSharedTimezone(
	t testing.TB,
	fieldCount int,
	timezone string,
) []byte {
	t.Helper()
	builder := flatbuffers.NewBuilder(128 + fieldCount*32 + len(timezone))
	timezoneOffset := builder.CreateString(timezone)
	fields := make([]flatbuffers.UOffsetT, fieldCount)
	for index := range fields {
		builder.StartObject(2)
		builder.PrependUOffsetTSlot(1, timezoneOffset, 0)
		timestamp := builder.EndObject()

		builder.StartObject(7)
		builder.PrependByteSlot(2, byte(ipcflatbuf.TypeTimestamp), 0)
		builder.PrependUOffsetTSlot(3, timestamp, 0)
		fields[index] = builder.EndObject()
	}
	builder.StartVector(4, len(fields), 4)
	for index := len(fields) - 1; index >= 0; index-- {
		builder.PrependUOffsetT(fields[index])
	}
	fieldVector := builder.EndVector(len(fields))
	builder.StartObject(4)
	builder.PrependUOffsetTSlot(1, fieldVector, 0)
	schema := builder.EndObject()
	builder.Finish(schema)
	return append([]byte(nil), builder.FinishedBytes()...)
}
