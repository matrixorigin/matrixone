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

package sidecarflight

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	nativeBatchFrameHeaderBytes = 24
	nativeResultSchemaVersion   = uint32(1)
	maxNativeResultSchemaBytes  = 1 << 20
	maxNativeResultColumns      = 4096
)

type nativeResultSchema struct {
	Version uint32                `protobuf:"varint,1,opt,name=version,proto3"`
	Columns []*nativeResultColumn `protobuf:"bytes,2,rep,name=columns,proto3"`
}

func (s *nativeResultSchema) Reset()         { *s = nativeResultSchema{} }
func (s *nativeResultSchema) String() string { return proto.CompactTextString(s) }
func (s *nativeResultSchema) ProtoMessage()  {}

type nativeResultColumn struct {
	Name        string `protobuf:"bytes,1,opt,name=name,proto3"`
	Oid         uint32 `protobuf:"varint,2,opt,name=oid,proto3"`
	Width       int32  `protobuf:"varint,3,opt,name=width,proto3"`
	Scale       int32  `protobuf:"varint,4,opt,name=scale,proto3"`
	Charset     uint32 `protobuf:"varint,5,opt,name=charset,proto3"`
	NotNullable bool   `protobuf:"varint,6,opt,name=not_nullable,json=notNullable,proto3"`
}

func (m *nativeResultColumn) Reset()         { *m = nativeResultColumn{} }
func (m *nativeResultColumn) String() string { return proto.CompactTextString(m) }
func (m *nativeResultColumn) ProtoMessage()  {}

func newNativeResultSchema(expected []planpb.Type, headings []string) (*nativeResultSchema, []byte, error) {
	if len(expected) == 0 || len(expected) > maxNativeResultColumns || len(headings) != len(expected) {
		return nil, nil, internalErrorf("sidecar flight: MatrixOne result schema is empty or inconsistent")
	}
	result := &nativeResultSchema{
		Version: nativeResultSchemaVersion,
		Columns: make([]*nativeResultColumn, len(expected)),
	}
	for i := range expected {
		column, err := nativeResultColumnFromPlan(headings[i], expected[i])
		if err != nil {
			return nil, nil, internalErrorf("sidecar flight: result column %d: %w", i, err)
		}
		result.Columns[i] = column
	}
	wire, err := proto.Marshal(result)
	if err != nil {
		return nil, nil, internalErrorf("sidecar flight: encode native result schema: %w", err)
	}
	if len(wire) == 0 || len(wire) > maxNativeResultSchemaBytes {
		return nil, nil, internalErrorf("sidecar flight: native result schema exceeds the supported bound")
	}
	return result, wire, nil
}

func nativeResultColumnFromPlan(name string, expected planpb.Type) (*nativeResultColumn, error) {
	oid := types.T(expected.Id)
	switch oid {
	case types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint32,
		types.T_float32, types.T_float64,
		types.T_date:
	case types.T_char, types.T_varchar:
		if expected.Width < 0 || expected.Charset > math.MaxUint8 {
			return nil, internalErrorf("unsupported MatrixOne result type %s", oid.String())
		}
	case types.T_decimal64:
		if expected.Width <= 0 || expected.Width > 18 || expected.Scale < 0 || expected.Scale > expected.Width {
			return nil, internalErrorf("invalid MatrixOne decimal64(%d,%d)", expected.Width, expected.Scale)
		}
	case types.T_decimal128:
		if expected.Width <= 18 || expected.Width > 38 || expected.Scale < 0 || expected.Scale > expected.Width {
			return nil, internalErrorf("invalid MatrixOne decimal128(%d,%d)", expected.Width, expected.Scale)
		}
	default:
		return nil, internalErrorf("unsupported MatrixOne result type %s", oid.String())
	}
	return &nativeResultColumn{
		Name: name, Oid: uint32(oid), Width: expected.Width, Scale: expected.Scale,
		Charset: expected.Charset, NotNullable: expected.NotNullable,
	}, nil
}

func (s *nativeResultSchema) validateWire(wire []byte) error {
	if s == nil {
		return internalErrorf("sidecar flight: missing native result schema")
	}
	expected, err := proto.Marshal(s)
	if err != nil {
		return internalErrorf("sidecar flight: encode expected native result schema: %w", err)
	}
	if !bytes.Equal(wire, expected) {
		return internalErrorf("sidecar flight: native result schema mismatch")
	}
	return nil
}

func marshalNativeBatchFrame(sequence uint64, payload []byte) []byte {
	frame := make([]byte, nativeBatchFrameHeaderBytes+len(payload))
	copy(frame[:4], "MOB1")
	binary.LittleEndian.PutUint16(frame[4:6], 1)
	binary.LittleEndian.PutUint64(frame[8:16], sequence)
	binary.LittleEndian.PutUint64(frame[16:24], uint64(len(payload)))
	copy(frame[nativeBatchFrameHeaderBytes:], payload)
	return frame
}

func unmarshalNativeBatchFrame(frame []byte, maximum uint64) (uint64, []byte, error) {
	if len(frame) < nativeBatchFrameHeaderBytes || string(frame[:4]) != "MOB1" ||
		binary.LittleEndian.Uint16(frame[4:6]) != 1 || frame[6] != 0 || frame[7] != 0 {
		return 0, nil, internalErrorf("sidecar flight: invalid MO native batch frame")
	}
	sequence := binary.LittleEndian.Uint64(frame[8:16])
	payloadBytes := binary.LittleEndian.Uint64(frame[16:24])
	if sequence == 0 || payloadBytes == 0 || payloadBytes > maximum ||
		payloadBytes != uint64(len(frame)-nativeBatchFrameHeaderBytes) {
		return 0, nil, internalErrorf("sidecar flight: invalid MO native batch frame bounds")
	}
	return sequence, frame[nativeBatchFrameHeaderBytes:], nil
}

func (s *nativeResultSchema) decodeBatch(payload []byte, mp *mpool.MPool) (result *batch.Batch, err error) {
	if s == nil || mp == nil {
		return nil, internalErrorf("sidecar flight: missing native result schema or memory pool")
	}
	result = batch.NewOffHeapEmpty()
	defer func() {
		if err != nil && result != nil {
			result.Clean(mp)
			result = nil
		}
	}()
	if err = result.UnmarshalBinaryWithAnyMp(payload, mp); err != nil {
		return nil, internalErrorf("sidecar flight: decode MO native result batch: %w", err)
	}
	if result.RowCount() <= 0 || len(result.Vecs) != len(s.Columns) ||
		(len(result.Attrs) != 0 && len(result.Attrs) != len(s.Columns)) ||
		len(result.ExtraBuf) != 0 || result.Recursive != 0 || result.ShuffleIDX != 0 {
		return nil, internalErrorf(
			"sidecar flight: MO native result batch metadata mismatch: rows=%d vectors=%d attrs=%d extra=%d recursive=%d shuffle=%d",
			result.RowCount(), len(result.Vecs), len(result.Attrs), len(result.ExtraBuf), result.Recursive, result.ShuffleIDX,
		)
	}
	for _, attr := range result.Attrs {
		if attr != "" {
			return nil, internalErrorf("sidecar flight: MO native result batch contains unexpected attributes")
		}
	}
	decodedAttrs := result.Attrs
	result.Attrs = nil
	canonical, marshalErr := result.MarshalBinary()
	result.Attrs = decodedAttrs
	if marshalErr != nil || !bytes.Equal(canonical, payload) {
		return nil, internalErrorf("sidecar flight: MO native result batch is non-canonical or has trailing data")
	}
	if err = result.CheckLength(); err != nil {
		return nil, internalErrorf("sidecar flight: MO native result batch length: %w", err)
	}
	for i, column := range s.Columns {
		vec := result.Vecs[i]
		if vec == nil {
			return nil, internalErrorf("sidecar flight: MO native result column %d is nil", i)
		}
		if !vec.IsFlat() {
			return nil, internalErrorf("sidecar flight: MO native result column %d is not flat", i)
		}
		actual := vec.GetType()
		expectedSize := types.T(column.Oid).ToType().Size
		if uint32(actual.Oid) != column.Oid || actual.Size != expectedSize ||
			actual.Width != column.Width || actual.Scale != column.Scale ||
			uint32(actual.Charset) != column.Charset || actual.GetNotNull() != column.NotNullable {
			return nil, internalErrorf("sidecar flight: MO native result column %d type mismatch", i)
		}
		if column.NotNullable && vec.HasNull() {
			return nil, internalErrorf("sidecar flight: required MO native result column %d contains nulls", i)
		}
	}
	return result, nil
}
