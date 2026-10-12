// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestCollationMetadataBatchReadersRejectBeforeAllocation(t *testing.T) {
	typ := types.NewWithCharset(types.T_varchar, 8, 0, 3)
	typ.CollationVersion = 2
	for _, variant := range []string{"current", "v1", "v2"} {
		t.Run(variant, func(t *testing.T) {
			envelope := MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
			defer envelope.Close()
			envelope.Append(types.EncodeFixed(uint16(1)), false)
			envelope.Append([]byte("v"), false)
			envelope.Append(types.EncodeType(&typ), false)
			var wire bytes.Buffer
			var err error
			if variant == "current" {
				_, err = envelope.WriteTo(&wire)
			} else {
				_, err = envelope.WriteToV1(&wire)
			}
			require.NoError(t, err)
			bat := NewBatch()
			defer bat.Close()
			var read func(io.Reader) (int64, error)
			switch variant {
			case "current":
				read = bat.ReadFrom
			case "v1":
				read = bat.ReadFromV1
			case "v2":
				read = bat.ReadFromV2
			}
			require.NotPanics(t, func() { _, err = read(bytes.NewReader(wire.Bytes())) })
			require.ErrorContains(t, err, "collation")
			require.Empty(t, bat.Vecs)
			require.Empty(t, bat.Attrs)
		})
	}
}

func TestCollationMetadataBatchPayloadRejection(t *testing.T) {
	for _, variant := range []string{"current", "v1", "v2"} {
		t.Run(variant, func(t *testing.T) {
			typ := types.NewWithCharset(types.T_varchar, 8, 0, 3)
			envelope := MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
			defer envelope.Close()
			envelope.Append(types.EncodeFixed(uint16(1)), false)
			envelope.Append([]byte("v"), false)
			envelope.Append(types.EncodeType(&typ), false)
			data := MakeVector(typ, common.DefaultAllocator)
			defer data.Close()
			data.Append([]byte("x"), false)
			var wire, payload bytes.Buffer
			var err error
			if variant == "current" {
				_, err = envelope.WriteTo(&wire)
				require.NoError(t, err)
				_, err = data.WriteTo(&payload)
			} else {
				_, err = envelope.WriteToV1(&wire)
				require.NoError(t, err)
				_, err = data.WriteToV1(&payload)
			}
			require.NoError(t, err)
			// TN length prefix, vector class, then byte 3 of the native Type.
			payload.Bytes()[8+1+3] = 2
			_, err = wire.Write(payload.Bytes())
			require.NoError(t, err)
			bat := NewBatch()
			defer bat.Close()
			read := bat.ReadFrom
			if variant == "v1" {
				read = bat.ReadFromV1
			} else if variant == "v2" {
				read = bat.ReadFromV2
			}
			_, err = read(bytes.NewReader(wire.Bytes()))
			require.ErrorContains(t, err, "collation")
			require.Empty(t, bat.Vecs, "failed vector must not transfer into the batch")
		})
	}
}

func TestCollationMetadataBatchEnvelopeShape(t *testing.T) {
	for _, rows := range [][][]byte{
		{}, {{1}}, {types.EncodeFixed(uint16(1))},
		{types.EncodeFixed(uint16(1)), nil, make([]byte, types.TSize)},
		{types.EncodeFixed(uint16(1)), []byte("v"), nil},
		{types.EncodeFixed(uint16(1)), []byte("v"), {1}},
	} {
		envelope := MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
		for _, row := range rows {
			envelope.Append(row, row == nil)
		}
		require.Error(t, validateBatchTypeMetadata(envelope))
		envelope.Close()
	}
	envelope := MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer envelope.Close()
	envelope.Append(types.EncodeFixed(uint16(0)), false)
	require.NoError(t, validateBatchTypeMetadata(envelope))
}
