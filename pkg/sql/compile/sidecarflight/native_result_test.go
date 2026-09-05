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
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func marshalNativeBatchFrame(sequence uint64, payload []byte) []byte {
	frame := make([]byte, nativeBatchFrameHeaderBytes+len(payload))
	copy(frame[:4], "MOB1")
	binary.LittleEndian.PutUint16(frame[4:6], 1)
	binary.LittleEndian.PutUint64(frame[8:16], sequence)
	binary.LittleEndian.PutUint64(frame[16:24], uint64(len(payload)))
	copy(frame[nativeBatchFrameHeaderBytes:], payload)
	return frame
}

func TestNativeResultCodecDecodesNegotiatedTypes(t *testing.T) {
	typesOut, headings := fixtureOutputShape()
	schema, wire, err := newNativeResultSchema(typesOut, headings)
	require.NoError(t, err)
	require.NoError(t, schema.validateWire(wire))

	mp := mpool.MustNewZero()
	bat, err := schema.decodeBatch(fixtureNativeResultPayload(), mp)
	require.NoError(t, err)
	require.Equal(t, 2, bat.RowCount())
	for _, vec := range bat.Vecs {
		require.False(t, vec.IsNull(0))
		require.True(t, vec.IsNull(1))
	}
	require.True(t, vector.GetFixedAtNoTypeCheck[bool](bat.Vecs[0], 0))
	require.Equal(t, "tpch", bat.Vecs[7].GetStringAt(0))
	require.Equal(t, types.DaysFromUnixEpochToDate(1), vector.GetFixedAtNoTypeCheck[types.Date](bat.Vecs[10], 0))
	require.Equal(t, uint32(42), vector.GetFixedAtNoTypeCheck[uint32](bat.Vecs[11], 0))
	bat.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestNativeResultCodecRejectsFramesAndSchemaMismatch(t *testing.T) {
	sequence, payload, err := unmarshalNativeBatchFrame(marshalNativeBatchFrame(7, []byte("payload")), 1<<20)
	require.NoError(t, err)
	require.Equal(t, uint64(7), sequence)
	require.Equal(t, []byte("payload"), payload)

	_, _, err = unmarshalNativeBatchFrame([]byte("MOB1"), 1<<20)
	require.Error(t, err)
	typesOut, headings := fixtureOutputShape()
	schema, wire, err := newNativeResultSchema(typesOut, headings)
	require.NoError(t, err)
	wire[len(wire)-1] ^= 1
	require.ErrorContains(t, schema.validateWire(wire), "schema mismatch")

	mp := mpool.MustNewZero()
	trailing := append(fixtureNativeResultPayload(), 0)
	_, err = schema.decodeBatch(trailing, mp)
	require.ErrorContains(t, err, "non-canonical or has trailing data")

	wrongSize := fixtureNativeResultPayload()
	// batch header (12), vector length (4), class (1), then the MO Type's
	// four-byte Size field begins four bytes into the 16-byte Type.
	binary.LittleEndian.PutUint32(wrongSize[21:25], 8)
	_, err = schema.decodeBatch(wrongSize, mp)
	require.ErrorContains(t, err, "invalid vector type size")
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestNativeResultCodecRejectsNonFlatVectors(t *testing.T) {
	schema := &nativeResultSchema{
		Version: nativeResultSchemaVersion,
		Columns: []*nativeResultColumn{{Name: "v", Oid: uint32(types.T_int64)}},
	}

	for _, tc := range []struct {
		name string
		rows int
		make func(*testing.T, *mpool.MPool) *vector.Vector
	}{
		{
			name: "constant billion-row broadcast",
			rows: 1 << 30,
			make: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				vec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
				require.NoError(t, err)
				vec.SetLength(1 << 30)
				return vec
			},
		},
		{
			name: "dictionary class",
			rows: 1,
			make: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				vec := vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(vec, int64(7), false, mp))
				vec.SetClass(vector.DIST)
				return vec
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			payload := func() []byte {
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = tc.make(t, mp)
				bat.SetRowCount(tc.rows)
				defer bat.Clean(mp)
				payload, err := bat.MarshalBinary()
				require.NoError(t, err)
				if tc.rows > 1 {
					require.Less(t, len(payload), 1024, "constant logical work must remain compressed on the wire")
				}
				return payload
			}()

			_, err := schema.decodeBatch(payload, mp)
			require.ErrorContains(t, err, "is not flat")
			require.Equal(t, int64(0), mp.CurrNB())
		})
	}
}
