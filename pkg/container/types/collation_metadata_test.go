// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"bytes"
	"encoding/hex"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestVersionedCollationTypeRoundTrip(t *testing.T) {
	require.Equal(t, 16, TSize)
	require.Equal(t, uintptr(4), unsafe.Offsetof(Type{}.Size))
	for id := uint8(0); id <= uint8(collation.GBKChineseCIIdentity); id++ {
		for version := uint8(0); version <= 1; version++ {
			if err := collation.ValidateMetadata(uint32(id), uint32(version)); err != nil {
				continue
			}
			typ := NewWithCharset(T_varchar, 32, 0, id)
			typ.CollationVersion = version
			p := typ.PlanType()
			wire, err := p.Marshal()
			require.NoError(t, err)
			var received plan.Type
			require.NoError(t, received.Unmarshal(wire))
			restored, err := TypeFromPlan(received)
			require.NoError(t, err)
			require.Equal(t, typ, restored)
			data, err := typ.Marshal()
			require.NoError(t, err)
			require.Len(t, data, 20)
			var decoded Type
			require.NoError(t, decoded.Unmarshal(data))
			require.Equal(t, typ, decoded)
			native := append([]byte(nil), EncodeType(&typ)...)
			decoded, err = DecodeTypeChecked(native)
			require.NoError(t, err)
			require.Equal(t, typ, decoded)
			decoded, err = ReadType(bytes.NewReader(native))
			require.NoError(t, err)
			require.Equal(t, typ, decoded)
		}
	}
	legacy := NewWithCharset(T_varchar, 32, 0, CharsetUTF8)
	data, err := legacy.Marshal()
	require.NoError(t, err)
	// Pre-version wire fixture: oid=61, charset=3. Signed size=24 and
	// width=32 use the existing Int32ToUint32 mapping (48 and 64).
	require.Equal(t, "003d000300000000000000300000004000000000", hex.EncodeToString(data))
	updated := legacy
	updated.CollationVersion = 1
	require.False(t, legacy.Eq(updated))
	require.Equal(t, float64(0), testing.AllocsPerRun(100, func() {
		_, err := DecodeTypeChecked(EncodeType(&legacy))
		if err != nil {
			panic(err)
		}
	}))
}

func TestCollationTypeRejectsUnknownBeforePublication(t *testing.T) {
	original := NewWithCharset(T_varchar, 32, 0, CharsetUTF8)
	data, err := original.Marshal()
	require.NoError(t, err)
	for _, tc := range []struct {
		offset int
		value  byte
	}{{2, 2}, {3, 255}, {4, 1}, {6, 1}} {
		bad := append([]byte(nil), data...)
		bad[tc.offset] = tc.value
		decoded := original
		require.Error(t, decoded.Unmarshal(bad))
		require.Equal(t, original, decoded)
	}
	for n := 0; n < 20; n++ {
		decoded := original
		require.Error(t, decoded.Unmarshal(data[:n]))
		_, err := original.MarshalTo(data[:n])
		require.Error(t, err)
	}
	_, err = DecodeTypeChecked(nil)
	require.Error(t, err)
	_, err = ReadType(bytes.NewReader(nil))
	require.Error(t, err)
	badType := original
	badType.CollationVersion = 2
	_, err = DecodeTypeChecked(EncodeType(&badType))
	require.Error(t, err)
	_, err = badType.Marshal()
	require.Error(t, err)
	for _, p := range []plan.Type{{Id: 61, Charset: 257}, {Id: 61, Charset: 3, CollationVersion: 256}, {Id: 23, CollationVersion: 1}} {
		_, err := TypeFromPlan(p)
		require.Error(t, err)
		require.Panics(t, func() { MustTypeFromPlan(p) })
	}
	marker, err := TypeFromPlan(plan.Type{Id: 23, Charset: 255})
	require.NoError(t, err)
	require.NoError(t, marker.ValidateCollation())
}
