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

package types

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/stretchr/testify/require"
)

func TestCollationTupleOrderAndOriginals(t *testing.T) {
	part, err := ResolveStringKeyPart(NewWithCharset(T_varchar, 100, 0, CharsetUTF8), PADSpaceKeyV1)
	require.NoError(t, err)
	p := NewPacker()
	defer p.Close()
	values := []string{"", " ", "A", "a ", "a\x00", "a \x00", "a b", "aa", "ß", "s", "é", "e", "中", "😀"}
	keys := make([][]byte, len(values))
	tuples := make([][]byte, len(values))
	var scratch []byte
	for i, v := range values {
		p.Reset()
		p.EncodeInt64(42)
		start := len(p.GetBuf())
		scratch, err = part.Encode(p, scratch, []byte(v))
		require.NoError(t, err)
		keys[i], err = part.Key(nil, []byte(v))
		require.NoError(t, err)
		end := len(p.GetBuf())
		p.EncodeInt64(-3)
		tuples[i] = p.Bytes()
		decoded, n, err := part.Decode(p.GetBuf()[start:])
		require.NoError(t, err)
		require.Equal(t, end-start, n)
		require.True(t, decoded.Opaque)
		require.Equal(t, keys[i], decoded.Bytes)
		p.Reset()
		p.EncodeStringType([]byte("overwrite"))
		require.Equal(t, keys[i], decoded.Bytes, "decoded weights must own their bytes")
	}
	for i := range values {
		for j := range values {
			require.Equal(t, bytes.Compare(keys[i], keys[j]), bytes.Compare(tuples[i], tuples[j]))
		}
	}
	require.Equal(t, tuples[2], tuples[3], "raw spelling must not be appended to comparison identity")
	require.NotEqual(t, []byte("A"), keys[2], "weights are not reversible original text")
}

func TestCollationTupleLegacyAndBinary(t *testing.T) {
	for _, format := range []KeyFormat{LegacyKeyFormat, PADSpaceKeyV1} {
		for _, charset := range []uint8{CharsetLegacy, CharsetBinary, CharsetUTF8MB4Bin, CharsetUTF8} {
			part, err := ResolveStringKeyPart(NewWithCharset(T_varchar, 0, 0, charset), format)
			require.NoError(t, err)
			if part.Transformed() {
				continue
			}
			p, q := NewPacker(), NewPacker()
			v := []byte{'A', 0, 0xff, ' '}
			_, err = part.Encode(p, nil, v)
			require.NoError(t, err)
			q.EncodeStringType(v)
			require.Equal(t, q.GetBuf(), p.GetBuf())
			decoded, n, err := part.Decode(p.GetBuf())
			require.NoError(t, err)
			require.Equal(t, len(p.GetBuf()), n)
			require.False(t, decoded.Opaque)
			require.Equal(t, v, decoded.Bytes)
			p.Close()
			q.Close()
		}
	}
	_, err := ResolveStringKeyPart(T_varchar.ToType(), KeyFormat(255))
	require.Error(t, err)
	_, err = ResolveStringKeyPart(T_int64.ToType(), PADSpaceKeyV1)
	require.Error(t, err)
}

func TestCollationKeyRejectsUnknownSemanticMetadata(t *testing.T) {
	for _, version := range []uint8{2, 255} {
		_, err := ResolveStringKeyPart(
			NewWithCharsetVersion(T_varchar, 32, 0, CharsetUTF8, version),
			PADSpaceKeyV1,
		)
		require.ErrorContains(t, err, "unsupported collation semantic version")
	}
	_, err := ResolveStringKeyPart(
		NewWithCharset(T_varchar, 32, 0, CharsetUTF8),
		KeyFormat(255),
	)
	require.ErrorContains(t, err, "unsupported collation key format")
	// The boolean fast-path must not turn a malformed identity into a raw
	// comparison path. Admission reports the underlying error before execution.
	require.True(t, NeedsCollationKey(
		NewWithCharsetVersion(T_varchar, 32, 0, CharsetUTF8, 2),
		PADSpaceKeyV1,
	))
}

func TestNative0900TupleKeysAreOpaqueAndNoPad(t *testing.T) {
	for _, charset := range []uint8{CharsetUTF8MB40900AI, CharsetUTF8MB40900Bin} {
		part, err := ResolveStringKeyPart(NewWithCharset(T_varchar, 0, 0, charset), PADSpaceKeyV1)
		require.NoError(t, err)
		require.True(t, part.Transformed())

		alpha, err := part.Key(nil, []byte("Alpha"))
		require.NoError(t, err)
		trailing, err := part.Key(nil, []byte("Alpha "))
		require.NoError(t, err)
		if charset == CharsetUTF8MB40900AI {
			caseInsensitive, err := part.Key(nil, []byte("alpha"))
			require.NoError(t, err)
			require.Equal(t, alpha, caseInsensitive)
		} else {
			require.NotEqual(t, alpha, trailing)
			require.NotEqual(t, alpha, mustKey(t, part, []byte("alpha")))
		}
		require.NotEqual(t, alpha, trailing, "native 0900 keys use NO PAD semantics")

		p := NewPacker()
		_, err = part.Encode(p, nil, []byte("Alpha"))
		require.NoError(t, err)
		decoded, consumed, err := part.Decode(p.GetBuf())
		require.NoError(t, err)
		require.Equal(t, len(p.GetBuf()), consumed)
		require.True(t, decoded.Opaque)
		require.Equal(t, alpha, decoded.Bytes)
		p.Close()
	}
	binPart, err := ResolveStringKeyPart(NewWithCharset(T_varchar, 0, 0, CharsetUTF8MB40900Bin), PADSpaceKeyV1)
	require.NoError(t, err)
	_, err = binPart.Key(nil, []byte{0xff})
	require.ErrorIs(t, err, collation.ErrUTF8)
}

func TestCollationVersionIsExplicitAndWireStable(t *testing.T) {
	legacy := NewWithCharset(T_varchar, 32, 0, CharsetUTF8)
	versioned := NewWithCharsetVersion(T_varchar, 32, 0, CharsetUTF8, CollationVersionV1)
	require.False(t, NeedsCollationKey(legacy, PADSpaceKeyV1))
	require.True(t, NeedsCollationKey(versioned, PADSpaceKeyV1))
	require.NotEqual(t, legacy, versioned)

	// The unsafe fixed-width type encoding remains the in-memory transport used
	// by object metadata, while Marshal/Unmarshal is the explicit endian-stable
	// form. Both must preserve the semantic version.
	raw := EncodeType(&versioned)
	require.Equal(t, versioned, DecodeType(raw))
	encoded, err := versioned.Marshal()
	require.NoError(t, err)
	var decoded Type
	require.NoError(t, decoded.Unmarshal(encoded))
	require.Equal(t, versioned, decoded)

	// Legacy bytes keep the charset in the low byte of the historical slot, so
	// an old reader still sees the same charset and a new reader sees version 0.
	legacyWire, err := legacy.Marshal()
	require.NoError(t, err)
	require.Equal(t, byte(CharsetUTF8), legacyWire[3])
	require.Zero(t, legacyWire[2])
	var legacyDecoded Type
	require.NoError(t, legacyDecoded.Unmarshal(legacyWire))
	require.Zero(t, legacyDecoded.CollationVersion)
}

func TestLegacyFixedWidthTypeLayoutRemainsDecodable(t *testing.T) {
	// Type is embedded in object/vector/catalog records through its historical
	// native representation. The former dummy byte is now the semantic-version
	// byte, so a legacy record must still decode as version zero without moving
	// any of the fixed-width fields.
	legacy := make([]byte, TSize)
	legacy[0] = byte(T_varchar)
	legacy[1] = CharsetUTF8
	legacy[2] = 1 // not-null
	legacy[3] = CollationVersionLegacy
	binary.LittleEndian.PutUint32(legacy[4:], uint32(32))
	binary.LittleEndian.PutUint32(legacy[8:], uint32(17))
	binary.LittleEndian.PutUint32(legacy[12:], uint32(3))

	decoded := DecodeType(legacy)
	require.Equal(t, T_varchar, decoded.Oid)
	require.Equal(t, CharsetUTF8, decoded.Charset)
	require.True(t, decoded.GetNotNull())
	require.Zero(t, decoded.CollationVersion)
	require.Equal(t, int32(32), decoded.Size)
	require.Equal(t, int32(17), decoded.Width)
	require.Equal(t, int32(3), decoded.Scale)
}

func TestCompareStringValuesUsesVersionedIdentity(t *testing.T) {
	general := NewWithCharsetVersion(T_varchar, 32, 0, CharsetUTF8, CollationVersionV1)
	bin := NewWithCharsetVersion(T_varchar, 32, 0, CharsetUTF8MB4Bin, CollationVersionV1)
	nativeBin := NewWithCharset(T_varchar, 32, 0, CharsetUTF8MB40900Bin)
	legacy := NewWithCharset(T_varchar, 32, 0, CharsetUTF8)

	require.Equal(t, 0, CompareStringValues(general, []byte("Alpha"), []byte("alpha")))
	require.NotEqual(t, 0, CompareStringValues(legacy, []byte("Alpha"), []byte("alpha")))
	require.Equal(t, 0, CompareStringValues(bin, []byte("a"), []byte("a ")))
	require.NotEqual(t, 0, CompareStringValues(nativeBin, []byte("a"), []byte("a ")))
}

func mustKey(t *testing.T, part StringKeyPart, value []byte) []byte {
	t.Helper()
	key, err := part.Key(nil, value)
	require.NoError(t, err)
	return key
}

func TestCollationTupleCapacityAndMalformed(t *testing.T) {
	part, err := ResolveStringKeyPart(NewWithCharset(T_varchar, 0, 0, CharsetUTF8), PADSpaceKeyV1)
	require.NoError(t, err)
	p := NewPackerWithFixedBuffer(make([]byte, 4))
	defer p.Close()
	p.EncodeNull()
	before := p.Bytes()
	_, err = part.Encode(p, nil, []byte("long"))
	require.ErrorIs(t, err, ErrPackerCapacity)
	require.Equal(t, before, p.GetBuf())
	_, err = part.Encode(p, nil, []byte{0xff})
	require.Error(t, err)
	require.Equal(t, before, p.GetBuf())
	q := NewPacker()
	defer q.Close()
	_, err = part.Encode(q, nil, []byte("a\x00"))
	require.NoError(t, err)
	for n := 0; n < len(q.GetBuf()); n++ {
		_, _, err := part.Decode(q.GetBuf()[:n])
		require.Error(t, err, "truncation %d", n)
	}
	for _, bad := range [][]byte{{stringTypeCode, bytesCode, 0}, {stringTypeCode, bytesCode, 0x22, 0, 0xff}, {stringTypeCode, bytesMaxCode, 0}} {
		_, _, err := part.Decode(bad)
		require.Error(t, err)
	}
}

func FuzzCollationTupleDecode(f *testing.F) {
	f.Add([]byte{stringTypeCode, bytesCode, 0x22, 0})
	f.Add([]byte{stringTypeCode, bytesCode, 0, 0xff})
	f.Add([]byte{stringTypeCode, bytesCode, 0x1c, 0})
	f.Add([]byte{stringTypeCode, bytesCode, 0, 0xff, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		for _, charset := range []uint8{CharsetBinary, CharsetUTF8, CharsetUTF8MB4Bin, CharsetUTF8MB40900AI, CharsetUTF8MB40900Bin} {
			part, _ := ResolveStringKeyPart(NewWithCharset(T_varchar, 0, 0, charset), PADSpaceKeyV1)
			decoded, n, err := part.Decode(b)
			if err == nil {
				if n < 3 || n > len(b) {
					t.Fatalf("invalid consumed length %d", n)
				}
				// Re-encode the payload as raw bytes: do not transform twice.
				p := NewPacker()
				p.EncodeStringType(decoded.Bytes)
				same := bytes.Equal(p.GetBuf(), b[:n])
				p.Close()
				if !same {
					t.Fatal("accepted non-canonical framing")
				}
			}
		}
	})
}
