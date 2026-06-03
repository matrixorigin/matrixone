// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package docfilter

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCorruptBloomPayloadRejected guards the bloom fallback path against an
// out-of-bounds read: a transported/forged TagBloom payload whose header
// declares a huge nbits but whose buffer is far too small must be REJECTED by
// New (-> CBloomFilter.Unmarshal -> bloomfilter_unmarshal), not accepted and
// then probed past the allocation. Without the length/power-of-two validation
// in bloomfilter_unmarshal this payload unmarshals "successfully" and the first
// Test reads ~128 GB out of bounds.
func TestCorruptBloomPayloadRejected(t *testing.T) {
	// Hand-craft a 32-byte bloomfilter_t header (host little-endian, no padding):
	// magic[4]="XXBF", k(u32)=1, nbits(u64)=2^40, seed(u64)=0, bitmap[0](u64)=0.
	// nbits=2^40 is a power of two but needs ~2^37 bytes of bitmap, which the
	// 32-byte buffer obviously lacks.
	hdr := make([]byte, 32)
	copy(hdr[0:4], []byte("XXBF"))
	binary.LittleEndian.PutUint32(hdr[4:8], 1)
	binary.LittleEndian.PutUint64(hdr[8:16], 1<<40)
	binary.LittleEndian.PutUint64(hdr[16:24], 0)
	binary.LittleEndian.PutUint64(hdr[24:32], 0)

	f, err := New(append([]byte{TagBloom}, hdr...))
	require.Error(t, err, "corrupt bloom payload (huge nbits, tiny buffer) must be rejected")
	require.Nil(t, f)

	// A buffer shorter than the header must also be rejected cleanly (no crash).
	f, err = New(append([]byte{TagBloom}, []byte("XXBF")...))
	require.Error(t, err)
	require.Nil(t, f)

	// Non-power-of-two nbits (impossible from a real Marshal, since init rounds
	// via next_pow2_64) must also be rejected, since the probe uses nbits-1 as a
	// mask.
	binary.LittleEndian.PutUint64(hdr[8:16], 1000) // not a power of two
	f, err = New(append([]byte{TagBloom}, hdr...))
	require.Error(t, err)
	require.Nil(t, f)
}

// TestCorruptCbitmapPayloadRejected guards mo_cbitmap_deserialize against a
// forged/corrupt header. nbits is read straight from the buffer; sizing the read
// via bitmap_size(nbits)=(nbits+63)>>6 OVERFLOWS for large nbits (e.g. MaxUint64
// -> nwords=0), so a 16-byte payload would be accepted as a "valid" filter that
// then crashes / matches nothing on probe. New must reject any header whose
// nbits is not exactly consistent with the payload length.
func TestCorruptCbitmapPayloadRejected(t *testing.T) {
	// cbitmap payload (host little-endian): [base u64][nbits u64][words...].
	// Forge base=0, nbits=MaxUint64, NO words (16-byte payload) — the overflow case.
	hdr := make([]byte, 16)
	binary.LittleEndian.PutUint64(hdr[0:8], 0)
	binary.LittleEndian.PutUint64(hdr[8:16], ^uint64(0))
	f, err := New(append([]byte{TagCbitmap}, hdr...))
	require.Error(t, err, "forged cbitmap header (huge nbits, no words) must be rejected")
	require.Nil(t, f)

	// nbits inconsistent with the words present: 24-byte payload = 1 word (64
	// bits) but nbits claims 1000.
	hdr2 := make([]byte, 24)
	binary.LittleEndian.PutUint64(hdr2[8:16], 1000)
	f, err = New(append([]byte{TagCbitmap}, hdr2...))
	require.Error(t, err)
	require.Nil(t, f)

	// Payload not a whole number of words (16-byte header + 3 stray bytes).
	f, err = New(append([]byte{TagCbitmap}, make([]byte, 16+3)...))
	require.Error(t, err)
	require.Nil(t, f)

	// Truncated below the 16-byte header.
	f, err = New(append([]byte{TagCbitmap}, []byte{1, 2, 3, 4}...))
	require.Error(t, err)
	require.Nil(t, f)
}
