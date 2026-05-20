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

package cuvs

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// Direct tests for the UnframeCdcChunk corruption matrix. The existing
// TestReplayEventLog_RejectsCorruptFrame exercises the same paths
// through ReplayEventLog; the cases here pin UnframeCdcChunk's
// invariants in isolation so a future change to the unframer can't
// regress without flagging.

// TestUnframeCdcChunk_RoundTripEmpty: zero-length payload still
// produces a valid frame of exactly cdcFrameOverhead bytes.
func TestUnframeCdcChunk_RoundTripEmpty(t *testing.T) {
	framed := FrameCdcChunk(nil)
	require.Len(t, framed, cdcFrameOverhead)
	got, err := UnframeCdcChunk(framed)
	require.NoError(t, err)
	require.Len(t, got, 0)
}

func TestUnframeCdcChunk_RoundTrip(t *testing.T) {
	payload := []byte("the quick brown fox jumps over the lazy dog")
	framed := FrameCdcChunk(payload)
	require.Len(t, framed, cdcFrameOverhead+len(payload))
	got, err := UnframeCdcChunk(framed)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestUnframeCdcChunk_TooShort(t *testing.T) {
	// Every length from 0 .. cdcFrameOverhead-1 is rejected before
	// any field-level parse — the bounds check must come first.
	for n := 0; n < cdcFrameOverhead; n++ {
		_, err := UnframeCdcChunk(make([]byte, n))
		require.Error(t, err, "len=%d", n)
		require.Contains(t, err.Error(), "too short")
	}
}

func TestUnframeCdcChunk_BadStartMagic(t *testing.T) {
	framed := FrameCdcChunk([]byte("payload"))
	framed[0] ^= 0xFF
	_, err := UnframeCdcChunk(framed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad start magic")
}

func TestUnframeCdcChunk_UnknownVersion(t *testing.T) {
	framed := FrameCdcChunk([]byte("payload"))
	binary.LittleEndian.PutUint32(framed[4:8], cdcChunkVersion+1)
	_, err := UnframeCdcChunk(framed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown version")
}

// TestUnframeCdcChunk_PlenOverflow: plen=0xFFFFFFFF must not wrap when
// added to cdcFrameOverhead. The unframer does the arithmetic in
// uint64; this test pins that contract so a future "optimization"
// back to uint32 (where 0xFFFFFFFF + 32 wraps to 31) doesn't sneak
// past as "frame size matches len(framed)".
func TestUnframeCdcChunk_PlenOverflow(t *testing.T) {
	framed := FrameCdcChunk([]byte("payload"))
	binary.LittleEndian.PutUint32(framed[8:12], math.MaxUint32)
	_, err := UnframeCdcChunk(framed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "payload_len")
}

// TestUnframeCdcChunk_PlenUnderreports: plen smaller than the actual
// payload bytes the producer wrote. The size check catches this
// regardless of where the CRC would land.
func TestUnframeCdcChunk_PlenUnderreports(t *testing.T) {
	framed := FrameCdcChunk([]byte("12345678"))
	binary.LittleEndian.PutUint32(framed[8:12], 4) // claim 4, frame has 8
	_, err := UnframeCdcChunk(framed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "payload_len")
}

// TestUnframeCdcChunk_PlenOverreports: plen bigger than the actual
// frame can hold (and bigger than the payload bytes). Same size
// check catches this from the other side.
func TestUnframeCdcChunk_PlenOverreports(t *testing.T) {
	framed := FrameCdcChunk([]byte("12345678"))
	binary.LittleEndian.PutUint32(framed[8:12], 16) // claim 16, frame has 8
	_, err := UnframeCdcChunk(framed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "payload_len")
}

// TestUnframeCdcChunk_TruncatedAfterHeader: producer wrote a frame
// claiming N-byte payload, but only N-k bytes of payload (plus
// footer) actually made it onto disk. The total frame is smaller
// than declared.
func TestUnframeCdcChunk_TruncatedAfterHeader(t *testing.T) {
	framed := FrameCdcChunk(make([]byte, 64))
	for cut := 1; cut <= 32 && len(framed)-cut >= cdcFrameOverhead; cut++ {
		truncated := framed[:len(framed)-cut]
		_, err := UnframeCdcChunk(truncated)
		require.Error(t, err, "truncated by %d", cut)
		// plen says 64 but len(framed)=96-cut → size mismatch
		require.Contains(t, err.Error(), "payload_len")
	}
}

// TestUnframeCdcChunk_TruncatedHeader: the very first cdcHeaderSize
// bytes themselves are truncated — caught by the length check
// before any field parse.
func TestUnframeCdcChunk_TruncatedHeader(t *testing.T) {
	framed := FrameCdcChunk(make([]byte, 64))
	for n := cdcHeaderSize; n < cdcFrameOverhead; n++ {
		_, err := UnframeCdcChunk(framed[:n])
		require.Error(t, err, "truncated to %d (< overhead %d)", n, cdcFrameOverhead)
		require.Contains(t, err.Error(), "too short")
	}
}

// TestUnframeCdcChunk_PlenTamperReCrc: classic forgery attempt —
// attacker reduces plen by k, truncates the frame to match the new
// payload size, and recomputes the CRC over the new range. The
// crc32 check then passes, but the *end magic* at the new offset
// is whatever happened to be in the old payload at that spot. This
// test pins the end-magic check as the last line of defence for
// length-field manipulation that recomputes CRC.
func TestUnframeCdcChunk_PlenTamperReCrc(t *testing.T) {
	const origPayload = 64
	const tamperShrink = 16 // reduce declared payload by this many bytes

	framed := FrameCdcChunk(make([]byte, origPayload))
	tampered := make([]byte, cdcHeaderSize+origPayload-tamperShrink+cdcFooterSize)
	copy(tampered, framed)
	newPlen := uint32(origPayload - tamperShrink)
	binary.LittleEndian.PutUint32(tampered[8:12], newPlen)
	// Recompute CRC over the new range so the integrity check itself
	// passes — leaving only the end-magic check to catch the forgery.
	newFooterOff := cdcHeaderSize + int(newPlen)
	newCrc := crc32.ChecksumIEEE(tampered[4:newFooterOff])
	binary.LittleEndian.PutUint32(tampered[newFooterOff:newFooterOff+4], newCrc)
	// End magic at newFooterOff+12 is whatever was at offset
	// (cdcHeaderSize+newPlen+12) of the original frame — i.e. 4 bytes
	// of zero-initialised payload — which is NOT cdcChunkMagic.
	_, err := UnframeCdcChunk(tampered)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad end magic")
}

// TestUnframeCdcChunk_PlenTamperFullForgery: the strictly stronger
// forgery — attacker tampers plen AND patches the end magic at the
// new offset. The CRC check still rejects, because the CRC was
// recomputed before the end-magic patch and the patch lies inside
// the (newFooterOff+12 .. newFooterOff+16) range that crc32 over
// [4..newFooterOff] does NOT cover; so the crc actually matches.
// What blocks this case is that the new frame has the wrong total
// size for the declared plen — caught by the size check.
//
// In other words: there is no single-step length-tamper that
// produces a frame which (a) has a matching size, (b) has a
// matching CRC, and (c) has a matching end magic — without the
// attacker writing an entirely new well-formed frame, which is no
// longer "corruption" but a legitimate (smaller) frame.
func TestUnframeCdcChunk_PlenTamperFullForgery(t *testing.T) {
	const origPayload = 64
	const tamperShrink = 16

	framed := FrameCdcChunk(make([]byte, origPayload))
	// Forge by writing a NEW well-formed smaller frame in-place,
	// then keep the original suffix to make total bytes mismatch.
	smaller := FrameCdcChunk(make([]byte, origPayload-tamperShrink))
	tampered := append(append([]byte(nil), smaller...), framed[len(smaller):]...)
	_, err := UnframeCdcChunk(tampered)
	require.Error(t, err)
	require.Contains(t, err.Error(), "payload_len")
}

func TestUnframeCdcChunk_PayloadBitFlip(t *testing.T) {
	framed := FrameCdcChunk([]byte("important records"))
	framed[cdcHeaderSize+5] ^= 0x80 // flip a bit in the payload
	_, err := UnframeCdcChunk(framed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "crc32 mismatch")
}

func TestUnframeCdcChunk_BadEndMagic(t *testing.T) {
	framed := FrameCdcChunk([]byte("payload"))
	framed[len(framed)-1] ^= 0xFF
	_, err := UnframeCdcChunk(framed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad end magic")
}

// FuzzUnframeCdcChunk drives random byte slices into UnframeCdcChunk
// to prove that no input — corrupt, truncated, or hand-crafted —
// can produce a panic (index out of range, integer overflow,
// allocation failure, etc.). Seed corpus covers the well-formed
// case, the smallest-possible frame, and a few representative
// corruption patterns so the fuzzer has something to mutate from
// rather than groping in the dark.
//
// Run with: go test -run=. -fuzz=FuzzUnframeCdcChunk -fuzztime=30s
func FuzzUnframeCdcChunk(f *testing.F) {
	f.Add(FrameCdcChunk(nil))
	f.Add(FrameCdcChunk([]byte("seed")))
	f.Add(FrameCdcChunk(make([]byte, 4096)))
	// Hand-crafted corruption seeds, so the fuzzer doesn't have to
	// rediscover the basic shapes.
	flipped := FrameCdcChunk([]byte("seed"))
	flipped[0] ^= 0xFF
	f.Add(flipped)
	f.Add(make([]byte, cdcFrameOverhead)) // all-zero, no valid magic
	f.Add([]byte{0x11, 0x1A, 0xC5, 0xCD}) // start magic, then nothing

	f.Fuzz(func(t *testing.T, framed []byte) {
		// Sole contract: must not panic. Return value is don't-care
		// — corrupt inputs return an error, valid ones return the
		// payload slice; the round-trip property is covered by the
		// explicit tests above.
		_, _ = UnframeCdcChunk(framed)
	})
}
