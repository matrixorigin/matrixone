// Copyright 2021 Matrix Origin
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

package bitmap

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// This file holds the Bitmap operations outside the Gobra-verified core in
// bitmap.go: copying, remapping, serialization and helpers,
// generics or packages Gobra cannot model. They must preserve the invariants
// documented in bitmap.go.

const MarshalHeaderSize = 24

// InitWithInvertedBits sets n to length bits read LSB-first from src
// starting at bit offset, each bit inverted. It turns an Arrow validity
// bitmap (1 = valid) into a null bitmap (1 = null). src must hold at least
// offset+length bits. The words are allocated from mp.
func (n *Bitmap) InitWithInvertedBits(src []byte, offset, length int, mp *mpool.MPool) error {
	if err := n.InitWithSize(int64(length), mp); err != nil {
		return err
	}
	var count int64
	for w := range n.data {
		bit := offset + w*64
		var buf [9]byte
		copy(buf[:], src[bit>>3:])
		shift := uint(bit & 7)
		word := ^(binary.LittleEndian.Uint64(buf[:8])>>shift | uint64(buf[8])<<(64-shift))
		if rem := length - w*64; rem < 64 {
			word &= uint64(1)<<uint(rem) - 1
		}
		n.data[w] = word
		count += int64(bits.OnesCount64(word))
	}
	n.popCount = count
	return nil
}

// Clone returns an owned copy of n allocated from mp, or nil for nil.
func (n *Bitmap) Clone(mp *mpool.MPool) (*Bitmap, error) {
	if n == nil {
		return nil, nil
	}
	var res Bitmap
	if err := res.InitWith(n, mp); err != nil {
		return nil, err
	}
	return &res, nil
}

func (n *Bitmap) Iterator() Iterator {
	itr := n.IteratorValue()
	return &itr
}

// Ptr returns the first data word for C code, or nil without words. A
// read-only bitmap's words may be shifted or hold rows past Len; callers
// handing words to C must use WordsCovering.
func (n *Bitmap) Ptr() *uint64 {
	if n == nil || len(n.data) == 0 {
		return nil
	}
	if n.shift != 0 {
		panic(moerr.NewInternalErrorNoCtx("bitmap Ptr on a shifted view; use WordsCovering"))
	}
	return &n.data[0]
}

// WordsCovering returns the bitmap's words, padded with zero words when they
// cover fewer than nbits bits. A bitmap ends at its highest set bit, so it
// can be shorter than the vector it describes; C code that tests the bit of
// every row must get words covering every row, or it reads past the bitmap.
// It returns nil for a bitmap without words.
// WordsCovering returns the bitmap's words, padded with zero words when they
// cover fewer than nbits bits. A bitmap ends at its highest set bit, so it
// can be shorter than the vector it describes; C code that tests the bit of
// every row must get words covering every row, or it reads past the bitmap.
// A read-only bitmap gets a clean, unshifted copy. It returns nil for a
// bitmap without words.
func (n *Bitmap) WordsCovering(nbits int) []uint64 {
	if n == nil || len(n.data) == 0 {
		return nil
	}
	need := (nbits + 63) / 64
	if n.mp == nil {
		words := n.cleanWords()
		if len(words) < need {
			words = append(words, make([]uint64, need-len(words))...)
		}
		return words
	}
	if len(n.data) >= need {
		return n.data
	}
	words := make([]uint64, need)
	copy(words, n.data)
	return words
}

func (n *Bitmap) ToArray() []uint64 {
	rows := make([]uint64, 0, n.Count())
	ToArray(n, &rows)
	return rows
}

func (n *Bitmap) ToI64Array(out *[]int64) []int64 {
	var res []int64
	if out != nil {
		res = (*out)[:0]
	}
	return AppendRows(n, res)
}

// liveWords returns the words that hold bits below numBits, and a mask for
// the last of them (all ones when numBits is a multiple of 64). Words
// missing from data read as zero, so they are simply not returned.
func (n *Bitmap) liveWords() ([]uint64, uint64) {
	nwords := int((n.numBits + 63) / 64)
	tail := ^uint64(0)
	if len(n.data) < nwords {
		return n.data, tail
	}
	if r := n.numBits % 64; r != 0 {
		tail = uint64(1)<<uint(r) - 1
	}
	return n.data[:nwords], tail
}

// AppendRows appends the positions of n's set bits, ascending, to dst. It
// grows dst once, by the bit count, and visits each word once, spending one
// step per set bit.
func AppendRows[T int64 | uint64 | int | int32 | uint32](n *Bitmap, dst []T) []T {
	if n == nil || n.popCount == 0 {
		return dst
	}
	dst = slices.Grow(dst, int(n.popCount))
	if n.shift != 0 {
		for i := range n.nwords() {
			w := n.lword(i)
			for w != 0 {
				dst = append(dst, T(i*64+bits.TrailingZeros64(w)))
				w &= w - 1
			}
		}
		return dst
	}
	words, tail := n.liveWords()
	last := (n.numBits+63)/64 - 1
	for i, w := range words {
		if int64(i) == last {
			w &= tail
		}
		base := T(i * 64)
		if w == ^uint64(0) {
			for k := T(0); k < 64; k++ {
				dst = append(dst, base+k)
			}
			continue
		}
		for w != 0 {
			dst = append(dst, base+T(bits.TrailingZeros64(w)))
			w &= w - 1
		}
	}
	return dst
}

// ExpandBools sets mask to length entries, mask[i] reporting whether bit i is
// set, and returns it (reusing its capacity). Bits at or beyond n's length
// read as unset.
func (n *Bitmap) ExpandBools(mask []bool, length int) []bool {
	if cap(mask) < length {
		mask = make([]bool, length)
	} else {
		mask = mask[:length]
		clear(mask) // one bulk clear beats per-word clears for sparse bitmaps
	}
	if n.shift != 0 {
		for i := range n.nwords() {
			for w := n.lword(i); w != 0; w &= w - 1 {
				if k := i*64 + bits.TrailingZeros64(w); k < length {
					mask[k] = true
				}
			}
		}
		return mask
	}
	words, tail := n.liveWords()
	last := (n.numBits+63)/64 - 1
	for i, w := range words {
		if w == 0 {
			continue
		}
		if int64(i) == last {
			w &= tail
		}
		base := i * 64
		if base >= length {
			break
		}
		chunk := mask[base:min(base+64, length)]
		switch {
		case w == ^uint64(0):
			for k := range chunk {
				chunk[k] = true
			}
		case bits.OnesCount64(w) <= 16:
			for w != 0 {
				if k := bits.TrailingZeros64(w); k < len(chunk) {
					chunk[k] = true
				}
				w &= w - 1
			}
		default:
			for k := range chunk {
				chunk[k] = w>>uint(k)&1 != 0
			}
		}
	}
	return mask
}

// InitWithRows sets n to length bits with exactly rows set. It reuses n's
// word storage when it is large enough. Every row must be below length;
// duplicates are allowed.
func (n *Bitmap) InitWithRows(length int64, rows []int64, mp *mpool.MPool) error {
	if length < 0 {
		panic("negative bitmap length")
	}
	if err := n.prepareInit(mp); err != nil {
		return err
	}
	if err := n.resetWords(int((length + 63) / 64)); err != nil {
		return err
	}
	n.numBits = length
	data := n.data
	for _, row := range rows {
		data[uint64(row)>>6] |= 1 << (uint64(row) & 63)
	}
	var count int64
	for _, w := range data {
		count += int64(bits.OnesCount64(w))
	}
	n.popCount = count
	return nil
}

func (n *Bitmap) Marshal() []byte {
	var buf bytes.Buffer
	_ = n.MarshalTo(&buf)
	return buf.Bytes()
}

func (n *Bitmap) MarshalSize() int {
	if n == nil {
		return 0
	}
	if n.mp == nil {
		return MarshalHeaderSize + n.nwords()*8
	}
	return MarshalHeaderSize + len(n.data)*8
}

// Validate checks the in-memory representation after streaming decode.
func (n *Bitmap) Validate() error {
	if n == nil || n.numBits < 0 || n.popCount < 0 ||
		n.popCount > n.numBits ||
		len(n.data) != int((n.numBits+63)/64) {
		return moerr.NewInvalidInputNoCtx("invalid bitmap representation")
	}
	actual := int64(0)
	for i, word := range n.data {
		if i == len(n.data)-1 && n.numBits%64 != 0 &&
			word>>uint(n.numBits%64) != 0 {
			return moerr.NewInvalidInputNoCtx("invalid bitmap trailing bits")
		}
		actual += int64(bits.OnesCount64(word))
	}
	if actual != n.popCount {
		return moerr.NewInvalidInputNoCtx("invalid bitmap count")
	}
	return nil
}

// DecodeMarshalHeader validates the fixed bitmap wire header.
func DecodeMarshalHeader(data []byte) (
	count int64,
	bitLength int64,
	dataSize int,
	err error,
) {
	if len(data) < MarshalHeaderSize {
		return 0, 0, 0, io.ErrUnexpectedEOF
	}
	count = types.DecodeInt64(data[:8])
	rawBitLength := types.DecodeUint64(data[8:16])
	rawDataSize := types.DecodeUint64(data[16:24])
	if count < 0 ||
		rawBitLength > math.MaxInt64 ||
		rawDataSize > math.MaxInt ||
		rawDataSize%8 != 0 {
		return 0, 0, 0, moerr.NewInvalidInputNoCtx("invalid bitmap wire header")
	}
	bitLength = int64(rawBitLength)
	dataSize = int(rawDataSize)
	if count > bitLength ||
		uint64(dataSize/8) != (rawBitLength+63)/64 {
		return 0, 0, 0, moerr.NewInvalidInputNoCtx("invalid bitmap wire header")
	}
	return count, bitLength, dataSize, nil
}

// PrepareUnmarshal publishes a validated bitmap header and returns the
// freshly allocated payload bytes for the caller to fill, so a stream decoder
// can read the words in place.
func (n *Bitmap) PrepareUnmarshal(header []byte, totalSize int) ([]byte, error) {
	popCount, numBits, dataSize, err := DecodeMarshalHeader(header)
	if err != nil {
		return nil, err
	}
	if totalSize != MarshalHeaderSize+dataSize {
		return nil, moerr.NewInvalidInputNoCtx("invalid bitmap payload size")
	}
	if n.mp == nil {
		return nil, errNoPool()
	}
	if err = n.resetWords(dataSize / 8); err != nil {
		return nil, err
	}
	n.shift = 0
	n.popCount = popCount
	n.numBits = numBits
	return types.EncodeSlice(n.data), nil
}

func (n *Bitmap) MarshalTo(w io.Writer) error {
	if n == nil {
		return nil
	}
	if w == nil {
		return io.ErrClosedPipe
	}
	if n.mp == nil {
		return n.marshalReadOnlyTo(w)
	}
	bitLength := uint64(n.numBits)
	dataLength := uint64(len(n.data) * 8)
	if typed, ok := w.(interface {
		WriteInt64(int64) error
		WriteUint64(uint64) error
	}); ok {
		if err := typed.WriteInt64(n.popCount); err != nil {
			return err
		}
		if err := typed.WriteUint64(bitLength); err != nil {
			return err
		}
		if err := typed.WriteUint64(dataLength); err != nil {
			return err
		}
		return types.WriteFull(w, types.EncodeSlice(n.data))
	}
	if err := types.WriteFull(w, types.EncodeInt64(&n.popCount)); err != nil {
		return err
	}
	if err := types.WriteFull(w, types.EncodeUint64(&bitLength)); err != nil {
		return err
	}
	if err := types.WriteFull(w, types.EncodeUint64(&dataLength)); err != nil {
		return err
	}
	return types.WriteFull(w, types.EncodeSlice(n.data))
}

// MarshalV1 encodes the legacy version-1 layout: a 4-byte reserved word
// (written as zero), the bit length, the byte length, then the words.
func (n *Bitmap) MarshalV1() []byte {
	var buf bytes.Buffer
	reserved := int32(0)
	u1 := uint64(n.numBits)
	words := n.data
	if n.mp == nil {
		words = n.cleanWords()
	}
	u2 := uint64(len(words) * 8)
	buf.Write(types.EncodeInt32(&reserved))
	buf.Write(types.EncodeUint64(&u1))
	buf.Write(types.EncodeUint64(&u2))
	buf.Write(types.EncodeSlice(words))
	return buf.Bytes()
}

// Unmarshal decodes data into n. A bitmap with a pool copies the words into
// it; a bitmap without one references data, which must then outlive n.
func (n *Bitmap) Unmarshal(data []byte) error {
	if n.mp == nil {
		n.UnmarshalNoCopy(data)
		return nil
	}
	popCount := types.DecodeInt64(data[:8])
	numBits := int64(types.DecodeUint64(data[8:16]))
	size := int(types.DecodeUint64(data[16:24]))
	if err := n.resetWords(size / 8); err != nil {
		return err
	}
	copy(n.data, types.DecodeSlice[uint64](data[24:24+size]))
	n.shift = 0
	n.popCount = popCount
	n.numBits = numBits
	return nil
}

// UnmarshalV1 decodes the legacy version-1 layout written by MarshalV1,
// copying into n's pool when it has one and referencing data otherwise. The
// leading 4-byte reserved word is skipped and popCount is recomputed.
func (n *Bitmap) UnmarshalV1(data []byte) error {
	if n.mp == nil {
		n.UnmarshalNoCopyV1(data)
		return nil
	}
	numBits := int64(types.DecodeUint64(data[4:12]))
	size := int(types.DecodeUint64(data[12:20]))
	if err := n.resetWords(size / 8); err != nil {
		return err
	}
	copy(n.data, types.DecodeSlice[uint64](data[20:20+size]))
	n.shift = 0
	n.numBits = numBits
	n.popCount = 0
	for _, w := range n.data {
		n.popCount += int64(bits.OnesCount64(w))
	}
	return nil
}

// UnmarshalNoCopyV1 makes n a read-only bitmap over a version-1 encoding.
func (n *Bitmap) UnmarshalNoCopyV1(data []byte) {
	n.Free()
	n.mp = nil
	data = data[4:]
	n.numBits = int64(types.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		n.data = nil
	} else {
		n.data = types.DecodeSlice[uint64](data[:size])
	}
	n.popCount = 0
	for i := 0; i < len(n.data); i++ {
		n.popCount += int64(bits.OnesCount64(n.data[i]))
	}
}

func (n *Bitmap) String() string {
	return fmt.Sprintf("%v", n.ToArray())
}

var _ encoding.BinaryMarshaler = new(Bitmap)

func (n *Bitmap) MarshalBinary() ([]byte, error) {
	return n.Marshal(), nil
}

var _ encoding.BinaryUnmarshaler = new(Bitmap)

func (n *Bitmap) UnmarshalBinary(data []byte) error {
	return n.Unmarshal(data)
}

func ToArray[T int64 | uint64 | int | int32 | uint32](bm *Bitmap, rows *[]T) {
	*rows = AppendRows(bm, *rows)
}

// marshalReadOnlyTo is MarshalTo for a read-only bitmap: its words may be
// shifted or hold rows past Len, so it writes clean logical words.
func (n *Bitmap) marshalReadOnlyTo(w io.Writer) error {
	words := n.cleanWords()
	bitLength := uint64(n.numBits)
	dataLength := uint64(len(words) * 8)
	if err := types.WriteFull(w, types.EncodeInt64(&n.popCount)); err != nil {
		return err
	}
	if err := types.WriteFull(w, types.EncodeUint64(&bitLength)); err != nil {
		return err
	}
	if err := types.WriteFull(w, types.EncodeUint64(&dataLength)); err != nil {
		return err
	}
	return types.WriteFull(w, types.EncodeSlice(words))
}

// cleanWords returns ceil(Len/64) unshifted words with rows past Len clear,
// the layout an owned bitmap keeps.
func (n *Bitmap) cleanWords() []uint64 {
	words := make([]uint64, n.nwords())
	for i := range words {
		words[i] = n.lword(i)
	}
	return words
}
