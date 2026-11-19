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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// This file holds the Bitmap operations outside the Gobra-verified core in
// bitmap.go: copying, remapping, serialization and helpers,
// generics or packages Gobra cannot model. They must preserve the invariants
// documented in bitmap.go.

const MarshalHeaderSize = 24

func (n *Bitmap) InitWith(m *Bitmap) {
	if n == m {
		return
	}
	n.numBits = m.numBits
	n.popCount = m.popCount
	n.data = append([]uint64(nil), m.data...)
}

// InitWithInvertedBits sets n to length bits read LSB-first from src
// starting at bit offset, each bit inverted. It turns an Arrow validity
// bitmap (1 = valid) into a null bitmap (1 = null). src must hold at least
// offset+length bits.
func (n *Bitmap) InitWithInvertedBits(src []byte, offset, length int) {
	n.InitWithSize(int64(length))
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
}

func (n *Bitmap) Clone() *Bitmap {
	if n == nil {
		return nil
	}
	var res Bitmap
	res.InitWith(n)
	return &res
}

func (n *Bitmap) Iterator() Iterator {
	itr := n.IteratorValue()
	return &itr
}

func (n *Bitmap) Ptr() *uint64 {
	if n == nil || len(n.data) == 0 {
		return nil
	}
	return &n.data[0]
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

	if n.EmptyByFlag() {
		return res
	}

	itr := n.Iterator()
	for itr.HasNext() {
		r := itr.Next()
		res = append(res, int64(r))
	}
	return res
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
	n.data = make([]uint64, dataSize/8)
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

// MarshalV1 in version 1, Bitmap.emptyFlag is type int32, now we use Bitmap.popCount replace it
func (n *Bitmap) MarshalV1() []byte {
	var buf bytes.Buffer
	empty := int32(0)
	u1 := uint64(n.numBits)
	u2 := uint64(len(n.data) * 8)
	buf.Write(types.EncodeInt32(&empty))
	buf.Write(types.EncodeUint64(&u1))
	buf.Write(types.EncodeUint64(&u2))
	buf.Write(types.EncodeSlice(n.data))
	return buf.Bytes()
}

func (n *Bitmap) Unmarshal(data []byte) {
	n.popCount = types.DecodeInt64(data[:8])
	data = data[8:]
	n.numBits = int64(types.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		n.data = nil
	} else {
		n.data = types.DecodeSlice[uint64](data[:size])
	}
}

func (n *Bitmap) UnmarshalNoCopy(data []byte) {
	n.popCount = types.DecodeInt64(data[:8])
	data = data[8:]
	n.numBits = int64(types.DecodeUint64(data[:8]))
	data = data[8:]
	size := int(types.DecodeUint64(data[:8]))
	data = data[8:]
	if size == 0 {
		n.data = nil
	} else {
		n.data = types.DecodeSlice[uint64](data[:size])
	}
}

// UnmarshalV1 in version 1, Bitmap.emptyFlag is type int32, now we use Bitmap.popCount replace it
func (n *Bitmap) UnmarshalV1(data []byte) {
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

func (n *Bitmap) UnmarshalNoCopyV1(data []byte) {
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
	n.Unmarshal(data)
	return nil
}

func ToArray[T int64 | uint64 | int | int32 | uint32](bm *Bitmap, rows *[]T) {
	if bm.IsEmpty() {
		return
	}
	it := bm.Iterator()
	for it.HasNext() {
		*rows = append(*rows, T(it.Next()))
	}
}
