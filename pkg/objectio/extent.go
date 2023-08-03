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

package objectio

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Alg | Offset | Length | OriginSize
// ----|--------|--------|------------
// 1   | 4      | 4      | 4
// Alg: Specifies the compression algorithm
// Offset: The offset of the compressed data in the file
// Length: The length of the compressed data
// OriginSize: The length of the original data
type Extent []byte

const (
	extentAlgLen    = 1
	extentOffsetOff = extentAlgLen
	extentOffsetLen = 4
	extentLengthOff = extentOffsetOff + extentOffsetLen
	extentLengthLen = 4
	extentOriginOff = extentLengthOff + extentLengthLen
	extentOriginLen = 4
	ExtentSize      = extentOriginOff + extentOriginLen
)

func NewExtent(alg uint8, offset, length, originSize uint32) Extent {
	var extent [ExtentSize]byte
	copy(extent[:extentAlgLen], types.EncodeUint8(&alg))
	copy(extent[extentOffsetOff:extentLengthOff], types.EncodeUint32(&offset))
	copy(extent[extentLengthOff:extentOriginOff], types.EncodeUint32(&length))
	copy(extent[extentOriginOff:ExtentSize], types.EncodeUint32(&originSize))
	return extent[:]
}

func (ex Extent) Alg() uint8 {
	return types.DecodeUint8(ex[:extentAlgLen])
}

func (ex Extent) SetAlg(alg uint8) {
	copy(ex[:extentAlgLen], types.EncodeUint8(&alg))
}

func (ex Extent) End() uint32 {
	return ex.Offset() + ex.Length()
}

func (ex Extent) Offset() uint32 {
	return types.DecodeUint32(ex[extentOffsetOff:extentLengthOff])
}

func (ex Extent) SetOffset(offset uint32) {
	copy(ex[extentOffsetOff:extentLengthOff], types.EncodeUint32(&offset))
}

func (ex Extent) Length() uint32 {
	return types.DecodeUint32(ex[extentLengthOff:extentOriginOff])
}

func (ex Extent) SetLength(length uint32) {
	copy(ex[extentLengthOff:extentOriginOff], types.EncodeUint32(&length))
}

func (ex Extent) OriginSize() uint32 {
	return types.DecodeUint32(ex[extentOriginOff:ExtentSize])
}

func (ex Extent) SetOriginSize(originSize uint32) {
	copy(ex[extentOriginOff:ExtentSize], types.EncodeUint32(&originSize))
}

func (ex Extent) String() string {
	return fmt.Sprintf("%d_%d_%d_%d", ex.Alg(), ex.Offset(), ex.Length(), ex.OriginSize())
}
