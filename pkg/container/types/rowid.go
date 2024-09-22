// Copyright 2023 Matrix Origin
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
	"encoding/hex"
	"fmt"
	"unsafe"

	"github.com/google/uuid"
)

/*

[SegUUID-16bytes] [ObjectOffset-2bytes] [BlockOffset-2bytes] [RowOffset-4bytes]
--------------------------------------- s3 file name
------------------------------------------------------------ block id
------------------------------------------------------------------------------ rowid

*/

var EmptyRowid Rowid

const ObjectBytesSize = 18

type ObjectBytes = [ObjectBytesSize]byte

// BuildTestRowid used only in unit test.
func BuildTestRowid(a, b int64) (ret Rowid) {
	copy(ret[0:8], EncodeInt64(&a))
	copy(ret[8:16], EncodeInt64(&b))
	copy(ret[20:], EncodeInt64(&b))
	return
}

// BuildTestBlockid used only in unit test
func BuildTestBlockid(a, b int64) (ret Blockid) {
	copy(ret[0:8], EncodeInt64(&a))
	copy(ret[8:16], EncodeInt64(&b))
	return
}

func NewObjectid() *Objectid {
	sid := Uuid(uuid.Must(uuid.NewV7()))
	var oid Objectid
	copy(oid[:UuidSize], sid[:])
	return &oid
}

func NewBlockidWithObjectID(id *Objectid, blknum uint16) *Blockid {
	var bid Blockid
	size := ObjectidSize
	copy(bid[:size], id[:])
	copy(bid[size:size+2], EncodeUint16(&blknum))
	return &bid
}

func NewRowid(blkid *Blockid, offset uint32) *Rowid {
	var rowid Rowid
	size := BlockidSize
	copy(rowid[:size], blkid[:])
	copy(rowid[size:size+4], EncodeUint32(&offset))
	return &rowid
}

func NewRowIDWithObjectIDBlkNumAndRowID(id Objectid, blknum uint16, offset uint32) Rowid {
	var rowID Rowid
	size := ObjectidSize
	copy(rowID[:size], id[:])
	copy(rowID[size:size+2], EncodeUint16(&blknum))
	copy(rowID[size+2:], EncodeUint32(&offset))
	return rowID
}

func CompareRowidRowidAligned(a, b Rowid) int {
	return a.Compare(&b)
}

func CompareBlockidBlockidAligned(a, b Blockid) int {
	return a.Compare(&b)
}

func (r *Rowid) ComparePrefix(to []byte) int {
	toLen := len(to)
	if toLen == BlockidSize {
		v1 := (*Blockid)(unsafe.Pointer(&r[0]))
		v2 := (*Blockid)(unsafe.Pointer(&to[0]))
		return v1.Compare(v2)
	}
	if toLen == RowidSize {
		toId := (*Rowid)(unsafe.Pointer(&to[0]))
		return r.Compare(toId)
	}
	if toLen == ObjectidSize {
		v1 := (*Objectid)(unsafe.Pointer(&r[0]))
		v2 := (*Objectid)(unsafe.Pointer(&to[0]))
		return v1.Compare(v2)
	}
	if toLen == SegmentidSize {
		return bytes.Compare(r[:toLen], to)
	}
	panic(fmt.Sprintf("invalid prefix length %d:%X", toLen, to))
}

func (r *Rowid) Compare(other *Rowid) int {
	if v := bytes.Compare(r[:SegmentidSize], other[:SegmentidSize]); v != 0 {
		return v
	}
	filen1 := *(*uint16)(unsafe.Pointer(&r[SegmentidSize]))
	filen2 := *(*uint16)(unsafe.Pointer(&other[SegmentidSize]))
	if filen1 < filen2 {
		return -1
	} else if filen1 > filen2 {
		return 1
	}
	blk1 := *(*uint16)(unsafe.Pointer(&r[ObjectidSize]))
	blk2 := *(*uint16)(unsafe.Pointer(&other[ObjectidSize]))
	if blk1 < blk2 {
		return -1
	} else if blk1 > blk2 {
		return 1
	}
	row1 := *(*uint32)(unsafe.Pointer(&r[BlockidSize]))
	row2 := *(*uint32)(unsafe.Pointer(&other[BlockidSize]))
	if row1 < row2 {
		return -1
	} else if row1 > row2 {
		return 1
	}
	return 0
}

func (r *Rowid) LT(than *Rowid) bool {
	return r.Compare(than) < 0
}

func (r *Rowid) LE(than *Rowid) bool {
	return r.Compare(than) <= 0
}

func (r *Rowid) EQ(to *Rowid) bool {
	return r.Compare(to) == 0
}

func (r *Rowid) GT(than *Rowid) bool {
	return r.Compare(than) > 0
}

func (r *Rowid) GE(than *Rowid) bool {
	return r.Compare(than) >= 0
}

// CloneBlockID clones the block id from row id.
func (r *Rowid) CloneBlockID() Blockid {
	return *(*Blockid)(unsafe.Pointer(&r[0]))
}

// BorrowBlockID borrows block id from row id.
func (r *Rowid) BorrowBlockID() *Blockid {
	return (*Blockid)(unsafe.Pointer(&r[0]))
}

// BorrowSegmentID borrows segment id from row id.
func (r *Rowid) BorrowSegmentID() *Segmentid {
	return (*Segmentid)(unsafe.Pointer(&r[0]))
}

// CloneSegmentID clones segment id from row id.
func (r *Rowid) CloneSegmentID() Segmentid {
	return *(*Segmentid)(unsafe.Pointer(&r[0]))
}

// PXU TODO:
func (r *Rowid) Decode() (Blockid, uint32) {
	b := *(*Blockid)(r[:BlockidSize])
	s := DecodeUint32(r[BlockidSize:])
	return b, s
}

func (r *Rowid) GetObject() ObjectBytes {
	return *(*ObjectBytes)(r[:ObjectBytesSize])
}

func (r *Rowid) BorrowObjectID() *Objectid {
	return (*Objectid)(unsafe.Pointer(&r[0]))
}
func (r *Rowid) SetRowOffset(offset uint32) {
	copy(r[BlockidSize:], EncodeUint32(&offset))
}

func (r *Rowid) GetRowOffset() uint32 {
	return DecodeUint32(r[BlockidSize:])
}

func (r *Rowid) GetBlockOffset() uint16 {
	return DecodeUint16(r[ObjectBytesSize:BlockidSize])
}

func (r *Rowid) GetObjectString() string {
	uuid := (*uuid.UUID)(r[:UuidSize])
	s := DecodeUint16(r[UuidSize:ObjectBytesSize])
	return fmt.Sprintf("%s-%d", uuid.String(), s)
}

func (r *Rowid) String() string {
	b := (*Blockid)(unsafe.Pointer(&r[0]))
	s := DecodeUint32(r[BlockidSize:])
	return fmt.Sprintf("%s-%d", b.String(), s)
}

func (r *Rowid) ShortStringEx() string {
	b := (*Blockid)(unsafe.Pointer(&r[0]))
	s := DecodeUint32(r[BlockidSize:])
	return fmt.Sprintf("%s-%d", b.ShortStringEx(), s)
}

func (b *Blockid) LT(than *Blockid) bool {
	return b.Compare(than) < 0
}

func (b *Blockid) GT(than *Blockid) bool {
	return b.Compare(than) > 0
}

func RandomRowid() Rowid {
	var r Rowid
	u, _ := uuid.NewV7()
	copy(r[:], u[:])
	return r
}

func (b *Blockid) Compare(other *Blockid) int {
	if r := bytes.Compare(b[:SegmentidSize], other[:SegmentidSize]); r != 0 {
		return r
	}
	filen1 := *(*uint16)(unsafe.Pointer(&b[SegmentidSize]))
	filen2 := *(*uint16)(unsafe.Pointer(&other[SegmentidSize]))
	if filen1 < filen2 {
		return -1
	} else if filen1 > filen2 {
		return 1
	}
	blk1 := *(*uint16)(unsafe.Pointer(&b[ObjectidSize]))
	blk2 := *(*uint16)(unsafe.Pointer(&other[ObjectidSize]))
	if blk1 < blk2 {
		return -1
	} else if blk1 > blk2 {
		return 1
	}
	return 0
}

func (b *Blockid) IsEmpty() bool {
	for _, v := range b[:] {
		if v != 0 {
			return false
		}
	}
	return true
}

func (b *Blockid) String() string {
	uuid := (*uuid.UUID)(b[:UuidSize])
	filen, blkn := b.Offsets()
	return fmt.Sprintf("%s-%d-%d", uuid.String(), filen, blkn)
}

func (b *Blockid) ObjectNameString() string {
	fileNum, _ := b.Offsets()
	return fmt.Sprintf("%v_%05d", b.Segment().String(), fileNum)
}

func (b *Blockid) ShortString() string {
	filen, blkn := b.Offsets()
	return fmt.Sprintf("%d-%d", filen, blkn)
}

func (b *Blockid) ShortStringEx() string {
	var shortuuid [12]byte
	hex.Encode(shortuuid[:], b[10:16])
	filen, blkn := b.Offsets()
	return fmt.Sprintf("%s-%d-%d", string(shortuuid[:]), filen, blkn)
}

func (b *Blockid) Offsets() (uint16, uint16) {
	filen := DecodeUint16(b[UuidSize:ObjectBytesSize])
	blkn := DecodeUint16(b[ObjectBytesSize:BlockidSize])
	return filen, blkn
}

func (b *Blockid) Segment() *Segmentid {
	return (*Uuid)(unsafe.Pointer(&b[0]))
}

func (b *Blockid) Object() *Objectid {
	return (*Objectid)(unsafe.Pointer(&b[0]))
}

func (b *Blockid) Sequence() uint16 {
	return DecodeUint16(b[ObjectBytesSize:BlockidSize])
}
func (o *Objectid) Segment() *Segmentid {
	return (*Uuid)(unsafe.Pointer(&o[0]))
}
func (o *Objectid) String() string {
	return fmt.Sprintf("%v_%d", o.Segment().String(), o.Offset())
}
func (o *Objectid) ShortStringEx() string {
	var shortuuid [12]byte
	hex.Encode(shortuuid[:], o[10:16])
	return string(shortuuid[:])
}
func (o *Objectid) Offset() uint16 {
	filen := DecodeUint16(o[UuidSize:ObjectBytesSize])
	return filen
}

func (o *Objectid) Compare(other *Objectid) int {
	if v := bytes.Compare(o[:SegmentidSize], other[:SegmentidSize]); v != 0 {
		return v
	}
	filen1 := *(*uint16)(unsafe.Pointer(&o[SegmentidSize]))
	filen2 := *(*uint16)(unsafe.Pointer(&other[SegmentidSize]))
	if filen1 < filen2 {
		return -1
	} else if filen1 > filen2 {
		return 1
	}
	return 0
}

func (o *Objectid) EQ(other *Objectid) bool {
	return o.Compare(other) == 0
}

func (o *Objectid) LE(other *Objectid) bool {
	return o.Compare(other) <= 0
}
func (o *Objectid) GE(other *Objectid) bool {
	return o.Compare(other) >= 0
}
func (o *Objectid) LT(other *Objectid) bool {
	return o.Compare(other) < 0
}
func (o *Objectid) GT(other *Objectid) bool {
	return o.Compare(other) > 0
}
