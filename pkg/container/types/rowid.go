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
	return bytes.Compare(a[:], b[:])
}

func CompareBlockidBlockidAligned(a, b Blockid) int {
	return bytes.Compare(a[:], b[:])
}

func (r Rowid) Compare(other Rowid) int {
	return bytes.Compare(r[:], other[:])
}

func (r Rowid) Less(than Rowid) bool {
	return bytes.Compare(r[:], than[:]) < 0
}

func (r Rowid) Le(than Rowid) bool {
	return r.Less(than) || r.Equal(than)
}

func (r Rowid) Equal(to Rowid) bool {
	return bytes.Equal(r[:], to[:])
}

func (r Rowid) NotEqual(to Rowid) bool {
	return !r.Equal(to)
}

func (r Rowid) Great(than Rowid) bool {
	return !r.Less(than)
}

func (r Rowid) Ge(than Rowid) bool {
	return r.Great(than) || r.Equal(than)
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

func (r Rowid) Decode() (Blockid, uint32) {
	b := *(*Blockid)(r[:BlockidSize])
	s := DecodeUint32(r[BlockidSize:])
	return b, s
}

func (r Rowid) GetObject() ObjectBytes {
	return *(*ObjectBytes)(r[:ObjectBytesSize])
}

func (r *Rowid) BorrowObjectID() *Objectid {
	return (*Objectid)(unsafe.Pointer(&r[0]))
}
func (r *Rowid) SetRowOffset(offset uint32) {
	copy(r[BlockidSize:], EncodeUint32(&offset))
}

func (r Rowid) GetRowOffset() uint32 {
	return DecodeUint32(r[BlockidSize:])
}

func (r Rowid) GetBlockOffset() uint16 {
	return DecodeUint16(r[ObjectBytesSize:BlockidSize])
}

func (r Rowid) GetObjectString() string {
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

func (b Blockid) Less(than Blockid) bool {
	return b.Compare(than) < 0
}

func (b Blockid) Great(than Blockid) bool {
	return b.Compare(than) > 0
}

func RandomRowid() Rowid {
	var r Rowid
	u, _ := uuid.NewV7()
	copy(r[:], u[:])
	return r
}

func (b Blockid) Compare(other Blockid) int {
	return bytes.Compare(b[:], other[:])
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

func (o *Objectid) Eq(other Objectid) bool {
	return bytes.Equal(o[:], other[:])
}

func (o *Objectid) Le(other Objectid) bool {
	return bytes.Compare(o[:], other[:]) <= 0
}
func (o *Objectid) Ge(other Objectid) bool {
	return bytes.Compare(o[:], other[:]) >= 0
}
func (o *Objectid) Lt(other Objectid) bool {
	return bytes.Compare(o[:], other[:]) < 0
}
func (o *Objectid) Gt(other Objectid) bool {
	return bytes.Compare(o[:], other[:]) > 0
}
