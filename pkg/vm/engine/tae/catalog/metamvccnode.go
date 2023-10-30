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

package catalog

import (
	"fmt"
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type MetadataMVCCNode struct {
	MetaLoc  objectio.Location
	DeltaLoc objectio.Location
}

func NewEmptyMetadataMVCCNode() *MetadataMVCCNode {
	return &MetadataMVCCNode{}
}

func (e *MetadataMVCCNode) CloneAll() *MetadataMVCCNode {
	node := &MetadataMVCCNode{
		MetaLoc:  e.MetaLoc,
		DeltaLoc: e.DeltaLoc,
	}
	return node
}

func (e *MetadataMVCCNode) CloneData() *MetadataMVCCNode {
	return &MetadataMVCCNode{
		MetaLoc:  e.MetaLoc,
		DeltaLoc: e.DeltaLoc,
	}
}

func (e *MetadataMVCCNode) String() string {

	return fmt.Sprintf("[MetaLoc=\"%s\",DeltaLoc=\"%s\"]",
		e.MetaLoc.String(),
		e.DeltaLoc.String())
}

// for create drop in one txn
func (e *MetadataMVCCNode) Update(un *MetadataMVCCNode) {
	if !un.MetaLoc.IsEmpty() {
		e.MetaLoc = un.MetaLoc
	}
	if !un.DeltaLoc.IsEmpty() {
		e.DeltaLoc = un.DeltaLoc
	}
}

func (e *MetadataMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	var sn int64
	if sn, err = objectio.WriteBytes(e.MetaLoc, w); err != nil {
		return
	}
	n += sn
	if sn, err = objectio.WriteBytes(e.DeltaLoc, w); err != nil {
		return
	}
	n += sn
	return
}

func (e *MetadataMVCCNode) ReadFromWithVersion(r io.Reader, ver uint16) (n int64, err error) {
	var sn int64
	if e.MetaLoc, sn, err = objectio.ReadBytes(r); err != nil {
		return
	}
	n += sn
	if e.DeltaLoc, sn, err = objectio.ReadBytes(r); err != nil {
		return
	}
	n += sn
	return
}

type ObjectMVCCNode struct {
	Name           objectio.ObjectName
	OriginSize     uint32
	CompressedSize uint32
	ZoneMap        index.ZM
	BlockNumber    uint16
}

func NewEmptyObjectMVCCNode() *ObjectMVCCNode {
	return &ObjectMVCCNode{}
}

// TODO
func NewObjectInfoWithMetaLocation(metalocation objectio.Location) *ObjectMVCCNode {
	return &ObjectMVCCNode{
		Name: metalocation.Name(),
	}
}

func (e *ObjectMVCCNode) CloneAll() *ObjectMVCCNode {
	return &ObjectMVCCNode{
		Name:           e.Name,
		OriginSize:     e.OriginSize,
		CompressedSize: e.CompressedSize,
		ZoneMap:        e.ZoneMap,
		BlockNumber:    e.BlockNumber,
	}
}
func (e *ObjectMVCCNode) CloneData() *ObjectMVCCNode {
	return &ObjectMVCCNode{
		Name:           e.Name,
		OriginSize:     e.OriginSize,
		CompressedSize: e.CompressedSize,
		ZoneMap:        e.ZoneMap,
		BlockNumber:    e.BlockNumber,
	}
}
func (e *ObjectMVCCNode) String() string {
	return e.Name.String()
}
func (e *ObjectMVCCNode) Update(vun *ObjectMVCCNode) {
	e.Name = vun.Name
	e.OriginSize = vun.OriginSize
	e.CompressedSize = vun.CompressedSize
	e.ZoneMap = vun.ZoneMap
	e.BlockNumber = vun.BlockNumber
}
func (e *ObjectMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	var sn int64
	if sn, err = objectio.WriteBytes(e.Name, w); err != nil {
		return
	}
	n += sn
	if _, err = w.Write(types.EncodeUint32(&e.OriginSize)); err != nil {
		return
	}
	n += 4
	if _, err = w.Write(types.EncodeUint32(&e.CompressedSize)); err != nil {
		return
	}
	n += 4
	if sn, err = objectio.WriteBytes(e.ZoneMap, w); err != nil {
		return
	}
	n += sn
	if _, err = w.Write(types.EncodeUint16(&e.BlockNumber)); err != nil {
		return
	}
	n += 2
	return
}
func (e *ObjectMVCCNode) ReadFromWithVersion(r io.Reader, ver uint16) (n int64, err error) {
	var sn int64
	if e.Name, sn, err = objectio.ReadBytes(r); err != nil {
		return
	}
	n += sn
	var sn2 int
	if sn2, err = r.Read(types.EncodeUint32(&e.OriginSize)); err != nil {
		return
	}
	n += int64(sn2)
	if sn2, err = r.Read(types.EncodeUint32(&e.CompressedSize)); err != nil {
		return
	}
	n += int64(sn2)
	if e.ZoneMap, sn, err = objectio.ReadBytes(r); err != nil {
		return
	}
	n += sn
	if sn2, err = r.Read(types.EncodeUint16(&e.BlockNumber)); err != nil {
		return
	}
	n += int64(sn2)
	return
}

func (e *ObjectMVCCNode) IsEmpty() bool {
	return e.OriginSize == 0
}

func (e *ObjectMVCCNode) AppendTuple(batch *containers.Batch) {
	batch.GetVectorByName(ObjectAttr_Name).Append(e.Name, false)
	batch.GetVectorByName(ObjectAttr_OriginSize).Append(e.OriginSize, false)
	batch.GetVectorByName(ObjectAttr_CompressedSize).Append(e.CompressedSize, false)
	batch.GetVectorByName(ObjectAttr_ZoneMap).Append(e.ZoneMap, false)
	batch.GetVectorByName(ObjectAttr_BlockNumber).Append(e.BlockNumber, false)
}

func ReadObjectInfoTuple(bat *containers.Batch, row int) (e *ObjectMVCCNode) {
	e = &ObjectMVCCNode{
		Name:           bat.GetVectorByName(ObjectAttr_Name).Get(row).([]byte),
		OriginSize:     bat.GetVectorByName(ObjectAttr_OriginSize).Get(row).(uint32),
		CompressedSize: bat.GetVectorByName(ObjectAttr_CompressedSize).Get(row).(uint32),
		ZoneMap:        bat.GetVectorByName(ObjectAttr_ZoneMap).Get(row).([]byte),
		BlockNumber:    bat.GetVectorByName(ObjectAttr_BlockNumber).Get(row).(uint16),
	}
	return
}

type SegmentNode struct {
	state    EntryState
	IsLocal  bool   // this segment is hold by a localsegment
	SortHint uint64 // sort segment by create time, make iteration on segment determined
	// used in appendable segment, bump this if creating a new block, and
	// the block will be eventually flushed to a s3 file.
	// for non-appendable segment, this field makes no sense, because if we
	// decide to create a new non-appendable segment, its content is all set.
	nextObjectIdx uint16
	sorted        bool // deprecated
}

const (
	BlockNodeSize int64 = int64(unsafe.Sizeof(BlockNode{}))
)

// not marshal nextObjectIdx
func (node *SegmentNode) ReadFrom(r io.Reader) (n int64, err error) {
	_, err = r.Read(types.EncodeInt8((*int8)(&node.state)))
	if err != nil {
		return
	}
	n += 1
	_, err = r.Read(types.EncodeBool(&node.IsLocal))
	if err != nil {
		return
	}
	n += 1
	_, err = r.Read(types.EncodeUint64(&node.SortHint))
	if err != nil {
		return
	}
	n += 8
	_, err = r.Read(types.EncodeBool(&node.sorted))
	if err != nil {
		return
	}
	n += 1
	return
}

func (node *SegmentNode) WriteTo(w io.Writer) (n int64, err error) {
	_, err = w.Write(types.EncodeInt8((*int8)(&node.state)))
	if err != nil {
		return
	}
	n += 1
	_, err = w.Write(types.EncodeBool(&node.IsLocal))
	if err != nil {
		return
	}
	n += 1
	_, err = w.Write(types.EncodeUint64(&node.SortHint))
	if err != nil {
		return
	}
	n += 8
	_, err = w.Write(types.EncodeBool(&node.sorted))
	if err != nil {
		return
	}
	n += 1
	return
}
func (node *SegmentNode) String() string {
	sorted := "US"
	if node.sorted {
		sorted = "S"
	}
	return fmt.Sprintf("%s/%d/%d", sorted, node.SortHint, node.nextObjectIdx)
}

type BlockNode struct {
	state EntryState
}

func EncodeBlockNode(node *BlockNode) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(node)), BlockNodeSize)
}

func (node *BlockNode) ReadFrom(r io.Reader) (n int64, err error) {
	if _, err = r.Read(EncodeBlockNode(node)); err != nil {
		return
	}
	n += BlockNodeSize
	return
}

func (node *BlockNode) WriteTo(w io.Writer) (n int64, err error) {
	if _, err = w.Write(EncodeBlockNode(node)); err != nil {
		return
	}
	n += BlockNodeSize
	return
}
