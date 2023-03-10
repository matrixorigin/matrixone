// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package evictable

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type ColDataNode struct {
	*buffer.Node

	Data containers.Vector

	colDataKey string
	def        *catalog.ColDef

	// used for rowid
	rows     uint32
	sid, bid uint64

	// used for other columns
	metaKey        string
	mgr            base.INodeManager
	colMetaFactory EvictableNodeFactory
}

type BackendKind = uint8

const (
	Disk = iota
	S3
)

var StorageBackend BackendKind = Disk

func NewColDataNode(
	id *common.ID,
	def *catalog.ColDef,
	colDataKey string,
	metaKey string,
	metaloc string,
	mgr base.INodeManager,
	fs *objectio.ObjectFS) (node *ColDataNode, err error) {
	node = &ColDataNode{
		colDataKey: colDataKey,
		metaKey:    metaKey,
		sid:        id.SegmentID,
		bid:        id.BlockID,
		mgr:        mgr,
		def:        def,
		colMetaFactory: func() (base.INode, error) {
			return NewColumnMetaNode(
				id.Idx,
				def.Type,
				metaloc,
				metaKey,
				mgr,
				fs), nil
		},
	}
	// For disk, size is zero, do not cache, read directly when GetData
	var size uint32 = 0
	if node.def.IsPhyAddr() {
		_, _, node.rows = blockio.DecodeMetaLoc(metaloc)
		size = types.RowidSize * node.rows
	} else if StorageBackend == S3 {
		// on s3, fetch coldata to get data size
		h, pinerr := PinEvictableNode(mgr, metaKey, node.colMetaFactory)
		if pinerr != nil {
			return nil, pinerr
		}
		defer h.Close()
		meta := h.GetNode().(*ColumnMetaNode)
		size = meta.GetMeta().GetLocation().OriginSize()
	}

	node.Node = buffer.NewNode(node, mgr, colDataKey, uint64(size))
	node.LoadFunc = node.onLoad
	node.UnloadFunc = node.onUnload
	node.HardEvictableFunc = func() bool { return true }
	return
}

func (n *ColDataNode) onLoad() {
	if n.def.IsPhyAddr() {
		n.constructRowId()
		return
	}
	switch StorageBackend {
	case S3:
		// fetch data via s3 and cache it
		// TODOa: error handling
		data, _ := n.fetchData()
		n.Data = data
	case Disk:
		// for disk, do nothing when load
	}
}

func (n *ColDataNode) constructRowId() {
	prefix := model.EncodeBlockKeyPrefix(n.sid, n.bid)
	n.Data, _ = model.PreparePhyAddrData(
		types.T_Rowid.ToType(),
		prefix,
		0,
		n.rows,
	)
}

func (n *ColDataNode) fetchData() (containers.Vector, error) {
	var h base.INodeHandle
	var err error
	h, err = PinEvictableNode(n.mgr, n.metaKey, n.colMetaFactory)
	if err != nil {
		return nil, err
	}
	metaNode := h.GetNode().(*ColumnMetaNode)
	defer h.Close()

	// Do IO, fetch data buf
	fsVector, err := metaNode.GetData(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	//srcBuf := fsVector.Entries[0].Object.([]byte)
	srcBuf := make([]byte, len(fsVector.Entries[0].Object.([]byte)))
	copy(srcBuf, fsVector.Entries[0].Object.([]byte))
	v := vector.New(n.def.Type)
	v.Read(srcBuf)
	return containers.NewVectorWithSharedMemory(v, n.def.NullAbility), nil
}

func (n *ColDataNode) GetData(buf *bytes.Buffer) (containers.Vector, error) {
	// after load, for s3 and phy addr, its data is n.Data
	if n.def.IsPhyAddr() {
		return copyVector(n.Data, buf), nil
	}
	switch StorageBackend {
	case Disk:
		// for disk, read directly
		return n.fetchData()
	case S3:
		return copyVector(n.Data, buf), nil
	}
	return nil, nil
}

func (n *ColDataNode) onUnload() {
	if n.Data != nil {
		n.Data.Close()
		n.Data = nil
	}
}

func FetchColumnData(
	id *common.ID,
	def *catalog.ColDef,
	metaloc string,
	buf *bytes.Buffer,
	mgr base.INodeManager,
	fs *objectio.ObjectFS) (res containers.Vector, err error) {
	id.Idx = uint16(def.Idx)
	colDataKey := EncodeColDataKey(id.Idx, metaloc)
	factory := func() (base.INode, error) {
		return NewColDataNode(
			id,
			def,
			colDataKey,
			EncodeColMetaKey(id.Idx, metaloc),
			metaloc,
			mgr,
			fs)
	}
	h, err := PinEvictableNode(mgr, colDataKey, factory)
	if err != nil {
		return nil, err
	}
	defer h.Close()
	node := h.GetNode().(*ColDataNode)
	return node.GetData(buf)
}
