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
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type DeltaDataNode struct {
	*buffer.Node

	Data containers.Vector

	deltaDataKey string

	colidx uint16
	typ    types.Type

	metaKey          string
	mgr              base.INodeManager
	deltaMetaFactory EvictableNodeFactory
}

func NewDeltaDataNode(mgr base.INodeManager, deltaDataKey, metaKey string, fs *objectio.ObjectFS, deltaloc string, colidx uint16, typ types.Type) (node *DeltaDataNode, err error) {
	node = &DeltaDataNode{
		deltaDataKey:     deltaDataKey,
		metaKey:          metaKey,
		mgr:              mgr,
		colidx:           colidx,
		typ:              typ,
		deltaMetaFactory: func() (base.INode, error) { return NewDeltaMetaNode(mgr, metaKey, fs, deltaloc), nil },
	}
	// For disk, size is zero, do not cache, read directly when GetData
	var size uint32 = 0
	if StorageBackend == S3 {
		// on s3, fetch coldata to get data size
		h, pinerr := PinEvictableNode(mgr, metaKey, node.deltaMetaFactory)
		if pinerr != nil {
			return nil, pinerr
		}
		defer h.Close()
		meta := h.GetNode().(*DeltaMetaNode)
		col, err := meta.GetColumn(colidx)
		if err != nil {
			return nil, err
		}
		size = col.GetMeta().GetLocation().OriginSize()
	}

	node.Node = buffer.NewNode(node, mgr, deltaDataKey, uint64(size))
	node.LoadFunc = node.onLoad
	node.UnloadFunc = node.onUnload
	node.HardEvictableFunc = func() bool { return true }
	return
}

func (n *DeltaDataNode) onLoad() {
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

func (n *DeltaDataNode) fetchData() (containers.Vector, error) {
	var h base.INodeHandle
	var err error
	h, err = PinEvictableNode(n.mgr, n.metaKey, n.deltaMetaFactory)
	if err != nil {
		return nil, err
	}
	defer h.Close()

	meta := h.GetNode().(*DeltaMetaNode)
	col, err := meta.GetColumn(n.colidx)
	if err != nil {
		return nil, err
	}

	// Do IO, fetch data buf
	fsVector, err := col.GetData(nil)
	if err != nil {
		return nil, err
	}

	srcBuf := fsVector.Entries[0].Data
	v := vector.New(n.typ)
	v.Read(srcBuf)
	return containers.NewVectorWithSharedMemory(v, false /* rowid committs abort are all non-nullable */), nil
}

func (n *DeltaDataNode) GetData(buf *bytes.Buffer) (containers.Vector, error) {
	// after loading, for s3, its data is n.Data
	switch StorageBackend {
	case Disk:
		// for disk, read directly
		return n.fetchData()
	case S3:
		return copyVector(n.Data, buf), nil
	}
	return nil, nil
}

func (n *DeltaDataNode) onUnload() {
	n.Data = nil
}

func FetchDeltaData(buf *bytes.Buffer, mgr base.INodeManager, fs *objectio.ObjectFS, deltaloc string, colidx uint16, typ types.Type) (res containers.Vector, err error) {
	deltaDataKey := EncodeDeltaDataKey(colidx, deltaloc)
	factory := func() (base.INode, error) {
		return NewDeltaDataNode(mgr, deltaDataKey, EncodeDeltaMetaKey(deltaloc), fs, deltaloc, colidx, typ)
	}
	h, err := PinEvictableNode(mgr, deltaDataKey, factory)
	if err != nil {
		return nil, err
	}
	defer h.Close()
	node := h.GetNode().(*DeltaDataNode)
	return node.GetData(buf)
}
