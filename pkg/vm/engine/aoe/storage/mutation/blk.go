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

package mutation

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type blockFlusher struct{}

func (f blockFlusher) flush(node base.INode, data batch.IBatch, meta *metadata.Block, file *dataio.TransientBlockFile) error {
	return file.Sync(data, meta)
}

type MutableBlockNode struct {
	buffer.Node
	TableData iface.ITableData
	Meta      *metadata.Block
	File      *dataio.TransientBlockFile
	Data      batch.IBatch
	Flusher   mb.BlockFlusher
	Stale     *atomic.Value
}

func NewMutableBlockNode(mgr base.INodeManager, file *dataio.TransientBlockFile,
	tabledata iface.ITableData, meta *metadata.Block, flusher mb.BlockFlusher, initSize uint64) *MutableBlockNode {
	if flusher == nil {
		t := blockFlusher{}
		flusher = t.flush
	}
	n := &MutableBlockNode{
		File:      file,
		Meta:      meta,
		TableData: tabledata,
		Flusher:   flusher,
		Stale:     new(atomic.Value),
	}
	n.File.InitMeta(meta)
	n.Node = *buffer.NewNode(n, mgr, *meta.AsCommonID(), initSize)
	n.UnloadFunc = n.unload
	n.LoadFunc = n.load
	n.DestroyFunc = n.destroy
	return n
}

func (n *MutableBlockNode) SetStale() {
	n.Stale.Store(true)
}

func (n *MutableBlockNode) destroy() {
	n.File.Unref()
}

func (n *MutableBlockNode) Flush() error {
	if n.Stale.Load() == true {
		return nil
	}
	n.RLock()
	currSize := n.Data.Length()
	if ok := n.File.PreSync(uint32(currSize)); !ok {
		n.RUnlock()
		return nil
	}
	cols := len(n.Meta.Segment.Table.Schema.ColDefs)
	attrs := make([]int, cols)
	vecs := make([]vector.IVector, cols)
	var err error
	for i, _ := range n.Meta.Segment.Table.Schema.ColDefs {
		attrs[i] = i
		vec, err := n.Data.GetVectorByAttr(i)
		if err != nil {
			return err
		}
		vecs[i] = vec.GetLatestView()
	}
	data, err := batch.NewBatch(attrs, vecs)
	if err != nil {
		return err
	}
	n.RUnlock()
	meta := n.Meta.View()
	if err := n.Flusher(n, data, meta, n.File); err != nil {
		return err
	}
	n.postFlush()
	return nil
}

func (n *MutableBlockNode) postFlush() {
	n.Meta.Segment.Table.UpdateFlushTS()
	wal := n.Meta.Segment.Table.Database.Catalog.IndexWal
	if wal != nil {
		snippet := n.Meta.ConsumeSnippet(false)
		// logutil.Infof("Mutblock %d Count %d Snippet %s", n.Meta.Id, n.Meta.GetCoarseCount(), snippet.String())
		wal.Checkpoint(snippet)
	}
}

func (n *MutableBlockNode) load() {
	n.Data = n.File.LoadBatch(n.Meta)
	// logutil.Infof("%s loaded %d", n.Meta.AsCommonID().BlockString(), n.Data.Length())
}

func (n *MutableBlockNode) releaseData() {
	if n.Data != nil {
		n.Data.Close()
		n.Data = nil
	}
}

func (n *MutableBlockNode) unload() {
	// logutil.Infof("%s presyncing %d", n.Meta.AsCommonID().BlockString(), n.Data.Length())
	defer n.releaseData()
	if n.Stale.Load() == true {
		return
	}
	if ok := n.File.PreSync(uint32(n.Data.Length())); ok {
		meta := n.Meta.View()
		if err := n.Flusher(n, n.Data, meta, n.File); err != nil {
			panic(err)
		}
		n.postFlush()
	}
}

func (n *MutableBlockNode) GetData() batch.IBatch {
	return n.Data
}

func (n *MutableBlockNode) GetMeta() *metadata.Block {
	return n.Meta
}

func (n *MutableBlockNode) GetFile() *dataio.TransientBlockFile {
	return n.File
}
