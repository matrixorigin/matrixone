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

package txnimpl

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	"sync/atomic"
	"time"
)

// anode corresponds to an appendable block which belongs to txn's workspace and can be appended data.
type anode struct {
	*baseNode
}

// NewANode creates a InsertNode with data in memory.
func NewANode(
	tbl *txnTable,
	fs *objectio.ObjectFS,
	mgr base.INodeManager,
	sched tasks.TaskScheduler,
	meta *catalog.BlockEntry,
	driver wal.Driver) *anode {
	impl := new(anode)
	impl.baseNode = newBaseNode(tbl, fs, mgr, sched, meta)
	impl.storage.mnode = newMemoryNode(impl.baseNode)

	//TODO::data of anode will not be managed by nodeManager
	impl.storage.mnode.Node = buffer.NewNode(impl.storage.mnode, mgr, *meta.AsCommonID(), 0)
	impl.storage.mnode.driver = driver
	impl.storage.mnode.typ = txnbase.PersistNode
	impl.storage.mnode.UnloadFunc = impl.storage.mnode.OnUnload
	impl.storage.mnode.DestroyFunc = impl.storage.mnode.OnDestroy
	impl.storage.mnode.LoadFunc = impl.storage.mnode.OnLoad
	mgr.RegisterNode(impl.storage.mnode)

	return impl
}

// NewMemInsertNodeWithID is just for test.
func NewMemInsertNodeWithID(
	tbl *txnTable,
	mgr base.INodeManager,
	id *common.ID,
	driver wal.Driver) *anode {
	impl := new(anode)
	impl.baseNode = newBaseNode(tbl, nil, mgr, nil, nil)
	impl.storage.mnode = newMemoryNode(impl.baseNode)

	impl.storage.mnode.Node = buffer.NewNode(impl.storage.mnode, mgr, *id, 0)
	impl.storage.mnode.driver = driver
	impl.storage.mnode.typ = txnbase.PersistNode
	impl.storage.mnode.UnloadFunc = impl.storage.mnode.OnUnload
	impl.storage.mnode.DestroyFunc = impl.storage.mnode.OnDestroy
	impl.storage.mnode.LoadFunc = impl.storage.mnode.OnLoad
	impl.table = tbl
	mgr.RegisterNode(impl.storage.mnode)
	return impl
}

func (n *anode) GetAppends() []*appendInfo {
	return n.storage.mnode.appends
}
func (n *anode) AddApplyInfo(srcOff, srcLen, destOff, destLen uint32, dbid uint64, dest *common.ID) *appendInfo {
	seq := len(n.storage.mnode.appends)
	info := &appendInfo{
		dest:    dest,
		destOff: destOff,
		destLen: destLen,
		dbid:    dbid,
		srcOff:  srcOff,
		srcLen:  srcLen,
		seq:     uint32(seq),
	}
	n.storage.mnode.appends = append(n.storage.mnode.appends, info)
	return info
}

func (n *anode) MakeCommand(id uint32, forceFlush bool) (cmd txnif.TxnCmd, entry wal.LogEntry, err error) {
	h, err := n.bufMgr.TryPin(n.storage.mnode, time.Second)
	if err != nil {
		return
	}
	if n.storage.mnode.data == nil {
		h.Close()
		return
	}
	composedCmd := NewAppendCmd(id, n)
	if n.storage.mnode.lsn == 0 && forceFlush {
		entry = n.storage.mnode.execUnload()
	}
	if n.storage.mnode.lsn == 0 {
		batCmd := txnbase.NewBatchCmd(n.storage.mnode.data)
		composedCmd.AddCmd(batCmd)
	} else {
		ptrCmd := new(txnbase.PointerCmd)
		ptrCmd.Lsn = n.storage.mnode.lsn
		ptrCmd.Group = wal.GroupUC
		composedCmd.AddCmd(ptrCmd)
	}
	n.storage.mnode.ToTransient()
	h.Close()
	return composedCmd, entry, nil
}

func (n *anode) Type() txnbase.NodeType { return NTInsert }

//func (n *anode) makeLogEntry() wal.LogEntry {
//	cmd := txnbase.NewBatchCmd(n.storage.mnode.data)
//	buf, err := cmd.Marshal()
//	e := entry.GetBase()
//	e.SetType(ETInsertNode)
//	if err != nil {
//		panic(err)
//	}
//	if err = e.SetPayload(buf); err != nil {
//		panic(err)
//	}
//	return e
//}

func (n *anode) IsTransient() bool {
	return atomic.LoadInt32(&n.storage.mnode.typ) == txnbase.TransientNode
}

//func (n *anode) OnDestroy() {
//	if n.storage.mnode.data != nil {
//		n.storage.mnode.data.Close()
//	}
//}

//func (n *anode) OnLoad() {
//	if n.IsTransient() {
//		return
//	}
//
//	lsn := atomic.LoadUint64(&n.storage.mnode.lsn)
//	if lsn == 0 {
//		return
//	}
//	e, err := n.storage.mnode.driver.LoadEntry(wal.GroupUC, lsn)
//	if err != nil {
//		panic(err)
//	}
//	logutil.Debugf("GetPayloadSize=%d", e.GetPayloadSize())
//	buf := e.GetPayload()
//	e.Free()
//	r := bytes.NewBuffer(buf)
//	cmd, _, err := txnbase.BuildCommandFrom(r)
//	if err != nil {
//		panic(err)
//	}
//	n.storage.mnode.data = cmd.(*txnbase.BatchCmd).Bat
//}

func (n *anode) Close() error {
	n.storage.mnode.ToTransient()
	return n.storage.mnode.Close()
}

//func (n *anode) OnUnload() {
//	entry := n.execUnload()
//	if entry != nil {
//		if err := entry.WaitDone(); err != nil {
//			panic(err)
//		}
//		entry.Free()
//	}
//}

//func (n *anode) execUnload() (en wal.LogEntry) {
//	if n.IsTransient() {
//		return
//	}
//	if atomic.LoadUint64(&n.storage.mnode.lsn) != 0 {
//		return
//	}
//	if n.storage.mnode.data == nil {
//		return
//	}
//	en = n.makeLogEntry()
//	info := &entry.Info{
//		Group:     wal.GroupUC,
//		Uncommits: n.table.store.txn.GetID(),
//	}
//	en.SetInfo(info)
//	if seq, err := n.storage.mnode.driver.AppendEntry(wal.GroupUC, en); err != nil {
//		panic(err)
//	} else {
//		atomic.StoreUint64(&n.storage.mnode.lsn, seq)
//		id := n.storage.mnode.Key()
//		logutil.Debugf("Unloading lsn=%d id=%v", seq, id)
//	}
//	// e.WaitDone()
//	// e.Free()
//	return
//}

//func (n *anode) PrepareAppend(data *containers.Batch, offset uint32) uint32 {
//	left := uint32(data.Length()) - offset
//	nodeLeft := txnbase.MaxNodeRows - n.storage.mnode.rows
//	if left <= nodeLeft {
//		return left
//	}
//	return nodeLeft
//}

func (n *anode) Append(data *containers.Batch, offset uint32) (an uint32, err error) {
	schema := n.table.entry.GetSchema()
	if n.storage.mnode.data == nil {
		opts := new(containers.Options)
		opts.Capacity = data.Length() - int(offset)
		if opts.Capacity > int(txnbase.MaxNodeRows) {
			opts.Capacity = int(txnbase.MaxNodeRows)
		}
		n.storage.mnode.data = containers.BuildBatch(
			schema.AllNames(),
			schema.AllTypes(),
			schema.AllNullables(),
			opts)
	}

	from := uint32(n.storage.mnode.data.Length())
	an = n.storage.mnode.PrepareAppend(data, offset)
	for _, attr := range data.Attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec := n.storage.mnode.data.Vecs[def.Idx]
		// logutil.Infof("destVec: %s, %d, %d", destVec.String(), cnt, data.Length())
		destVec.ExtendWithOffset(data.Vecs[def.Idx], int(offset), int(an))
	}
	n.storage.mnode.rows = uint32(n.storage.mnode.data.Length())
	err = n.storage.mnode.FillPhyAddrColumn(from, an)
	return
}

//func (n *anode) FillPhyAddrColumn(startRow, length uint32) (err error) {
//	col, err := model.PreparePhyAddrData(catalog.PhyAddrColumnType, n.meta.MakeKey(), startRow, length)
//	if err != nil {
//		return
//	}
//	defer col.Close()
//	vec := n.storage.mnode.data.Vecs[n.table.entry.GetSchema().PhyAddrKey.Idx]
//	vec.Extend(col)
//	return
//}

func (n *anode) FillBlockView(view *model.BlockView, buffers []*bytes.Buffer, colIdxes []int) (err error) {
	for i, colIdx := range colIdxes {
		orig := n.storage.mnode.data.Vecs[colIdx]
		if buffers[i] != nil {
			buffers[i].Reset()
			view.SetData(colIdx, containers.CloneWithBuffer(orig, buffers[i]))
		} else {
			view.SetData(colIdx, orig.CloneWindow(0, orig.Length()))
		}

	}
	view.DeleteMask = n.storage.mnode.data.Deletes
	return
}
func (n *anode) FillColumnView(view *model.ColumnView, buffer *bytes.Buffer) (err error) {
	orig := n.storage.mnode.data.Vecs[view.ColIdx]
	if buffer != nil {
		buffer.Reset()
		view.SetData(containers.CloneWithBuffer(orig, buffer))
	} else {
		view.SetData(orig.CloneWindow(0, orig.Length()))
	}
	view.DeleteMask = n.storage.mnode.data.Deletes
	return
}

func (n *anode) RowsWithoutDeletes() uint32 {
	deletes := uint32(0)
	if n.storage.mnode.data != nil && n.storage.mnode.data.Deletes != nil {
		deletes = uint32(n.storage.mnode.data.DeleteCnt())
	}
	return n.storage.mnode.rows - deletes
}

func (n *anode) LengthWithDeletes(appended, toAppend uint32) uint32 {
	if !n.storage.mnode.data.HasDelete() {
		return toAppend
	}
	appendedOffset := n.OffsetWithDeletes(appended)
	toAppendOffset := n.OffsetWithDeletes(toAppend + appended)
	// logutil.Infof("appened:%d, toAppend:%d, off1=%d, off2=%d", appended, toAppend, appendedOffset, toAppendOffset)
	return toAppendOffset - appendedOffset
}

func (n *anode) OffsetWithDeletes(count uint32) uint32 {
	if !n.storage.mnode.data.HasDelete() {
		return count
	}
	offset := count
	for offset < n.storage.mnode.rows {
		deletes := n.storage.mnode.data.Deletes.Rank(offset)
		if offset == count+uint32(deletes) {
			break
		}
		offset = count + uint32(deletes)
	}
	return offset
}

func (n *anode) GetValue(col int, row uint32) any {
	return n.storage.mnode.data.Vecs[col].Get(int(row))
}

func (n *anode) RangeDelete(start, end uint32) error {
	n.storage.mnode.data.RangeDelete(int(start), int(end+1))
	return nil
}

func (n *anode) IsRowDeleted(row uint32) bool {
	return n.storage.mnode.data.IsDeleted(int(row))
}

func (n *anode) PrintDeletes() string {
	if !n.storage.mnode.data.HasDelete() {
		return "NoDeletes"
	}
	return n.storage.mnode.data.Deletes.String()
}

func (n *anode) Window(start, end uint32) (bat *containers.Batch, err error) {
	bat = n.storage.mnode.data.CloneWindow(int(start), int(end-start))
	bat.Compact()
	return
}
