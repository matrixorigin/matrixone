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
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

const (
	NTInsert txnbase.NodeType = iota
	NTUpdate
	NTDelete
	NTCreateTable
	NTDropTable
	NTCreateDB
	NTDropDB
)

type InsertNode interface {
	base.INode
	PrepareAppend(data *containers.Batch, offset uint32) (toAppend uint32)
	Append(data *containers.Batch, offset uint32) (appended uint32, err error)
	RangeDelete(start, end uint32) error
	IsRowDeleted(row uint32) bool
	PrintDeletes() string
	FillBlockView(view *model.BlockView, buffers []*bytes.Buffer, colIdxes []int) (err error)
	FillColumnView(*model.ColumnView, *bytes.Buffer) error
	Window(start, end uint32) (*containers.Batch, error)
	GetSpace() uint32
	Rows() uint32
	GetValue(col int, row uint32) any
	MakeCommand(uint32, bool) (txnif.TxnCmd, wal.LogEntry, error)
	ToTransient()
	AddApplyInfo(srcOff, srcLen, destOff, destLen uint32, dbid uint64, dest *common.ID) *appendInfo
	RowsWithoutDeletes() uint32
	LengthWithDeletes(appended, toAppend uint32) uint32
	OffsetWithDeletes(count uint32) uint32
	GetAppends() []*appendInfo
	GetTxn() txnif.AsyncTxn
}

type appendInfo struct {
	seq              uint32
	srcOff, srcLen   uint32
	dbid             uint64
	dest             *common.ID
	destOff, destLen uint32
}

func (info *appendInfo) GetDest() *common.ID {
	return info.dest
}
func (info *appendInfo) GetDBID() uint64 {
	return info.dbid
}
func (info *appendInfo) GetSrcOff() uint32 {
	return info.srcOff
}
func (info *appendInfo) GetSrcLen() uint32 {
	return info.srcLen
}
func (info *appendInfo) GetDestOff() uint32 {
	return info.destOff
}
func (info *appendInfo) GetDestLen() uint32 {
	return info.destLen
}
func (info *appendInfo) Desc() string {
	return info.dest.BlockString()
}
func (info *appendInfo) String() string {
	s := fmt.Sprintf("[From=[%d:%d];To=%s[%d:%d]]",
		info.srcOff, info.srcLen+info.srcOff, info.dest.BlockString(), info.destOff, info.destLen+info.destOff)
	return s
}
func (info *appendInfo) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, info.seq); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, info.srcOff); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, info.srcLen); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, info.dbid); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, info.dest.TableID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, info.dest.SegmentID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, info.dest.BlockID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, info.destOff); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, info.destLen); err != nil {
		return
	}
	n = 4 + 4 + 4 + 8 + 8 + 8 + 4 + 4
	return
}
func (info *appendInfo) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &info.seq); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &info.srcOff); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &info.srcLen); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &info.dbid); err != nil {
		return
	}
	info.dest = &common.ID{}
	if err = binary.Read(r, binary.BigEndian, &info.dest.TableID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &info.dest.SegmentID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &info.dest.BlockID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &info.destOff); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &info.destLen); err != nil {
		return
	}
	n = 4 + 4 + 4 + 8 + 8 + 8 + 4 + 4
	return
}

type insertNode struct {
	*buffer.Node
	driver  wal.Driver
	data    *containers.Batch
	lsn     uint64
	typ     txnbase.NodeState
	rows    uint32
	table   *txnTable
	appends []*appendInfo
	prefix  []byte
}

func NewInsertNode(tbl *txnTable, mgr base.INodeManager, id *common.ID, driver wal.Driver) *insertNode {
	impl := new(insertNode)
	impl.Node = buffer.NewNode(impl, mgr, *id, 0)
	impl.driver = driver
	impl.typ = txnbase.PersistNode
	impl.UnloadFunc = impl.OnUnload
	impl.DestroyFunc = impl.OnDestroy
	impl.LoadFunc = impl.OnLoad
	impl.table = tbl
	impl.appends = make([]*appendInfo, 0)
	impl.prefix = model.EncodeBlockKeyPrefix(id.SegmentID, id.BlockID)
	mgr.RegisterNode(impl)
	return impl
}

func (n *insertNode) GetTxn() txnif.AsyncTxn {
	return n.table.store.txn
}
func (n *insertNode) GetAppends() []*appendInfo {
	return n.appends
}
func (n *insertNode) AddApplyInfo(srcOff, srcLen, destOff, destLen uint32, dbid uint64, dest *common.ID) *appendInfo {
	seq := len(n.appends)
	info := &appendInfo{
		dest:    dest,
		destOff: destOff,
		destLen: destLen,
		dbid:    dbid,
		srcOff:  srcOff,
		srcLen:  srcLen,
		seq:     uint32(seq),
	}
	n.appends = append(n.appends, info)
	return info
}

func (n *insertNode) MakeCommand(id uint32, forceFlush bool) (cmd txnif.TxnCmd, entry wal.LogEntry, err error) {
	if n.data == nil {
		return
	}
	composedCmd := NewAppendCmd(id, n)
	if n.lsn == 0 && forceFlush {
		entry = n.execUnload()
	}
	if n.lsn == 0 {
		batCmd := txnbase.NewBatchCmd(n.data)
		composedCmd.AddCmd(batCmd)
	} else {
		ptrCmd := new(txnbase.PointerCmd)
		ptrCmd.Lsn = n.lsn
		ptrCmd.Group = wal.GroupUC
		composedCmd.AddCmd(ptrCmd)
	}
	return composedCmd, entry, nil
}

func (n *insertNode) Type() txnbase.NodeType { return NTInsert }

func (n *insertNode) makeLogEntry() wal.LogEntry {
	cmd := txnbase.NewBatchCmd(n.data)
	buf, err := cmd.Marshal()
	e := entry.GetBase()
	e.SetType(ETInsertNode)
	if err != nil {
		panic(err)
	}
	if err = e.SetPayload(buf); err != nil {
		panic(err)
	}
	return e
}

func (n *insertNode) IsTransient() bool {
	return atomic.LoadInt32(&n.typ) == txnbase.TransientNode
}

func (n *insertNode) ToTransient() {
	atomic.StoreInt32(&n.typ, txnbase.TransientNode)
}

func (n *insertNode) OnDestroy() {
	if n.data != nil {
		n.data.Close()
	}
}

func (n *insertNode) OnLoad() {
	if n.IsTransient() {
		return
	}

	lsn := atomic.LoadUint64(&n.lsn)
	if lsn == 0 {
		return
	}
	e, err := n.driver.LoadEntry(wal.GroupUC, lsn)
	if err != nil {
		panic(err)
	}
	logutil.Debugf("GetPayloadSize=%d", e.GetPayloadSize())
	buf := e.GetPayload()
	e.Free()
	r := bytes.NewBuffer(buf)
	cmd, _, err := txnbase.BuildCommandFrom(r)
	if err != nil {
		panic(err)
	}
	n.data = cmd.(*txnbase.BatchCmd).Bat
}

func (n *insertNode) Close() error {
	n.ToTransient()
	return n.Node.Close()
}

func (n *insertNode) OnUnload() {
	entry := n.execUnload()
	if entry != nil {
		if err := entry.WaitDone(); err != nil {
			panic(err)
		}
		entry.Free()
	}
}

func (n *insertNode) execUnload() (en wal.LogEntry) {
	if n.IsTransient() {
		return
	}
	if atomic.LoadUint64(&n.lsn) != 0 {
		return
	}
	if n.data == nil {
		return
	}
	en = n.makeLogEntry()
	info := &entry.Info{
		Group:     wal.GroupUC,
		Uncommits: n.table.store.txn.GetID(),
	}
	en.SetInfo(info)
	if seq, err := n.driver.AppendEntry(wal.GroupUC, en); err != nil {
		panic(err)
	} else {
		atomic.StoreUint64(&n.lsn, seq)
		id := n.Key()
		logutil.Debugf("Unloading lsn=%d id=%v", seq, id)
	}
	// e.WaitDone()
	// e.Free()
	return
}

func (n *insertNode) PrepareAppend(data *containers.Batch, offset uint32) uint32 {
	left := uint32(data.Length()) - offset
	nodeLeft := txnbase.MaxNodeRows - n.rows
	if left <= nodeLeft {
		return left
	}
	return nodeLeft
}

func (n *insertNode) Append(data *containers.Batch, offset uint32) (an uint32, err error) {
	schema := n.table.entry.GetSchema()
	if n.data == nil {
		opts := new(containers.Options)
		opts.Capacity = data.Length() - int(offset)
		if opts.Capacity > int(txnbase.MaxNodeRows) {
			opts.Capacity = int(txnbase.MaxNodeRows)
		}
		n.data = containers.BuildBatch(
			schema.AllNames(),
			schema.AllTypes(),
			schema.AllNullables(),
			opts)
	}

	from := uint32(n.data.Length())
	an = n.PrepareAppend(data, offset)
	for _, attr := range data.Attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec := n.data.Vecs[def.Idx]
		// logutil.Infof("destVec: %s, %d, %d", destVec.String(), cnt, data.Length())
		destVec.ExtendWithOffset(data.Vecs[def.Idx], int(offset), int(an))
	}
	n.rows = uint32(n.data.Length())
	err = n.FillPhyAddrColumn(from, an)
	return
}

func (n *insertNode) FillPhyAddrColumn(startRow, length uint32) (err error) {
	col, err := model.PreparePhyAddrData(catalog.PhyAddrColumnType, n.prefix, startRow, length)
	if err != nil {
		return
	}
	defer col.Close()
	vec := n.data.Vecs[n.table.entry.GetSchema().PhyAddrKey.Idx]
	vec.Extend(col)
	return
}
func (n *insertNode) FillBlockView(view *model.BlockView, buffers []*bytes.Buffer, colIdxes []int) (err error) {
	for i, colIdx := range colIdxes {
		orig := n.data.Vecs[colIdx]
		if buffers[i] != nil {
			buffers[i].Reset()
			view.SetData(colIdx, containers.CloneWithBuffer(orig, buffers[i]))
		} else {
			view.SetData(colIdx, orig.CloneWindow(0, orig.Length()))
		}

	}
	view.DeleteMask = n.data.Deletes
	return
}
func (n *insertNode) FillColumnView(view *model.ColumnView, buffer *bytes.Buffer) (err error) {
	orig := n.data.Vecs[view.ColIdx]
	if buffer != nil {
		buffer.Reset()
		view.SetData(containers.CloneWithBuffer(orig, buffer))
	} else {
		view.SetData(orig.CloneWindow(0, orig.Length()))
	}
	view.DeleteMask = n.data.Deletes
	return
}

func (n *insertNode) GetSpace() uint32 {
	return txnbase.MaxNodeRows - n.rows
}

func (n *insertNode) Rows() uint32 {
	return n.rows
}

func (n *insertNode) RowsWithoutDeletes() uint32 {
	deletes := uint32(0)
	if n.data != nil && n.data.Deletes != nil {
		deletes = uint32(n.data.DeleteCnt())
	}
	return n.rows - deletes
}

func (n *insertNode) LengthWithDeletes(appended, toAppend uint32) uint32 {
	if !n.data.HasDelete() {
		return toAppend
	}
	appendedOffset := n.OffsetWithDeletes(appended)
	toAppendOffset := n.OffsetWithDeletes(toAppend + appended)
	// logutil.Infof("appened:%d, toAppend:%d, off1=%d, off2=%d", appended, toAppend, appendedOffset, toAppendOffset)
	return toAppendOffset - appendedOffset
}

func (n *insertNode) OffsetWithDeletes(count uint32) uint32 {
	if !n.data.HasDelete() {
		return count
	}
	offset := count
	for offset < n.rows {
		deletes := n.data.Deletes.Rank(offset)
		if offset == count+uint32(deletes) {
			break
		}
		offset = count + uint32(deletes)
	}
	return offset
}

func (n *insertNode) GetValue(col int, row uint32) any {
	return n.data.Vecs[col].Get(int(row))
}

func (n *insertNode) RangeDelete(start, end uint32) error {
	n.data.RangeDelete(int(start), int(end+1))
	return nil
}

func (n *insertNode) IsRowDeleted(row uint32) bool {
	return n.data.IsDeleted(int(row))
}

func (n *insertNode) PrintDeletes() string {
	if !n.data.HasDelete() {
		return "NoDeletes"
	}
	return n.data.Deletes.String()
}

func (n *insertNode) Window(start, end uint32) (bat *containers.Batch, err error) {
	if n.data.HasDelete() {
		bat = n.data.CloneWindow(int(start), int(end-start))
		bat.Compact()
	} else {
		bat = n.data.Window(int(start), int(end-start))
	}
	return
}
