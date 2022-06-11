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

	"github.com/RoaringBitmap/roaring"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
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
	PrepareAppend(data *gbat.Batch, offset uint32) (toAppend uint32)
	Append(data *gbat.Batch, offset uint32) (appended uint32, err error)
	RangeDelete(start, end uint32) error
	IsRowDeleted(row uint32) bool
	PrintDeletes() string
	FillColumnView(*model.ColumnView, *bytes.Buffer, *bytes.Buffer) error
	Window(start, end uint32) (*gbat.Batch, error)
	GetSpace() uint32
	Rows() uint32
	GetValue(col int, row uint32) (any, error)
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

func mockAppendInfo() *appendInfo {
	return &appendInfo{
		seq:    1,
		srcOff: 678,
		srcLen: 2134,
		dest: &common.ID{
			TableID:   1234,
			SegmentID: 45,
			BlockID:   9,
		},
		destOff: 6790,
		destLen: 9876,
	}
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
func (info *appendInfo) String() string {
	s := fmt.Sprintf("[%d]: Append from [%d:%d] to blk %s[%d:%d]",
		info.seq, info.srcOff, info.srcLen+info.srcOff, info.dest.BlockString(), info.destOff, info.destLen+info.destOff)
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
	data    batch.IBatch
	lsn     uint64
	typ     txnbase.NodeState
	deletes *roaring.Bitmap
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
	impl.DestroyFunc = impl.OnDestory
	impl.LoadFunc = impl.OnLoad
	impl.table = tbl
	impl.appends = make([]*appendInfo, 0)
	impl.prefix = model.EncodeBlockKeyPrefix(id.SegmentID, id.BlockID)
	mgr.RegisterNode(impl)
	return impl
}

func mockInsertNodeWithAppendInfo(infos []*appendInfo) *insertNode {
	node := new(insertNode)
	node.appends = infos
	attrs := []int{0, 1}
	vecs := make([]vector.IVector, 2)
	node.data, _ = batch.NewBatch(attrs, vecs)
	node.lsn = 1
	return node
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
		batCmd := txnbase.NewBatchCmd(n.data, n.table.GetSchema().AllTypes())
		composedCmd.AddCmd(batCmd)
	} else {
		ptrCmd := new(txnbase.PointerCmd)
		ptrCmd.Lsn = n.lsn
		ptrCmd.Group = wal.GroupUC
		composedCmd.AddCmd(ptrCmd)
	}
	if n.deletes != nil {
		delCmd := txnbase.NewDeleteBitmapCmd(n.deletes)
		composedCmd.AddCmd(delCmd)
	}
	return composedCmd, entry, nil
}

func (n *insertNode) Type() txnbase.NodeType { return NTInsert }

func (n *insertNode) makeLogEntry() wal.LogEntry {
	cmd := txnbase.NewBatchCmd(n.data, n.table.GetSchema().AllTypes())
	buf, err := cmd.Marshal()
	e := entry.GetBase()
	e.SetType(ETInsertNode)
	if err != nil {
		panic(err)
	}
	if err = e.Unmarshal(buf); err != nil {
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

func (n *insertNode) OnDestory() {
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
	r := bytes.NewBuffer(buf)
	cmd, _, err := txnbase.BuildCommandFrom(r)
	if err != nil {
		panic(err)
	}
	n.data = cmd.(*txnbase.BatchCmd).Bat
	// v, err := n.GetValue(n.table.GetSchema().PrimaryKey, 10)
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
		Group:     entry.GTUncommit,
		Uncommits: []entry.Tid{{Group: wal.GroupC, Tid: n.table.store.txn.GetID()}},
	}
	en.SetInfo(info)
	if seq, err := n.driver.AppendEntry(wal.GroupUC, en); err != nil {
		panic(err)
	} else {
		atomic.StoreUint64(&n.lsn, seq)
		id := n.GetID()
		logutil.Debugf("Unloading lsn=%d id=%s", seq, id.SegmentString())
	}
	// e.WaitDone()
	// e.Free()
	return
}

func (n *insertNode) PrepareAppend(data *gbat.Batch, offset uint32) uint32 {
	length := gvec.Length(data.Vecs[0])
	left := uint32(length) - offset
	nodeLeft := txnbase.MaxNodeRows - n.rows
	if left <= nodeLeft {
		return left
	}
	return nodeLeft
}

func (n *insertNode) Append(data *gbat.Batch, offset uint32) (an uint32, err error) {
	schema := n.table.entry.GetSchema()
	if n.data == nil {
		vecs := make([]vector.IVector, len(schema.ColDefs))
		attrIds := make([]int, len(schema.ColDefs))
		for i, def := range schema.ColDefs {
			attrIds[i] = def.Idx
			vecs[i] = vector.NewVector(def.Type, uint64(txnbase.MaxNodeRows))
		}
		if n.data, err = batch.NewBatch(attrIds, vecs); err != nil {
			return
		}
	}

	var cnt int
	from := uint32(n.data.Length())
	for i, attr := range data.Attrs {
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec, err := n.data.GetVectorByAttr(def.Idx)
		if err != nil {
			return an, err
		}
		if cnt, err = destVec.AppendVector(data.Vecs[i], int(offset)); err != nil {
			return an, err
		}
		n.rows = uint32(destVec.Length())
	}
	an = uint32(cnt)
	err = n.FillHiddenColumn(from, uint32(compute.LengthOfBatch(data))-offset)
	return
}

func (n *insertNode) FillHiddenColumn(startRow, length uint32) (err error) {
	col, closer, err := model.PrepareHiddenData(catalog.HiddenColumnType, n.prefix, startRow, length)
	if err != nil {
		return
	}
	defer closer()
	vec, err := n.data.GetVectorByAttr(n.table.entry.GetSchema().HiddenKey.Idx)
	if err != nil {
		return
	}
	_, err = vec.AppendVector(col, 0)
	if err != nil {
		panic(err)
	}
	return
}

func (n *insertNode) FillColumnView(view *model.ColumnView, compressed, decompressed *bytes.Buffer) (err error) {
	ivec, err := n.data.GetVectorByAttr(view.ColIdx)
	if err != nil {
		return
	}
	ivec = ivec.GetLatestView()
	if decompressed == nil || compressed == nil {
		view.AppliedVec, err = ivec.CopyToVector()
	} else {
		decompressed.Reset()
		compressed.Reset()
		view.AppliedVec, err = ivec.CopyToVectorWithBuffer(compressed, decompressed)
	}
	view.DeleteMask = n.deletes
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
	if n.deletes != nil {
		deletes = uint32(n.deletes.GetCardinality())
	}
	return n.rows - deletes
}

func (n *insertNode) LengthWithDeletes(appended, toAppend uint32) uint32 {
	if n.deletes == nil || n.deletes.GetCardinality() == 0 {
		return toAppend
	}
	appendedOffset := n.OffsetWithDeletes(appended)
	toAppendOffset := n.OffsetWithDeletes(toAppend + appended)
	// logutil.Infof("appened:%d, toAppend:%d, off1=%d, off2=%d", appended, toAppend, appendedOffset, toAppendOffset)
	return toAppendOffset - appendedOffset
}

func (n *insertNode) OffsetWithDeletes(count uint32) uint32 {
	if n.deletes == nil || n.deletes.GetCardinality() == 0 {
		return count
	}
	offset := count
	for offset < n.rows {
		deletes := n.deletes.Rank(offset)
		if offset == count+uint32(deletes) {
			break
		}
		offset = count + uint32(deletes)
	}
	return offset
}

func (n *insertNode) GetValue(col int, row uint32) (any, error) {
	vec, err := n.data.GetVectorByAttr(col)
	if err != nil {
		return nil, err
	}
	v, err := vec.GetValue(int(row))
	return v, err
}

func (n *insertNode) RangeDelete(start, end uint32) error {
	if n.deletes == nil {
		n.deletes = roaring.New()
	}
	n.deletes.AddRange(uint64(start), uint64(end)+1)
	return nil
}

func (n *insertNode) IsRowDeleted(row uint32) bool {
	if n.deletes == nil {
		return false
	}
	return n.deletes.Contains(row)
}

func (n *insertNode) PrintDeletes() string {
	if n.deletes == nil {
		return "NoDeletes"
	}
	return n.deletes.String()
}

// TODO: Rewrite later
func (n *insertNode) Window(start, end uint32) (bat *gbat.Batch, err error) {
	bat = gbat.New(true, []string{})
	for _, attrId := range n.data.GetAttrs() {
		def := n.table.entry.GetSchema().ColDefs[attrId]
		if def.IsHidden() {
			continue
		}
		src, err := n.data.GetVectorByAttr(def.Idx)
		if err != nil {
			return nil, err
		}
		srcVec, err2 := src.Window(start, end+1).CopyToVector()
		if err2 != nil {
			panic(err2)
		}
		deletes := common.BM32Window(n.deletes, int(start), int(end))
		srcVec = compute.ApplyDeleteToVector(srcVec, deletes)
		bat.Vecs = append(bat.Vecs, srcVec)
		bat.Attrs = append(bat.Attrs, def.Name)
	}
	return
}
