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

package updates

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type NodeType int8

const (
	NT_Normal NodeType = iota
	NT_Merge
	NT_Persisted
)

func (node *DeleteNode) Less(b *DeleteNode) int {
	return node.TxnMVCCNode.Compare2(b.TxnMVCCNode)
}

type DeleteNode struct {
	*common.GenericDLNode[*DeleteNode]
	*txnbase.TxnMVCCNode
	chain atomic.Pointer[DeleteChain]
	mask  *roaring.Bitmap
	//FIXME::how to free pk vector?
	rowid2PK map[uint32]containers.Vector
	deltaloc objectio.Location
	nt       NodeType
	id       *common.ID
	dt       handle.DeleteType
	version  uint16
}

func NewMergedNode(commitTs types.TS) *DeleteNode {
	n := &DeleteNode{
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTS(commitTs),
		mask:        roaring.New(),
		rowid2PK:    make(map[uint32]containers.Vector),
		nt:          NT_Merge,
	}
	return n
}
func NewEmptyDeleteNode() *DeleteNode {
	n := &DeleteNode{
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(nil),
		mask:        roaring.New(),
		rowid2PK:    make(map[uint32]containers.Vector),
		nt:          NT_Normal,
		id:          &common.ID{},
		version:     IOET_WALTxnCommand_DeleteNode_CurrVer,
	}
	return n
}
func NewDeleteNode(txn txnif.AsyncTxn, dt handle.DeleteType, version uint16) *DeleteNode {
	n := &DeleteNode{
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
		mask:        roaring.New(),
		rowid2PK:    make(map[uint32]containers.Vector),
		nt:          NT_Normal,
		dt:          dt,
		version:     version,
	}
	if n.dt == handle.DT_MergeCompact {
		_, err := n.TxnMVCCNode.PrepareCommit()
		if err != nil {
			panic(err)
		}
	}
	return n
}

func NewPersistedDeleteNode(txn txnif.AsyncTxn, deltaloc objectio.Location) *DeleteNode {
	n := &DeleteNode{
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(txn),
		nt:          NT_Persisted,
		dt:          handle.DT_Normal,
		deltaloc:    deltaloc,
	}
	return n

}

func NewEmptyPersistedDeleteNode() *DeleteNode {
	n := &DeleteNode{
		TxnMVCCNode: txnbase.NewTxnMVCCNodeWithTxn(nil),
		nt:          NT_Persisted,
		id:          &common.ID{},
	}
	return n
}

func (node *DeleteNode) CloneAll() *DeleteNode  { panic("todo") }
func (node *DeleteNode) CloneData() *DeleteNode { panic("todo") }
func (node *DeleteNode) Update(*DeleteNode)     { panic("todo") }
func (node *DeleteNode) IsNil() bool            { return node == nil }
func (node *DeleteNode) GetPrepareTS() types.TS {
	return node.TxnMVCCNode.GetPrepare()
}
func (node *DeleteNode) GetMeta() *catalog.ObjectEntry { return node.chain.Load().mvcc.meta }
func (node *DeleteNode) GetID() *common.ID {
	return node.id
}

func (node *DeleteNode) SetDeletes(mask *roaring.Bitmap) {
	node.mask = mask
}
func (node *DeleteNode) Close() {
	for _, vec := range node.rowid2PK {
		vec.Close()
	}
}
func (node *DeleteNode) IsMerged() bool { return node.nt == NT_Merge }
func (node *DeleteNode) AttachTo(chain *DeleteChain) {
	node.chain.Store(chain)
	node.GenericDLNode = chain.Insert(node)
}

func (node *DeleteNode) GetChain() txnif.DeleteChain          { return node.chain.Load() }
func (node *DeleteNode) GetDeleteMaskLocked() *roaring.Bitmap { return node.mask }

func (node *DeleteNode) HasOverlapLocked(start, end uint32) bool {
	if node.mask == nil || node.mask.IsEmpty() {
		return false
	}
	var yes bool
	if start == end {
		yes = node.mask.Contains(start)
	} else {
		x2 := roaring.New()
		x2.AddRange(uint64(start), uint64(end+1))
		yes = node.mask.Intersects(x2)
	}
	return yes
}

func (node *DeleteNode) MergeLocked(o *DeleteNode) {
	if node.mask == nil {
		node.mask = roaring.New()
	}
	node.mask.Or(o.mask)
}
func (node *DeleteNode) GetCommitTSLocked() types.TS { return node.TxnMVCCNode.GetEnd() }
func (node *DeleteNode) GetStartTS() types.TS        { return node.TxnMVCCNode.GetStart() }

func (node *DeleteNode) IsDeletedLocked(row uint32) bool {
	return node.mask.Contains(row)
}

func (node *DeleteNode) GetBlockID() *objectio.Blockid {
	return objectio.NewBlockidWithObjectID(&node.GetMeta().ID, node.chain.Load().mvcc.blkID)
}

func (node *DeleteNode) RangeDeleteLocked(
	start, end uint32, pk containers.Vector, mp *mpool.MPool,
) {
	// logutil.Debugf("RangeDelete BLK-%d Start=%d End=%d",
	// 	node.chain.mvcc.meta.ID,
	// 	start,
	// 	end)
	node.mask.AddRange(uint64(start), uint64(end+1))
	if pk != nil && pk.Length() > 0 {
		begin := start
		for ; begin < end+1; begin++ {
			node.rowid2PK[begin] = pk.CloneWindow(int(begin-start), 1, mp)
		}
	} else {
		panic("pk vector is empty")
	}
	node.chain.Load().insertInMaskByRange(start, end)
	for i := start; i < end+1; i++ {
		node.chain.Load().InsertInDeleteView(i, node)
	}
	node.chain.Load().mvcc.IncChangeIntentionCnt()
}

func (node *DeleteNode) DeletedRows() (rows []uint32) {
	if node.mask == nil {
		return
	}
	rows = node.mask.ToArray()
	return
}

func (node *DeleteNode) DeletedPK() (pkVec map[uint32]containers.Vector) {
	if node.mask == nil {
		return
	}
	pkVec = node.rowid2PK
	return
}

func (node *DeleteNode) GetCardinalityLocked() uint32 { return uint32(node.mask.GetCardinality()) }

func (node *DeleteNode) PrepareCommit() (err error) {
	node.chain.Load().mvcc.Lock()
	defer node.chain.Load().mvcc.Unlock()
	_, err = node.TxnMVCCNode.PrepareCommit()
	if err != nil {
		return
	}
	node.chain.Load().UpdateLocked(node)
	return
}
func (node *DeleteNode) IsPersistedDeletedNode() bool {
	return node.nt == NT_Persisted
}
func (node *DeleteNode) ApplyCommit() (err error) {
	node.chain.Load().mvcc.Lock()
	defer node.chain.Load().mvcc.Unlock()
	_, err = node.TxnMVCCNode.ApplyCommit()
	if err != nil {
		return
	}
	switch node.nt {
	// AddDeleteCnt is for checkpoint, deletes in NT_PERSISTED node have already flushed
	case NT_Normal, NT_Merge:
		node.chain.Load().AddDeleteCnt(uint32(node.mask.GetCardinality()))
	}
	return node.OnApply()
}

func (node *DeleteNode) ApplyRollback() (err error) {
	node.chain.Load().mvcc.Lock()
	defer node.chain.Load().mvcc.Unlock()
	_, err = node.TxnMVCCNode.ApplyRollback()
	node.chain.Load().mvcc.DecChangeIntentionCnt()
	return
}

func (node *DeleteNode) setPersistedRows() {
	if node.nt != NT_Persisted {
		panic("unsupport")
	}
	var closeFunc func()
	var rowids containers.Vector
	var vectors []containers.Vector
	var err error
	defer func() {
		if closeFunc != nil {
			closeFunc()
		}
		if rowids != nil {
			rowids.Close()
		}
	}()
	//Extend lifetime of vectors is within the function.
	//No NeedCopy. closeFunc is required after use.
	vectors, closeFunc, err = blockio.LoadTombstoneColumns2(
		node.Txn.GetContext(),
		[]uint16{0},
		nil,
		node.chain.Load().mvcc.meta.GetObjectData().GetFs().Service,
		node.deltaloc,
		false,
		nil,
	)
	if err != nil {
		for {
			logutil.Warnf(fmt.Sprintf("load deletes failed, deltaloc: %s, err: %v", node.deltaloc.String(), err))
			vectors, closeFunc, err = blockio.LoadTombstoneColumns2(
				node.Txn.GetContext(),
				[]uint16{0},
				nil,
				node.chain.Load().mvcc.meta.GetObjectData().GetFs().Service,
				node.deltaloc,
				false,
				nil,
			)
			if err == nil {
				break
			}
		}
	}
	node.mask = roaring.NewBitmap()
	rowids = vectors[0]
	err = containers.ForeachVector(rowids, func(rowid types.Rowid, _ bool, row int) error {
		offset := rowid.GetRowOffset()
		node.mask.Add(offset)
		return nil
	}, nil)
	if err != nil {
		panic(err)
	}
}

func (node *DeleteNode) GeneralString() string {
	switch node.nt {
	case NT_Merge, NT_Normal:
		return fmt.Sprintf("%s;Cnt=%d", node.TxnMVCCNode.String(), node.mask.GetCardinality())
	case NT_Persisted:
		return fmt.Sprintf("%s;DeltaLoc=%s", node.TxnMVCCNode.String(), node.deltaloc.String())
	default:
		panic(fmt.Sprintf("not support type %d", node.nt))
	}
}

func (node *DeleteNode) GeneralDesc() string {
	switch node.nt {
	case NT_Merge, NT_Normal:
		return fmt.Sprintf("%s;Cnt=%d", node.TxnMVCCNode.String(), node.mask.GetCardinality())
	case NT_Persisted:
		return fmt.Sprintf("%s;DeltaLoc=%s", node.TxnMVCCNode.String(), node.deltaloc.String())
	default:
		panic(fmt.Sprintf("not support type %d", node.nt))
	}
}

func (node *DeleteNode) GeneralVerboseString() string {
	switch node.nt {
	case NT_Merge, NT_Normal:
		return fmt.Sprintf("%s;Cnt=%d;Deletes=%v", node.TxnMVCCNode.String(), node.mask.GetCardinality(), node.mask)
	case NT_Persisted:
		return fmt.Sprintf("%s;DeltaLoc=%s", node.TxnMVCCNode.String(), node.deltaloc.String())
	default:
		panic(fmt.Sprintf("not support type %d", node.nt))
	}
}

func (node *DeleteNode) StringLocked() string {
	ntype := "TXN"
	if node.nt == NT_Merge {
		ntype = "MERGE"
	}
	if node.nt == NT_Persisted {
		ntype = "PERSISTED"
	}
	dtype := "N"
	if node.dt == handle.DT_MergeCompact {
		dtype = "MC"
	}
	commitState := "C"
	if node.GetEnd() == txnif.UncommitTS {
		commitState = "UC"
	}
	payload := ""
	switch node.nt {
	case NT_Normal, NT_Merge:
		payload = fmt.Sprintf("[%d:%s]", node.mask.GetCardinality(), node.mask.String())
	case NT_Persisted:
		payload = fmt.Sprintf("[delta=%s]", node.deltaloc.String())
	}
	s := fmt.Sprintf("[%s:%s:%s]%s%s", ntype, commitState, dtype, payload, node.TxnMVCCNode.String())
	return s
}

func (node *DeleteNode) WriteTo(w io.Writer) (n int64, err error) {
	cn, err := w.Write(common.EncodeID(node.chain.Load().mvcc.GetID()))
	if err != nil {
		return
	}
	n += int64(cn)
	var buf []byte
	var sn int64
	buf, err = node.mask.ToBytes()
	if err != nil {
		return
	}
	if sn, err = objectio.WriteBytes(buf, w); err != nil {
		return
	}
	n += int64(sn)

	switch node.nt {
	case NT_Merge, NT_Normal:
		var sn2 int
		length := uint32(len(node.rowid2PK))
		if sn2, err = w.Write(types.EncodeUint32(&length)); err != nil {
			return
		}
		n += int64(sn2)
		for row, pk := range node.rowid2PK {
			if sn2, err = w.Write(types.EncodeUint32(&row)); err != nil {
				return
			}
			n += int64(sn2)
			if sn2, err = w.Write(types.EncodeType(pk.GetType())); err != nil {
				return
			}
			n += int64(sn2)
			if sn, err = pk.WriteTo(w); err != nil {
				return
			}
			n += int64(sn)
		}
	case NT_Persisted:
		if sn, err = objectio.WriteBytes(node.deltaloc, w); err != nil {
			return
		}
		n += int64(sn)
	}
	var sn3 int64
	if sn3, err = node.TxnMVCCNode.WriteTo(w); err != nil {
		return
	}
	n += sn3
	return
}

func (node *DeleteNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int
	if node.id == nil {
		node.id = &common.ID{}
	}
	if sn, err = r.Read(common.EncodeID(node.id)); err != nil {
		return
	}
	n += int64(sn)
	var sn2 int64
	var buf []byte
	if buf, sn2, err = objectio.ReadBytes(r); err != nil {
		return
	}
	n += sn2
	node.mask = roaring.New()
	err = node.mask.UnmarshalBinary(buf)
	if err != nil {
		return
	}
	switch node.nt {
	case NT_Merge, NT_Normal:
		if node.version >= IOET_WALTxnCommand_DeleteNode_V2 {
			var sn3 int
			length := uint32(0)
			if sn3, err = r.Read(types.EncodeUint32(&length)); err != nil {
				return
			}
			n += int64(sn3)
			node.rowid2PK = make(map[uint32]containers.Vector)
			for i := 0; i < int(length); i++ {
				row := uint32(0)
				if sn3, err = r.Read(types.EncodeUint32(&row)); err != nil {
					return
				}
				n += int64(sn3)
				typ := &types.Type{}
				if sn3, err = r.Read(types.EncodeType(typ)); err != nil {
					return
				}
				n += int64(sn3)
				pk := containers.MakeVector(*typ, common.MutMemAllocator)
				if sn2, err = pk.ReadFrom(r); err != nil {
					return
				}
				node.rowid2PK[row] = pk
				n += int64(sn2)
			}
		}
	case NT_Persisted:
		if buf, sn2, err = objectio.ReadBytes(r); err != nil {
			return
		}
		n += sn2
		node.deltaloc = buf
	}
	txnMVCCNodeVersion := node.decideTxnMVCCNodeVersion()
	if sn2, err = node.TxnMVCCNode.ReadFromWithVersion(r, txnMVCCNodeVersion); err != nil {
		return
	}
	n += sn2
	return
}

func (node *DeleteNode) decideTxnMVCCNodeVersion() int {
	if node.IsPersistedDeletedNode() {
		switch node.version {
		case IOET_WALTxnCommand_PersistedDeleteNode_V1:
			return txnbase.TxnMVCCNodeV1
		case IOET_WALTxnCommand_PersistedDeleteNode_V2:
			return txnbase.TxnMVCCNodeV2
		default:
			panic(fmt.Sprintf("invalid delete node version %d", node.version))
		}
	} else {
		switch node.version {
		case IOET_WALTxnCommand_DeleteNode_V1, IOET_WALTxnCommand_DeleteNode_V2:
			return txnbase.TxnMVCCNodeV1
		case IOET_WALTxnCommand_DeleteNode_V3:
			return txnbase.TxnMVCCNodeV2
		default:
			panic(fmt.Sprintf("invalid delete node version %d", node.version))
		}
	}
}

func (node *DeleteNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	switch node.nt {
	case NT_Merge, NT_Normal:
		cmd = NewDeleteCmd(id, node)
	case NT_Persisted:
		cmd = NewPersistedDeleteCmd(id, node)
	}
	return
}
func (node *DeleteNode) GetPrefix() []byte {
	return objectio.NewBlockidWithObjectID(&node.GetMeta().ID, node.chain.Load().mvcc.blkID)[:]
}
func (node *DeleteNode) Set1PC()     { node.TxnMVCCNode.Set1PC() }
func (node *DeleteNode) Is1PC() bool { return node.TxnMVCCNode.Is1PC() }
func (node *DeleteNode) PrepareRollback() (err error) {
	node.chain.Load().mvcc.Lock()
	defer node.chain.Load().mvcc.Unlock()
	switch node.nt {
	case NT_Merge, NT_Normal:
		node.chain.Load().RemoveNodeLocked(node)
		node.chain.Load().DeleteInDeleteView(node)
	case NT_Persisted:
		node.chain.Load().RemoveNodeLocked(node)
	}
	node.TxnMVCCNode.PrepareRollback()
	return
}

func (node *DeleteNode) OnApply() (err error) {
	if node.dt == handle.DT_Normal {
		listener := node.chain.Load().mvcc.GetDeletesListener()
		if listener == nil {
			return
		}
		switch node.nt {
		case NT_Merge, NT_Normal:
			err = listener(node.mask.GetCardinality(), node.GetCommitTSLocked())
		case NT_Persisted:
			err = listener(uint64(node.mask.GetCardinality()), node.GetCommitTSLocked())
		}
	}
	return
}

func (node *DeleteNode) GetRowMaskRefLocked() *roaring.Bitmap {
	return node.mask
}
