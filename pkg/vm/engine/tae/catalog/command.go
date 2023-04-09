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
	"bytes"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

const (
	CmdUpdateDatabase = int16(256) + iota
	CmdUpdateTable
	CmdUpdateSegment
	CmdUpdateBlock
)

var cmdNames = map[int16]string{
	CmdUpdateDatabase: "UDB",
	CmdUpdateTable:    "UTBL",
	CmdUpdateSegment:  "USEG",
	CmdUpdateBlock:    "UBLK",
}

func CmdName(t int16) string {
	return cmdNames[t]
}

func init() {
	txnif.RegisterCmdFactory(CmdUpdateDatabase, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType,
			NewEmptyMVCCNodeFactory(NewEmptyEmptyMVCCNode),
			func() *DBNode { return &DBNode{} })
	})
	txnif.RegisterCmdFactory(CmdUpdateTable, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType,
			NewEmptyMVCCNodeFactory(NewEmptyTableMVCCNode),
			func() *TableNode { return &TableNode{} })
	})
	txnif.RegisterCmdFactory(CmdUpdateSegment, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType,
			NewEmptyMVCCNodeFactory(NewEmptyMetadataMVCCNode),
			func() *SegmentNode { return &SegmentNode{} })
	})
	txnif.RegisterCmdFactory(CmdUpdateBlock, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType,
			NewEmptyMVCCNodeFactory(NewEmptyMetadataMVCCNode),
			func() *BlockNode { return &BlockNode{} })
	})
}

type Node interface {
	WriteTo(w io.Writer) (n int64, err error)
	ReadFrom(r io.Reader) (n int64, err error)
}

type EntryCommand[T BaseNode[T], N Node] struct {
	*txnbase.BaseCustomizedCmd
	cmdType  int16
	DBID     uint64
	ID       *common.ID
	mvccNode *MVCCNode[T]
	node     N
}

func newEmptyEntryCmd[T BaseNode[T], N Node](cmdType int16, mvccNodeFactory func() *MVCCNode[T], nodeFactory func() N) *EntryCommand[T, N] {
	impl := &EntryCommand[T, N]{
		cmdType:  cmdType,
		ID:       &common.ID{},
		mvccNode: mvccNodeFactory(),
		node:     nodeFactory(),
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(0, impl)
	return impl
}

func newBlockCmd(id uint32, cmdType int16, entry *BlockEntry) *EntryCommand[*MetadataMVCCNode, *BlockNode] {
	impl := &EntryCommand[*MetadataMVCCNode, *BlockNode]{
		DBID:     entry.GetSegment().GetTable().GetDB().ID,
		ID:       entry.AsCommonID(),
		cmdType:  cmdType,
		mvccNode: entry.BaseEntryImpl.GetLatestNodeLocked(),
		node:     entry.BlockNode,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newSegmentCmd(id uint32, cmdType int16, entry *SegmentEntry) *EntryCommand[*MetadataMVCCNode, *SegmentNode] {
	impl := &EntryCommand[*MetadataMVCCNode, *SegmentNode]{
		DBID:     entry.GetTable().GetDB().ID,
		ID:       entry.AsCommonID(),
		cmdType:  cmdType,
		mvccNode: entry.BaseEntryImpl.GetLatestNodeLocked(),
		node:     entry.SegmentNode,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newTableCmd(id uint32, cmdType int16, entry *TableEntry) *EntryCommand[*TableMVCCNode, *TableNode] {
	impl := &EntryCommand[*TableMVCCNode, *TableNode]{
		DBID:     entry.GetDB().ID,
		ID:       entry.AsCommonID(),
		cmdType:  cmdType,
		mvccNode: entry.BaseEntryImpl.GetLatestNodeLocked(),
		node:     entry.TableNode,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newDBCmd(id uint32, cmdType int16, entry *DBEntry) *EntryCommand[*EmptyMVCCNode, *DBNode] {
	impl := &EntryCommand[*EmptyMVCCNode, *DBNode]{
		DBID:     entry.ID,
		ID:       &common.ID{},
		cmdType:  cmdType,
		node:     entry.DBNode,
		mvccNode: entry.GetLatestNodeLocked(),
	}
	// if entry != nil {
	// 	impl.mvccNode = entry.BaseEntryImpl.GetLatestNodeLocked().(*MVCCNode[*DBMVCCNode])
	// }
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func (cmd *EntryCommand[T, N]) Desc() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%s;CSN=%d", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs().ToString(), cmd.ID)
	return s
}

func (cmd *EntryCommand[T, N]) GetLogIndex() *wal.Index {
	if cmd.mvccNode == nil {
		return nil
	}
	return cmd.mvccNode.GetLogIndex()
}

func (cmd *EntryCommand[T, N]) SetReplayTxn(txn txnif.AsyncTxn) {
	cmd.mvccNode.Txn = txn
}

func (cmd *EntryCommand[T, N]) ApplyCommit() {
	if cmd.mvccNode.Is1PC() {
		return
	}
	if err := cmd.mvccNode.ApplyCommit(nil); err != nil {
		panic(err)
	}
}

func (cmd *EntryCommand[T, N]) ApplyRollback() {
	if cmd.mvccNode.Is1PC() {
		return
	}
	cmd.mvccNode.ApplyRollback(nil)
}

func (cmd *EntryCommand[T, N]) GetTs() types.TS {
	ts := cmd.mvccNode.GetPrepare()
	return ts
}

func (cmd *EntryCommand[T, N]) IDString() string {
	s := ""
	dbid, id := cmd.GetID()
	switch cmd.cmdType {
	case CmdUpdateDatabase:
		s = fmt.Sprintf("%sDB=%d", s, dbid)
	case CmdUpdateTable:
		s = fmt.Sprintf("%sDB=%d;CommonID=%s", s, dbid, id.TableString())
	case CmdUpdateSegment:
		s = fmt.Sprintf("%sDB=%d;CommonID=%s", s, dbid, id.SegmentString())
	case CmdUpdateBlock:
		s = fmt.Sprintf("%sDB=%d;CommonID=%s", s, dbid, id.BlockString())
	}
	return s
}
func (cmd *EntryCommand[T, N]) GetID() (uint64, *common.ID) {
	return cmd.DBID, cmd.ID
}

func (cmd *EntryCommand[T, N]) String() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%s;CSN=%d;BaseEntry=%s", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs().ToString(), cmd.ID, cmd.mvccNode.String())
	return s
}

func (cmd *EntryCommand[T, N]) VerboseString() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%s;CSN=%d;BaseEntry=%s", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs().ToString(), cmd.ID, cmd.mvccNode.String())
	return s
}
func (cmd *EntryCommand[T, N]) GetType() int16 { return cmd.cmdType }

func (cmd *EntryCommand[T, N]) WriteTo(w io.Writer) (n int64, err error) {
	t := cmd.GetType()
	if _, err = w.Write(types.EncodeInt16(&t)); err != nil {
		return
	}
	n += 2
	var sn2 int
	if sn2, err = w.Write(types.EncodeUint64(&cmd.DBID)); err != nil {
		return
	}
	n += int64(sn2)
	if sn2, err = w.Write(common.EncodeID(cmd.ID)); err != nil {
		return
	}
	n += int64(sn2)
	var sn int64
	if sn, err = cmd.mvccNode.WriteTo(w); err != nil {
		return
	}
	n += sn
	if sn, err = cmd.node.WriteTo(w); err != nil {
		return
	}
	n += sn
	return
}
func (cmd *EntryCommand[T, N]) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *EntryCommand[T, N]) ReadFrom(r io.Reader) (n int64, err error) {
	var sn2 int
	if sn2, err = r.Read(types.EncodeUint64(&cmd.DBID)); err != nil {
		return
	}
	n += int64(sn2)
	if sn2, err = r.Read(common.EncodeID(cmd.ID)); err != nil {
		return
	}
	n += int64(sn2)
	var sn int64
	if sn, err = cmd.mvccNode.ReadFrom(r); err != nil {
		return
	}
	n += sn
	if sn, err = cmd.node.ReadFrom(r); err != nil {
		return
	}
	n += sn
	return
}

func (cmd *EntryCommand[T, N]) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = cmd.ReadFrom(bbuf)
	return
}
