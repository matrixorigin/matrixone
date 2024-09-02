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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const (
	IOET_WALTxnCommand_Database uint16 = 3009
	IOET_WALTxnCommand_Table    uint16 = 3010
	IOET_WALTxnCommand_Segment  uint16 = 3011
	IOET_WALTxnCommand_Block    uint16 = 3012
	IOET_WALTxnCommand_Object   uint16 = 3015

	IOET_WALTxnCommand_Database_V1 uint16 = 1
	IOET_WALTxnCommand_Table_V1    uint16 = 1
	IOET_WALTxnCommand_Table_V2    uint16 = 2
	IOET_WALTxnCommand_Table_V3    uint16 = 3
	IOET_WALTxnCommand_Segment_V1  uint16 = 1
	IOET_WALTxnCommand_Block_V1    uint16 = 1
	IOET_WALTxnCommand_Object_V1   uint16 = 1

	IOET_WALTxnCommand_Database_CurrVer = IOET_WALTxnCommand_Database_V1
	IOET_WALTxnCommand_Table_CurrVer    = IOET_WALTxnCommand_Table_V3
	IOET_WALTxnCommand_Segment_CurrVer  = IOET_WALTxnCommand_Segment_V1
	IOET_WALTxnCommand_Block_CurrVer    = IOET_WALTxnCommand_Block_V1
	IOET_WALTxnCommand_Object_CurrVer   = IOET_WALTxnCommand_Object_V1
)

var cmdNames = map[uint16]string{
	IOET_WALTxnCommand_Database: "UDB",
	IOET_WALTxnCommand_Table:    "UTBL",
	IOET_WALTxnCommand_Segment:  "USEG",
	IOET_WALTxnCommand_Block:    "UBLK",
	IOET_WALTxnCommand_Object:   "UOBJ",
}

func CmdName(t uint16) string {
	return cmdNames[t]
}

func init() {
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_Database,
			Version: IOET_WALTxnCommand_Database_V1,
		}, nil,
		func(b []byte) (any, error) {
			cmd := newEmptyEntryCmd(IOET_WALTxnCommand_Database,
				NewEmptyMVCCNodeFactory(NewEmptyEmptyMVCCNode),
				func() *DBNode { return &DBNode{} },
				IOET_WALTxnCommand_Database_V1)
			err := cmd.UnmarshalBinary(b)
			return cmd, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_Table,
			Version: IOET_WALTxnCommand_Table_V1,
		}, nil,
		func(b []byte) (any, error) {
			cmd := newEmptyEntryCmd(IOET_WALTxnCommand_Table,
				NewEmptyMVCCNodeFactory(NewEmptyTableMVCCNode),
				func() *TableNode { return &TableNode{} },
				IOET_WALTxnCommand_Table_V1)
			err := cmd.UnmarshalBinary(b)
			return cmd, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_Table,
			Version: IOET_WALTxnCommand_Table_V2,
		}, nil,
		func(b []byte) (any, error) {
			cmd := newEmptyEntryCmd(IOET_WALTxnCommand_Table,
				NewEmptyMVCCNodeFactory(NewEmptyTableMVCCNode),
				func() *TableNode { return &TableNode{} },
				IOET_WALTxnCommand_Table_V2)
			err := cmd.UnmarshalBinary(b)
			return cmd, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_Table,
			Version: IOET_WALTxnCommand_Table_V3,
		}, nil,
		func(b []byte) (any, error) {
			cmd := newEmptyEntryCmd(IOET_WALTxnCommand_Table,
				NewEmptyMVCCNodeFactory(NewEmptyTableMVCCNode),
				func() *TableNode { return &TableNode{} },
				IOET_WALTxnCommand_Table_V3)
			err := cmd.UnmarshalBinary(b)
			return cmd, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_Segment,
			Version: IOET_WALTxnCommand_Segment_V1,
		}, nil,
		func(b []byte) (any, error) {
			cmd := newEmptyEntryCmd(IOET_WALTxnCommand_Segment,
				NewEmptyMVCCNodeFactory(NewEmptyMetadataMVCCNode),
				func() *ObjectNode { return &ObjectNode{} },
				IOET_WALTxnCommand_Segment_V1)
			err := cmd.UnmarshalBinary(b)
			return cmd, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_Block,
			Version: IOET_WALTxnCommand_Block_V1,
		}, nil,
		func(b []byte) (any, error) {
			cmd := newEmptyEntryCmd(IOET_WALTxnCommand_Block,
				NewEmptyMVCCNodeFactory(NewEmptyMetadataMVCCNode),
				func() *BlockNode { return &BlockNode{} },
				IOET_WALTxnCommand_Block_V1)
			err := cmd.UnmarshalBinary(b)
			return cmd, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_Object,
			Version: IOET_WALTxnCommand_Object_V1,
		}, nil,
		func(b []byte) (any, error) {
			cmd := newEmptyEntryCmd(IOET_WALTxnCommand_Object,
				NewEmptyMVCCNodeFactory(NewEmptyObjectMVCCNode),
				func() *ObjectNode { return &ObjectNode{} },
				IOET_WALTxnCommand_Object_V1)
			err := cmd.UnmarshalBinary(b)
			return cmd, err
		},
	)
}

type Node interface {
	WriteTo(w io.Writer) (n int64, err error)
	ReadFrom(r io.Reader) (n int64, err error)
}

type EntryCommand[T BaseNode[T], N Node] struct {
	*txnbase.BaseCustomizedCmd
	cmdType  uint16
	version  uint16
	ID       *common.ID
	mvccNode *MVCCNode[T]
	node     N
}

func newEmptyEntryCmd[T BaseNode[T], N Node](cmdType uint16, mvccNodeFactory func() *MVCCNode[T], nodeFactory func() N, ver uint16) *EntryCommand[T, N] {
	impl := &EntryCommand[T, N]{
		cmdType:  cmdType,
		ID:       &common.ID{},
		mvccNode: mvccNodeFactory(),
		node:     nodeFactory(),
		version:  ver,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(0, impl)
	return impl
}

func NewDeltalocCmd(id uint32, cmdType uint16, commonID *common.ID, baseEntry *BaseEntryImpl[*MetadataMVCCNode]) *EntryCommand[*MetadataMVCCNode, *BlockNode] {
	impl := &EntryCommand[*MetadataMVCCNode, *BlockNode]{
		ID:       commonID,
		cmdType:  cmdType,
		mvccNode: baseEntry.GetLatestNodeLocked(),
		node:     &BlockNode{},
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newObjectCmd(id uint32, cmdType uint16, entry *ObjectEntry) *EntryCommand[*ObjectMVCCNode, *ObjectNode] {
	impl := &EntryCommand[*ObjectMVCCNode, *ObjectNode]{
		ID:       entry.AsCommonID(),
		cmdType:  cmdType,
		mvccNode: entry.GetLatestNode().GetCommandMVCCNode(),
		node:     &entry.ObjectNode,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newTableCmd(id uint32, cmdType uint16, entry *TableEntry) *EntryCommand[*TableMVCCNode, *TableNode] {
	impl := &EntryCommand[*TableMVCCNode, *TableNode]{
		ID:       entry.AsCommonID(),
		cmdType:  cmdType,
		mvccNode: entry.BaseEntryImpl.GetLatestNodeLocked(),
		node:     entry.TableNode,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newDBCmd(id uint32, cmdType uint16, entry *DBEntry) *EntryCommand[*EmptyMVCCNode, *DBNode] {
	impl := &EntryCommand[*EmptyMVCCNode, *DBNode]{
		ID:       entry.AsCommonID(),
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
	s := fmt.Sprintf("CmdName=%s;%s;TS=%s;DEST=%s", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs().ToString(), cmd.ID)
	return s
}

func (cmd *EntryCommand[T, N]) SetReplayTxn(txn txnif.AsyncTxn) {
	cmd.mvccNode.Txn = txn
}

func (cmd *EntryCommand[T, N]) ApplyCommit() {
	if err := cmd.mvccNode.ApplyCommit(cmd.mvccNode.Txn.GetID()); err != nil {
		panic(err)
	}
}

func (cmd *EntryCommand[T, N]) ApplyRollback() {
	cmd.mvccNode.ApplyRollback()
}

func (cmd *EntryCommand[T, N]) GetTs() types.TS {
	ts := cmd.mvccNode.GetPrepare()
	return ts
}

func (cmd *EntryCommand[T, N]) IDString() string {
	s := ""
	id := cmd.GetID()
	switch cmd.cmdType {
	case IOET_WALTxnCommand_Database:
		s = fmt.Sprintf("%sCommonID=%s", s, id.DBString())
	case IOET_WALTxnCommand_Table:
		s = fmt.Sprintf("%sCommonID=%s", s, id.TableString())
	case IOET_WALTxnCommand_Segment:
		s = fmt.Sprintf("%sCommonID=%s", s, id.ObjectString())
	case IOET_WALTxnCommand_Object:
		s = fmt.Sprintf("%sCommonID=%s", s, id.ObjectString())
	case IOET_WALTxnCommand_Block:
		s = fmt.Sprintf("%sCommonID=%s", s, id.BlockString())
	}
	return s
}
func (cmd *EntryCommand[T, N]) GetID() *common.ID {
	return cmd.ID
}

func (cmd *EntryCommand[T, N]) String() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%s;DEST=%v;BaseEntry=%s", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs().ToString(), cmd.ID, cmd.mvccNode.String())
	return s
}

func (cmd *EntryCommand[T, N]) VerboseString() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%s;DEST=%v;BaseEntry=%s", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs().ToString(), cmd.ID, cmd.mvccNode.String())
	return s
}
func (cmd *EntryCommand[T, N]) GetType() uint16 { return cmd.cmdType }
func (cmd *EntryCommand[T, N]) GetCurrVersion() uint16 {
	switch cmd.cmdType {
	case IOET_WALTxnCommand_Database:
		return IOET_WALTxnCommand_Database_CurrVer
	case IOET_WALTxnCommand_Table:
		return IOET_WALTxnCommand_Table_CurrVer
	case IOET_WALTxnCommand_Object:
		return IOET_WALTxnCommand_Object_CurrVer
	case IOET_WALTxnCommand_Segment:
		return IOET_WALTxnCommand_Segment_CurrVer
	case IOET_WALTxnCommand_Block:
		return IOET_WALTxnCommand_Block_CurrVer
	default:
		panic(fmt.Sprintf("not support type %d", cmd.cmdType))
	}
}

func (cmd *EntryCommand[T, N]) WriteTo(w io.Writer) (n int64, err error) {
	t := cmd.GetType()
	if _, err = w.Write(types.EncodeUint16(&t)); err != nil {
		return
	}
	n += 2
	ver := cmd.GetCurrVersion()
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return
	}
	n += 2
	var sn2 int
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
func (cmd *EntryCommand[T, N]) MarshalBinary() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *EntryCommand[T, N]) ReadFrom(r io.Reader) (n int64, err error) {
	var sn2 int
	if sn2, err = r.Read(common.EncodeID(cmd.ID)); err != nil {
		return
	}
	n += int64(sn2)
	var sn int64
	if sn, err = cmd.mvccNode.ReadFromWithVersion(r, cmd.version); err != nil {
		return
	}
	n += sn
	if sn, err = cmd.node.ReadFrom(r); err != nil {
		return
	}
	n += sn
	return
}

func (cmd *EntryCommand[T, N]) UnmarshalBinary(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = cmd.ReadFrom(bbuf)
	return
}
