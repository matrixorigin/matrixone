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
	"encoding/binary"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const (
	CmdCreateDatabase = int16(256) + iota
	CmdDropDatabase
	CmdCreateTable
	CmdDropTable
	CmdCreateSegment
	CmdDropSegment
	CmdCreateBlock
	CmdDropBlock
	CmdLogDatabase
	CmdLogTable
	CmdLogSegment
	CmdLogBlock
)

func init() {
	txnif.RegisterCmdFactory(CmdCreateDatabase, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdDropDatabase, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdCreateTable, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdDropTable, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdCreateSegment, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdDropSegment, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdCreateBlock, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdDropBlock, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdLogBlock, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdLogSegment, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdLogTable, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
	txnif.RegisterCmdFactory(CmdLogDatabase, func(cmdType int16) txnif.TxnCmd {
		return newEmptyEntryCmd(cmdType)
	})
}

type EntryCommand struct {
	*txnbase.BaseCustomizedCmd
	cmdType   int16
	entry     *BaseEntry
	DBID      uint64
	TableID   uint64
	SegmentID uint64
	DB        *DBEntry
	Table     *TableEntry
	Segment   *SegmentEntry
	Block     *BlockEntry
}

func newEmptyEntryCmd(cmdType int16) *EntryCommand {
	return newDBCmd(0, cmdType, nil)
}

func newBlockCmd(id uint32, cmdType int16, entry *BlockEntry) *EntryCommand {
	impl := &EntryCommand{
		DB:      entry.GetSegment().GetTable().GetDB(),
		Table:   entry.GetSegment().GetTable(),
		Segment: entry.GetSegment(),
		Block:   entry,
		cmdType: cmdType,
		entry:   entry.BaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newSegmentCmd(id uint32, cmdType int16, entry *SegmentEntry) *EntryCommand {
	impl := &EntryCommand{
		DB:      entry.GetTable().GetDB(),
		Table:   entry.GetTable(),
		Segment: entry,
		cmdType: cmdType,
		entry:   entry.BaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newTableCmd(id uint32, cmdType int16, entry *TableEntry) *EntryCommand {
	impl := &EntryCommand{
		DB:      entry.GetDB(),
		Table:   entry,
		cmdType: cmdType,
		entry:   entry.BaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newDBCmd(id uint32, cmdType int16, entry *DBEntry) *EntryCommand {
	impl := &EntryCommand{
		DB:      entry,
		cmdType: cmdType,
	}
	if entry != nil {
		impl.entry = entry.BaseEntry
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

// TODO
func (cmd *EntryCommand) String() string {
	return ""
}
func (cmd *EntryCommand) GetType() int16 { return cmd.cmdType }

func (cmd *EntryCommand) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, cmd.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.ID); err != nil {
		return
	}
	var sn int64
	n = 4 + 2
	switch cmd.GetType() {
	case CmdLogBlock:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Table.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Segment.ID); err != nil {
			return
		}
		sn, err = cmd.Block.WriteTo(w)
		n += sn + 8 + 8 + 8
		return
	case CmdLogSegment:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Table.ID); err != nil {
			return
		}
		sn, err = cmd.Segment.WriteTo(w)
		n += sn + 8 + 8
		return
	case CmdLogTable:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		sn, err = cmd.Table.WriteTo(w)
		n += sn + 8
		return
	case CmdLogDatabase:
		sn, err = cmd.DB.WriteTo(w)
		n += sn
		return
	}

	if err = binary.Write(w, binary.BigEndian, cmd.entry.GetID()); err != nil {
		return
	}
	n += 4
	switch cmd.GetType() {
	case CmdCreateDatabase:
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
		if sn, err = common.WriteString(cmd.DB.name, w); err != nil {
			return
		}
		n += sn + 8
	case CmdCreateTable:
		if err = binary.Write(w, binary.BigEndian, cmd.Table.db.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
		n += 8 + 8
		var schemaBuf []byte
		if schemaBuf, err = cmd.Table.schema.Marshal(); err != nil {
			return
		}
		if _, err = w.Write(schemaBuf); err != nil {
			return
		}
		n += int64(len(schemaBuf))
	case CmdCreateSegment:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Table.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
		n += 8 + 8 + 8
	case CmdCreateBlock:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Table.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Segment.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
		n += 8 + 8 + 8 + 8
	case CmdDropTable:
		if err = binary.Write(w, binary.BigEndian, cmd.Table.db.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Table.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.DeleteAt); err != nil {
			return
		}
		n += 8 + 8 + 8
	case CmdDropDatabase:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.DeleteAt); err != nil {
			return
		}
		n += 8 + 8
	}
	return
}
func (cmd *EntryCommand) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *EntryCommand) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &cmd.ID); err != nil {
		return
	}
	n += 4
	var cn int64
	switch cmd.GetType() {
	case CmdLogBlock:
		cmd.Block = &BlockEntry{
			BaseEntry: new(BaseEntry),
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.TableID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.SegmentID); err != nil {
			return
		}

		cn, err = cmd.Block.ReadFrom(r)
		n += cn + 24
		return
	case CmdLogSegment:
		cmd.Segment = &SegmentEntry{
			BaseEntry: new(BaseEntry),
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.TableID); err != nil {
			return
		}
		cn, err = cmd.Segment.ReadFrom(r)
		n += cn + 16
		return
	case CmdLogTable:
		cmd.Table = &TableEntry{
			BaseEntry: new(BaseEntry),
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		cn, err = cmd.Table.ReadFrom(r)
		n += cn + 8
		return
	case CmdLogDatabase:
		cmd.DB = &DBEntry{
			BaseEntry: new(BaseEntry),
		}
		cn, err = cmd.DB.ReadFrom(r)
		n += cn
		return
	}

	cmd.entry = &BaseEntry{}
	if err = binary.Read(r, binary.BigEndian, &cmd.entry.ID); err != nil {
		return
	}
	var sn int64
	n += 8
	switch cmd.GetType() {
	case CmdCreateDatabase:
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.entry.CurrOp = OpCreate
		cmd.DB = &DBEntry{
			BaseEntry: cmd.entry,
		}
		if cmd.DB.name, sn, err = common.ReadString(r); err != nil {
			return
		}
		n += sn + 8
	case CmdCreateTable:
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.entry.CurrOp = OpCreate
		cmd.Table = &TableEntry{
			BaseEntry: cmd.entry,
			schema:    new(Schema),
		}
		if sn, err = cmd.Table.schema.ReadFrom(r); err != nil {
			return
		}
		n += sn + 8 + 8
	case CmdCreateSegment:
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.TableID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.entry.CurrOp = OpCreate
		cmd.Segment = &SegmentEntry{
			BaseEntry: cmd.entry,
		}
		n += 8 + 8 + 8
	case CmdCreateBlock:
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.TableID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.SegmentID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.entry.CurrOp = OpCreate
		cmd.Block = &BlockEntry{
			BaseEntry: cmd.entry,
		}
		n += 8 + 8 + 8 + 8
	case CmdDropTable:
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.TableID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.DeleteAt); err != nil {
			return
		}
		n += 8 + 8 + 8
	case CmdDropDatabase:
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.DeleteAt); err != nil {
			return
		}
		n += 8 + 8
	}
	return
}
func (cmd *EntryCommand) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = cmd.ReadFrom(bbuf)
	return
}
