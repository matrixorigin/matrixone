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

func (cmd *EntryCommand) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, cmd.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.ID); err != nil {
		return
	}
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
		return cmd.Block.WriteTo(w)
	case CmdLogSegment:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Table.ID); err != nil {
			return
		}
		return cmd.Segment.WriteTo(w)
	case CmdLogTable:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		return cmd.Table.WriteTo(w)
	case CmdLogDatabase:
		return cmd.DB.WriteTo(w)
	}

	if err = binary.Write(w, binary.BigEndian, cmd.entry.GetID()); err != nil {
		return
	}
	switch cmd.GetType() {
	case CmdCreateDatabase:
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
		if _, err = common.WriteString(cmd.DB.name, w); err != nil {
			return
		}
	case CmdCreateTable:
		if err = binary.Write(w, binary.BigEndian, cmd.Table.db.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
		var schemaBuf []byte
		if schemaBuf, err = cmd.Table.schema.Marshal(); err != nil {
			return
		}
		if _, err = w.Write(schemaBuf); err != nil {
			return
		}
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
	case CmdDropDatabase:
		if err = binary.Write(w, binary.BigEndian, cmd.DB.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.DeleteAt); err != nil {
			return
		}
	}
	return
}
func (cmd *EntryCommand) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *EntryCommand) ReadFrom(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &cmd.ID); err != nil {
		return
	}
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
		return cmd.Block.ReadFrom(r)
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
		return cmd.Segment.ReadFrom(r)
	case CmdLogTable:
		cmd.Table = &TableEntry{
			BaseEntry: new(BaseEntry),
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		return cmd.Table.ReadFrom(r)
	case CmdLogDatabase:
		cmd.DB = &DBEntry{
			BaseEntry: new(BaseEntry),
		}
		return cmd.DB.ReadFrom(r)
	}

	cmd.entry = &BaseEntry{}
	if err = binary.Read(r, binary.BigEndian, &cmd.entry.ID); err != nil {
		return
	}
	switch cmd.GetType() {
	case CmdCreateDatabase:
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.entry.CurrOp = OpCreate
		cmd.DB = &DBEntry{
			BaseEntry: cmd.entry,
		}
		if cmd.DB.name, err = common.ReadString(r); err != nil {
			return
		}
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
		if err = cmd.Table.schema.ReadFrom(r); err != nil {
			return
		}
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
	case CmdDropDatabase:
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.DeleteAt); err != nil {
			return
		}
	}
	return
}
func (cmd *EntryCommand) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	err = cmd.ReadFrom(bbuf)
	return
}
