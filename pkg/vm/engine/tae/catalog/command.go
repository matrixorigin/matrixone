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
}

type entryCmd struct {
	*txnbase.BaseCustomizedCmd
	db      *DBEntry
	table   *TableEntry
	entry   *BaseEntry
	segment *SegmentEntry
	block   *BlockEntry
	cmdType int16
}

func newEmptyEntryCmd(cmdType int16) *entryCmd {
	return newDBCmd(0, cmdType, nil)
}

func newBlockCmd(id uint32, cmdType int16, entry *BlockEntry) *entryCmd {
	impl := &entryCmd{
		db:      entry.GetSegment().GetTable().GetDB(),
		table:   entry.GetSegment().GetTable(),
		segment: entry.GetSegment(),
		block:   entry,
		cmdType: cmdType,
		entry:   entry.BaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newSegmentCmd(id uint32, cmdType int16, entry *SegmentEntry) *entryCmd {
	impl := &entryCmd{
		db:      entry.GetTable().GetDB(),
		table:   entry.GetTable(),
		segment: entry,
		cmdType: cmdType,
		entry:   entry.BaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newTableCmd(id uint32, cmdType int16, entry *TableEntry) *entryCmd {
	impl := &entryCmd{
		db:      entry.GetDB(),
		table:   entry,
		cmdType: cmdType,
		entry:   entry.BaseEntry,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func newDBCmd(id uint32, cmdType int16, entry *DBEntry) *entryCmd {
	impl := &entryCmd{
		db:      entry,
		cmdType: cmdType,
	}
	if entry != nil {
		impl.entry = entry.BaseEntry
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

// TODO
func (cmd *entryCmd) String() string {
	return ""
}
func (cmd *entryCmd) GetType() int16 { return cmd.cmdType }

func (cmd *entryCmd) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, cmd.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.ID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.entry.GetID()); err != nil {
		return
	}
	switch cmd.GetType() {
	case CmdCreateDatabase:
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
		if _, err = common.WriteString(cmd.db.name, w); err != nil {
			return
		}
	case CmdCreateTable:
		if err = binary.Write(w, binary.BigEndian, cmd.table.db.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
		var schemaBuf []byte
		if schemaBuf, err = cmd.table.schema.Marshal(); err != nil {
			return
		}
		if _, err = w.Write(schemaBuf); err != nil {
			return
		}
	case CmdCreateSegment:
		if err = binary.Write(w, binary.BigEndian, cmd.db.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.table.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
	case CmdCreateBlock:
		if err = binary.Write(w, binary.BigEndian, cmd.db.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.table.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.segment.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
	case CmdDropTable:
		if err = binary.Write(w, binary.BigEndian, cmd.table.db.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.DeleteAt); err != nil {
			return
		}
	case CmdDropDatabase:
		if err = binary.Write(w, binary.BigEndian, cmd.entry.DeleteAt); err != nil {
			return
		}
	}
	return
}
func (cmd *entryCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *entryCmd) ReadFrom(r io.Reader) (err error) {
	if err = binary.Read(r, binary.BigEndian, &cmd.ID); err != nil {
		return
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
		cmd.db = &DBEntry{
			BaseEntry: cmd.entry,
		}
		if cmd.db.name, err = common.ReadString(r); err != nil {
			return
		}
	case CmdCreateTable:
		cmd.db = &DBEntry{BaseEntry: &BaseEntry{}}
		if err = binary.Read(r, binary.BigEndian, &cmd.db.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.table = &TableEntry{
			BaseEntry: cmd.entry,
			schema:    new(Schema),
		}
		if err = cmd.table.schema.ReadFrom(r); err != nil {
			return
		}
	case CmdCreateSegment:
		cmd.db = &DBEntry{BaseEntry: &BaseEntry{}}
		cmd.table = &TableEntry{BaseEntry: &BaseEntry{}}
		if err = binary.Read(r, binary.BigEndian, &cmd.db.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.table.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.segment = &SegmentEntry{
			BaseEntry: cmd.entry,
		}
	case CmdCreateBlock:
		cmd.db = &DBEntry{BaseEntry: &BaseEntry{}}
		cmd.table = &TableEntry{BaseEntry: &BaseEntry{}}
		cmd.segment = &SegmentEntry{BaseEntry: &BaseEntry{}}
		if err = binary.Read(r, binary.BigEndian, &cmd.db.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.table.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.segment.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.segment = &SegmentEntry{
			BaseEntry: cmd.entry,
		}
	case CmdDropTable:
		cmd.db = &DBEntry{BaseEntry: &BaseEntry{}}
		if err = binary.Read(r, binary.BigEndian, &cmd.db.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.DeleteAt); err != nil {
			return
		}
	case CmdDropDatabase:
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.DeleteAt); err != nil {
			return
		}
	}
	return
}
func (cmd *entryCmd) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	err = cmd.ReadFrom(bbuf)
	return
}
