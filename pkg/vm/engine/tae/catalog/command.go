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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
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

var cmdNames = map[int16]string{
	CmdCreateDatabase: "CDB",
	CmdDropDatabase:   "DDB",
	CmdCreateTable:    "CTBL",
	CmdDropTable:      "DTBL",
	CmdCreateSegment:  "CSEG",
	CmdDropSegment:    "DSEG",
	CmdCreateBlock:    "CBLK",
	CmdDropBlock:      "DBLK",
	CmdLogDatabase:    "LDB",
	CmdLogTable:       "LTBL",
	CmdLogSegment:     "LSEG",
	CmdLogBlock:       "LBLK",
}

func CmdName(t int16) string {
	return cmdNames[t]
}

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

func (cmd *EntryCommand) Desc() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%d;CSN=%d", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs(), cmd.ID)
	return s
}

func (cmd *EntryCommand) GetLogIndex() *wal.Index {
	if cmd.entry == nil {
		return nil
	}
	return cmd.entry.LogIndex
}

func (cmd *EntryCommand) GetTs() types.TS {
	//ts := uint64(0)
	var ts types.TS
	switch cmd.cmdType {
	case CmdCreateDatabase, CmdCreateTable, CmdCreateSegment, CmdCreateBlock:
		ts = cmd.entry.CreateAt
	case CmdDropDatabase, CmdDropTable, CmdDropSegment, CmdDropBlock:
		ts = cmd.entry.DeleteAt
	case CmdLogDatabase:
	case CmdLogTable:
	case CmdLogSegment:
	case CmdLogBlock:
	}
	return ts
}
func (cmd *EntryCommand) IDString() string {
	s := ""
	dbid, id := cmd.GetID()
	switch cmd.cmdType {
	case CmdCreateDatabase, CmdDropDatabase, CmdLogDatabase:
		s = fmt.Sprintf("%sDB=%d", s, dbid)
	case CmdCreateTable, CmdDropTable, CmdLogTable:
		s = fmt.Sprintf("%sDB=%d;CommonID=%s", s, dbid, id.TableString())
	case CmdCreateSegment, CmdDropSegment, CmdLogSegment:
		s = fmt.Sprintf("%sDB=%d;CommonID=%s", s, dbid, id.SegmentString())
	case CmdCreateBlock, CmdDropBlock, CmdLogBlock:
		s = fmt.Sprintf("%sDB=%d;CommonID=%s", s, dbid, id.BlockString())
	}
	return s
}
func (cmd *EntryCommand) GetID() (uint64, *common.ID) {
	id := &common.ID{}
	dbid := uint64(0)
	switch cmd.cmdType {
	case CmdCreateDatabase:
		dbid = cmd.entry.ID
	case CmdDropDatabase:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
		} else {
			dbid = cmd.DB.ID
		}
	case CmdCreateTable:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.Table.ID
		} else {
			dbid = cmd.Table.db.ID
			id.TableID = cmd.entry.ID
		}
	case CmdDropTable:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.TableID
		} else {
			dbid = cmd.Table.db.ID
			id.TableID = cmd.Table.ID
		}
	case CmdCreateSegment:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.TableID
			id.SegmentID = cmd.entry.ID
		} else {
			dbid = cmd.DB.ID
			id.TableID = cmd.Table.ID
			id.SegmentID = cmd.Segment.ID
		}
	case CmdDropSegment:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.TableID
			id.SegmentID = cmd.entry.ID
		} else {
			dbid = cmd.DB.ID
			id.TableID = cmd.Table.ID
			id.SegmentID = cmd.entry.ID
		}
	case CmdCreateBlock:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.TableID
			id.SegmentID = cmd.SegmentID
			id.BlockID = cmd.entry.ID
		} else {
			dbid = cmd.DB.ID
			id.TableID = cmd.Table.ID
			id.SegmentID = cmd.Segment.ID
			id.BlockID = cmd.entry.ID
		}
	case CmdDropBlock:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.TableID
			id.SegmentID = cmd.SegmentID
			id.BlockID = cmd.entry.ID
		} else {
			dbid = cmd.Table.db.ID
			id.TableID = cmd.Table.ID
			id.SegmentID = cmd.Segment.ID
			id.BlockID = cmd.entry.ID
		}
	case CmdLogDatabase:
		dbid = cmd.DB.ID
	case CmdLogTable:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.Table.ID
		} else {
			dbid = cmd.DB.ID
			id.TableID = cmd.Table.ID
		}
	case CmdLogSegment:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.TableID
			id.SegmentID = cmd.Segment.ID
		} else {
			dbid = cmd.DB.ID
			id.TableID = cmd.Table.ID
			id.SegmentID = cmd.Segment.ID
		}
	case CmdLogBlock:
		if cmd.DBID != 0 {
			dbid = cmd.DBID
			id.TableID = cmd.TableID
			id.SegmentID = cmd.SegmentID
			id.BlockID = cmd.Block.ID
		} else {
			dbid = cmd.DB.ID
			id.TableID = cmd.Table.ID
			id.SegmentID = cmd.Segment.ID
			id.BlockID = cmd.Block.ID
		}
	}
	return dbid, id
}

func (cmd *EntryCommand) String() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%d;CSN=%d;BaseEntry=%s", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs(), cmd.ID, cmd.entry.String())
	return s
}

func (cmd *EntryCommand) VerboseString() string {
	s := fmt.Sprintf("CmdName=%s;%s;TS=%d;CSN=%d;BaseEntry=%s", CmdName(cmd.cmdType), cmd.IDString(), cmd.GetTs(), cmd.ID, cmd.entry.String())
	switch cmd.cmdType {
	case CmdCreateTable, CmdLogTable:
		s = fmt.Sprintf("%s;Schema=%v", s, cmd.Table.schema.String())
	}
	return s
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
		if err = binary.Write(w, binary.BigEndian, cmd.entry.ID); err != nil {
			return
		}
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
		if err = binary.Write(w, binary.BigEndian, cmd.entry.ID); err != nil {
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
		if err = binary.Write(w, binary.BigEndian, cmd.entry.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Segment.state); err != nil {
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
		if err = binary.Write(w, binary.BigEndian, cmd.Block.state); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.CreateAt); err != nil {
			return
		}
		n += 8 + 8 + 8 + 8 + 8
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
	case CmdDropSegment:
		if err = binary.Write(w, binary.BigEndian, cmd.Table.db.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Table.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.DeleteAt); err != nil {
			return
		}
		n += 8 + 8 + 8 + 8
	case CmdDropBlock:
		if err = binary.Write(w, binary.BigEndian, cmd.Table.db.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Table.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.Segment.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.ID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, cmd.entry.DeleteAt); err != nil {
			return
		}
		n += 8 + 8 + 8 + 8 + 8
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
		cmd.Block = NewReplayBlockEntry()
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
		cmd.Segment = NewReplaySegmentEntry()
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
		cmd.Table = NewReplayTableEntry()
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		cn, err = cmd.Table.ReadFrom(r)
		n += cn + 8
		return
	case CmdLogDatabase:
		cmd.DB = NewReplayDBEntry()
		cn, err = cmd.DB.ReadFrom(r)
		n += cn
		return
	}

	cmd.entry = NewReplayBaseEntry()
	if err = binary.Read(r, binary.BigEndian, &cmd.entry.ID); err != nil {
		return
	}
	var sn int64
	n += 8
	switch cmd.GetType() {
	case CmdCreateDatabase:
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.entry.CurrOp = OpCreate
		cmd.DB = NewReplayDBEntry()
		cmd.DB.BaseEntry = cmd.entry
		if cmd.DB.name, sn, err = common.ReadString(r); err != nil {
			return
		}
		n += sn + 8
	case CmdCreateTable:
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.entry.CurrOp = OpCreate
		cmd.Table = NewReplayTableEntry()
		cmd.Table.BaseEntry = cmd.entry
		cmd.Table.schema = NewEmptySchema("")
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
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.ID); err != nil {
			return
		}
		var state EntryState
		if err = binary.Read(r, binary.BigEndian, &state); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.entry.CurrOp = OpCreate
		cmd.Segment = &SegmentEntry{
			BaseEntry: cmd.entry,
			state:     state,
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
		var state EntryState
		if err = binary.Read(r, binary.BigEndian, &state); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.CreateAt); err != nil {
			return
		}
		cmd.entry.CurrOp = OpCreate
		cmd.Block = &BlockEntry{
			BaseEntry: cmd.entry,
			state:     state,
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
	case CmdDropSegment:
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.TableID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.DeleteAt); err != nil {
			return
		}
		n += 8 + 8 + 8 + 8
	case CmdDropBlock:
		if err = binary.Read(r, binary.BigEndian, &cmd.DBID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.TableID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.SegmentID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.ID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &cmd.entry.DeleteAt); err != nil {
			return
		}

		n += 8 + 8 + 8 + 8 + 8
	}
	return
}

func (cmd *EntryCommand) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = cmd.ReadFrom(bbuf)
	return
}
