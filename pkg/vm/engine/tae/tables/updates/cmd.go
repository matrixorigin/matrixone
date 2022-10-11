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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

func init() {
	txnif.RegisterCmdFactory(txnbase.CmdDelete, func(int16) txnif.TxnCmd {
		return NewEmptyCmd(txnbase.CmdDelete)
	})
	txnif.RegisterCmdFactory(txnbase.CmdAppend, func(int16) txnif.TxnCmd {
		return NewEmptyCmd(txnbase.CmdAppend)
	})
}

type UpdateCmd struct {
	*txnbase.BaseCustomizedCmd
	dbid    uint64
	dest    *common.ID
	delete  *DeleteNode
	append  *AppendNode
	cmdType int16
}

func NewEmptyCmd(cmdType int16) *UpdateCmd {
	cmd := &UpdateCmd{}
	cmd.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(0, cmd)
	cmd.cmdType = cmdType
	if cmdType == txnbase.CmdDelete {
		cmd.delete = NewDeleteNode(nil, 0)
	} else if cmdType == txnbase.CmdAppend {
		cmd.append = NewAppendNode(nil, 0, 0, nil)
	}
	return cmd
}

func NewAppendCmd(id uint32, app *AppendNode) *UpdateCmd {
	impl := &UpdateCmd{
		append:  app,
		cmdType: txnbase.CmdAppend,
		dest:    app.mvcc.meta.AsCommonID(),
		dbid:    app.mvcc.meta.GetSegment().GetTable().GetDB().ID,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func NewDeleteCmd(id uint32, del *DeleteNode) *UpdateCmd {
	impl := &UpdateCmd{
		delete:  del,
		cmdType: txnbase.CmdDelete,
		dest:    del.chain.mvcc.meta.AsCommonID(),
		dbid:    del.chain.mvcc.meta.GetSegment().GetTable().GetDB().ID,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func (c *UpdateCmd) GetAppendNode() *AppendNode {
	return c.append
}
func (c *UpdateCmd) GetDeleteNode() *DeleteNode {
	return c.delete
}
func (c *UpdateCmd) GetDBID() uint64 {
	return c.dbid
}

func (c *UpdateCmd) GetDest() *common.ID {
	return c.dest
}

func (c *UpdateCmd) Desc() string {
	if c.cmdType == txnbase.CmdAppend {
		return fmt.Sprintf("CmdName=Append;Dest=%s;%s;CSN=%d", c.dest.BlockString(), c.append.GeneralDesc(), c.ID)
	} else if c.cmdType == txnbase.CmdDelete {
		return fmt.Sprintf("CmdName=Delete;Dest=%s;%s;CSN=%d", c.dest.BlockString(), c.delete.GeneralDesc(), c.ID)
	}
	panic(moerr.NewInternalError("unknown cmd type: %d", c.cmdType))
}

func (c *UpdateCmd) String() string {
	if c.cmdType == txnbase.CmdAppend {
		return fmt.Sprintf("CmdName=Append;Dest=%s;%s;CSN=%d", c.dest.BlockString(), c.append.GeneralString(), c.ID)
	} else if c.cmdType == txnbase.CmdDelete {
		return fmt.Sprintf("CmdName=Delete;Dest=%s;%s;CSN=%d", c.dest.BlockString(), c.delete.GeneralString(), c.ID)
	}
	panic(moerr.NewInternalError("unknown cmd type: %d", c.cmdType))
}

func (c *UpdateCmd) VerboseString() string {
	if c.cmdType == txnbase.CmdAppend {
		return fmt.Sprintf("CmdName=Append;Dest=%s;CSN=%d;%s", c.dest.BlockString(), c.ID, c.append.GeneralVerboseString())
	} else if c.cmdType == txnbase.CmdDelete {
		return fmt.Sprintf("CmdName=Delete;Dest=%s;CSN=%d;%s", c.dest.BlockString(), c.ID, c.delete.GeneralVerboseString())
	}
	panic(moerr.NewInternalError("unknown cmd type: %d", c.cmdType))
}

func (c *UpdateCmd) GetType() int16 { return c.cmdType }

func (c *UpdateCmd) WriteTo(w io.Writer) (n int64, err error) {
	var sn int64
	if err = binary.Write(w, binary.BigEndian, c.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, c.ID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, c.dbid); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, c.dest.TableID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, c.dest.SegmentID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, c.dest.BlockID); err != nil {
		return
	}
	switch c.GetType() {
	case txnbase.CmdDelete:
		sn, err = c.delete.WriteTo(w)
	case txnbase.CmdAppend:
		sn, err = c.append.WriteTo(w)
	}
	n += sn + 2 + 4
	return
}

func (c *UpdateCmd) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &c.ID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &c.dbid); err != nil {
		return
	}
	c.dest = &common.ID{}
	if err = binary.Read(r, binary.BigEndian, &c.dest.TableID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &c.dest.SegmentID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &c.dest.BlockID); err != nil {
		return
	}
	switch c.GetType() {
	case txnbase.CmdDelete:
		n, err = c.delete.ReadFrom(r)
	case txnbase.CmdAppend:
		n, err = c.append.ReadFrom(r)
	}
	n += 4
	return
}

func (c *UpdateCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (c *UpdateCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := c.ReadFrom(bbuf)
	return err
}
