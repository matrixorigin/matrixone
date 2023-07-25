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
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const (
	IOET_WALTxnCommand_AppendNode          uint16 = 3004
	IOET_WALTxnCommand_DeleteNode          uint16 = 3005
	IOET_WALTxnCommand_PersistedDeleteNode uint16 = 3013

	IOET_WALTxnCommand_AppendNode_V1          uint16 = 1
	IOET_WALTxnCommand_DeleteNode_V1          uint16 = 1
	IOET_WALTxnCommand_PersistedDeleteNode_V1 uint16 = 1

	IOET_WALTxnCommand_AppendNode_CurrVer          = IOET_WALTxnCommand_AppendNode_V1
	IOET_WALTxnCommand_DeleteNode_CurrVer          = IOET_WALTxnCommand_DeleteNode_V1
	IOET_WALTxnCommand_PersistedDeleteNode_CurrVer = IOET_WALTxnCommand_PersistedDeleteNode_V1
)

func init() {
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_AppendNode,
			Version: IOET_WALTxnCommand_AppendNode_V1,
		},
		nil,
		func(b []byte) (any, error) {
			txnCmd := NewEmptyCmd(IOET_WALTxnCommand_AppendNode)
			err := txnCmd.UnmarshalBinary(b)
			return txnCmd, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_DeleteNode,
			Version: IOET_WALTxnCommand_DeleteNode_V1,
		},
		nil,
		func(b []byte) (any, error) {
			txnCmd := NewEmptyCmd(IOET_WALTxnCommand_DeleteNode)
			err := txnCmd.UnmarshalBinary(b)
			return txnCmd, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_PersistedDeleteNode,
			Version: IOET_WALTxnCommand_PersistedDeleteNode_V1,
		},
		nil,
		func(b []byte) (any, error) {
			txnCmd := NewEmptyCmd(IOET_WALTxnCommand_PersistedDeleteNode)
			err := txnCmd.UnmarshalBinary(b)
			return txnCmd, err
		},
	)
}

type UpdateCmd struct {
	*txnbase.BaseCustomizedCmd
	dest    *common.ID
	delete  *DeleteNode
	append  *AppendNode
	cmdType uint16
}

func NewEmptyCmd(cmdType uint16) *UpdateCmd {
	cmd := &UpdateCmd{}
	cmd.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(0, cmd)
	cmd.cmdType = cmdType
	if cmdType == IOET_WALTxnCommand_DeleteNode {
		cmd.delete = NewDeleteNode(nil, 0)
	} else if cmdType == IOET_WALTxnCommand_AppendNode {
		cmd.append = NewAppendNode(nil, 0, 0, nil)
	} else if cmdType == IOET_WALTxnCommand_PersistedDeleteNode {
		cmd.delete = NewEmptyPersistedDeleteNode()
	}
	return cmd
}

func NewAppendCmd(id uint32, app *AppendNode) *UpdateCmd {
	impl := &UpdateCmd{
		append:  app,
		cmdType: IOET_WALTxnCommand_AppendNode,
		dest:    app.mvcc.meta.AsCommonID(),
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func NewDeleteCmd(id uint32, del *DeleteNode) *UpdateCmd {
	impl := &UpdateCmd{
		delete:  del,
		cmdType: IOET_WALTxnCommand_DeleteNode,
		dest:    del.chain.Load().mvcc.meta.AsCommonID(),
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}

func NewPersistedDeleteCmd(id uint32, del *DeleteNode) *UpdateCmd {
	impl := &UpdateCmd{
		delete:  del,
		cmdType: IOET_WALTxnCommand_PersistedDeleteNode,
		dest:    del.chain.Load().mvcc.meta.AsCommonID(),
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

func (c *UpdateCmd) GetDest() *common.ID {
	return c.dest
}
func (c *UpdateCmd) SetReplayTxn(txn txnif.AsyncTxn) {
	switch c.cmdType {
	case IOET_WALTxnCommand_AppendNode:
		c.append.Txn = txn
	case IOET_WALTxnCommand_DeleteNode, IOET_WALTxnCommand_PersistedDeleteNode:
		c.delete.Txn = txn
	default:
		panic(fmt.Sprintf("invalid command type %d", c.cmdType))
	}
}
func (c *UpdateCmd) GetCurrentVersion() uint16 {
	switch c.cmdType {
	case IOET_WALTxnCommand_AppendNode:
		return IOET_WALTxnCommand_AppendNode_CurrVer
	case IOET_WALTxnCommand_DeleteNode:
		return IOET_WALTxnCommand_DeleteNode_CurrVer
	case IOET_WALTxnCommand_PersistedDeleteNode:
		return IOET_WALTxnCommand_PersistedDeleteNode_CurrVer
	default:
		panic(fmt.Sprintf("invalid command type %d", c.cmdType))
	}
}
func (c *UpdateCmd) ApplyCommit() {
	switch c.cmdType {
	case IOET_WALTxnCommand_AppendNode:
		if _, err := c.append.TxnMVCCNode.ApplyCommit(); err != nil {
			panic(err)
		}
	case IOET_WALTxnCommand_DeleteNode, IOET_WALTxnCommand_PersistedDeleteNode:
		if _, err := c.delete.TxnMVCCNode.ApplyCommit(); err != nil {
			panic(err)
		}
	default:
		panic(fmt.Sprintf("invalid command type %d", c.cmdType))
	}
}
func (c *UpdateCmd) ApplyRollback() {
	switch c.cmdType {
	case IOET_WALTxnCommand_AppendNode:
		if err := c.append.ApplyRollback(); err != nil {
			panic(err)
		}
	case IOET_WALTxnCommand_DeleteNode, IOET_WALTxnCommand_PersistedDeleteNode:
		if err := c.delete.ApplyRollback(); err != nil {
			panic(err)
		}
	default:
		panic(fmt.Sprintf("invalid command type %d", c.cmdType))
	}
}
func (c *UpdateCmd) Desc() string {
	switch c.cmdType {
	case IOET_WALTxnCommand_AppendNode:
		return fmt.Sprintf("CmdName=Append;Dest=%s;%s;CSN=%d", c.dest.BlockString(), c.append.GeneralDesc(), c.ID)
	case IOET_WALTxnCommand_DeleteNode, IOET_WALTxnCommand_PersistedDeleteNode:
		return fmt.Sprintf("CmdName=Delete;Dest=%s;%s;CSN=%d", c.dest.BlockString(), c.delete.GeneralDesc(), c.ID)
	}
	panic(moerr.NewInternalErrorNoCtx("unknown cmd type: %d", c.cmdType))
}

func (c *UpdateCmd) String() string {
	switch c.cmdType {
	case IOET_WALTxnCommand_AppendNode:
		return fmt.Sprintf("CmdName=Append;Dest=%s;%s;CSN=%d", c.dest.BlockString(), c.append.GeneralString(), c.ID)
	case IOET_WALTxnCommand_DeleteNode, IOET_WALTxnCommand_PersistedDeleteNode:
		return fmt.Sprintf("CmdName=Delete;Dest=%s;%s;CSN=%d", c.dest.BlockString(), c.delete.GeneralString(), c.ID)
	}
	panic(moerr.NewInternalErrorNoCtx("unknown cmd type: %d", c.cmdType))
}

func (c *UpdateCmd) VerboseString() string {
	switch c.cmdType {
	case IOET_WALTxnCommand_AppendNode:
		return fmt.Sprintf("CmdName=Append;Dest=%s;CSN=%d;%s", c.dest.BlockString(), c.ID, c.append.GeneralVerboseString())
	case IOET_WALTxnCommand_DeleteNode, IOET_WALTxnCommand_PersistedDeleteNode:
		return fmt.Sprintf("CmdName=Delete;Dest=%s;CSN=%d;%s", c.dest.BlockString(), c.ID, c.delete.GeneralVerboseString())
	}
	panic(moerr.NewInternalErrorNoCtx("unknown cmd type: %d", c.cmdType))
}

func (c *UpdateCmd) GetType() uint16 { return c.cmdType }

func (c *UpdateCmd) WriteTo(w io.Writer) (n int64, err error) {
	var sn int64
	if _, err = w.Write(types.EncodeUint16(&c.cmdType)); err != nil {
		return
	}
	ver := c.GetCurrentVersion()
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return
	}

	if _, err = w.Write(types.EncodeUint32(&c.ID)); err != nil {
		return
	}
	n += 6
	if _, err = w.Write(common.EncodeID(c.dest)); err != nil {
		return
	}
	n += common.IDSize
	switch c.GetType() {
	case IOET_WALTxnCommand_DeleteNode, IOET_WALTxnCommand_PersistedDeleteNode:
		sn, err = c.delete.WriteTo(w)
	case IOET_WALTxnCommand_AppendNode:
		sn, err = c.append.WriteTo(w)
	}
	n += sn
	return
}

func (c *UpdateCmd) ReadFrom(r io.Reader) (n int64, err error) {
	if _, err = r.Read(types.EncodeUint32(&c.ID)); err != nil {
		return
	}
	c.dest = &common.ID{}
	if _, err = r.Read(common.EncodeID(c.dest)); err != nil {
		return
	}
	switch c.GetType() {
	case IOET_WALTxnCommand_DeleteNode, IOET_WALTxnCommand_PersistedDeleteNode:
		n, err = c.delete.ReadFrom(r)
	case IOET_WALTxnCommand_AppendNode:
		n, err = c.append.ReadFrom(r)
	}
	n += 4 + common.IDSize
	return
}

func (c *UpdateCmd) MarshalBinary() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (c *UpdateCmd) UnmarshalBinary(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := c.ReadFrom(bbuf)
	return err
}
