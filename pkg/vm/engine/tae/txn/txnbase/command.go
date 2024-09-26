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

package txnbase

import (
	"bytes"
	"fmt"
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

const (
	IOET_WALTxnEntry            uint16 = 3000
	IOET_WALTxnCommand_Composed uint16 = 3001
	IOET_WALTxnCommand_TxnState uint16 = 3002

	IOET_WALTxnEntry_V1            uint16 = 1
	IOET_WALTxnEntry_V2            uint16 = 2
	IOET_WALTxnEntry_V3            uint16 = 3
	IOET_WALTxnCommand_Composed_V1 uint16 = 1
	IOET_WALTxnCommand_TxnState_V1 uint16 = 1

	IOET_WALTxnEntry_CurrVer            = IOET_WALTxnEntry_V3
	IOET_WALTxnCommand_Composed_CurrVer = IOET_WALTxnCommand_Composed_V1
	IOET_WALTxnCommand_TxnState_CurrVer = IOET_WALTxnCommand_TxnState_V1

	// CmdBufReserved is reserved size of cmd buffer, mainly the size of TxnCtx.Memo.
	// ComposedCmd.CmdBufLimit is the max buffer size that could be sent out to log-service.
	// This value is normally the max RPC message size which is configured in config of DN.
	// The message contains mainly commands, but also other information whose size is CmdBufReserved.
	// TODO(volgariver6): this buf size is about the max size of TxnCt.Memo, we need to calculate
	// the exact size of it.
	CmdBufReserved = 1024 * 1024 * 10
)

func init() {
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnEntry,
			Version: IOET_WALTxnEntry_V1,
		},
		nil,
		func(b []byte) (any, error) {
			txnEntry := NewEmptyTxnCmd()
			err := txnEntry.UnmarshalBinaryWithVersion(b, IOET_WALTxnEntry_V1)
			return txnEntry, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnEntry,
			Version: IOET_WALTxnEntry_V2,
		},
		nil,
		func(b []byte) (any, error) {
			txnEntry := NewEmptyTxnCmd()
			err := txnEntry.UnmarshalBinaryWithVersion(b, IOET_WALTxnEntry_V2)
			return txnEntry, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnEntry,
			Version: IOET_WALTxnEntry_V3,
		},
		nil,
		func(b []byte) (any, error) {
			txnEntry := NewEmptyTxnCmd()
			err := txnEntry.UnmarshalBinaryWithVersion(b, IOET_WALTxnEntry_V3)
			return txnEntry, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_Composed,
			Version: IOET_WALTxnCommand_Composed_V1,
		},
		nil,
		func(b []byte) (any, error) {
			txnEntry := NewComposedCmd(0)
			err := txnEntry.UnmarshalBinary(b)
			return txnEntry, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_TxnState,
			Version: IOET_WALTxnCommand_TxnState_V1,
		},
		nil,
		func(b []byte) (any, error) {
			txnEntry := NewEmptyTxnStateCmd()
			err := txnEntry.UnmarshalBinary(b)
			return txnEntry, err
		},
	)
}

type BaseCmd struct{}

func (base *BaseCmd) Close() {}

type TxnCmd struct {
	*ComposedCmd
	*TxnCtx
	Txn txnif.AsyncTxn

	// for replay
	isLast bool
	Lsn    uint64
}

type TxnStateCmd struct {
	ID       string
	State    txnif.TxnState
	CommitTs types.TS
}

type ComposedCmd struct {
	Cmds []txnif.TxnCmd

	// CmdBufLimit indicates max cmd buffer size. We can only send out
	// the cmd buffer whose size is less than it.
	CmdBufLimit int64

	// lastPos is the position in the Cmds list, before which the cmds have
	// been marshalled into buffer.
	LastPos int
}

type BaseCustomizedCmd struct {
	BaseCmd
	ID   uint32
	Impl txnif.TxnCmd
}

func NewBaseCustomizedCmd(id uint32, impl txnif.TxnCmd) *BaseCustomizedCmd {
	return &BaseCustomizedCmd{
		ID:   id,
		Impl: impl,
	}
}

func NewComposedCmd(maxSize uint64) *ComposedCmd {
	if maxSize < CmdBufReserved {
		maxSize = math.MaxInt64
	}
	return &ComposedCmd{
		Cmds:        make([]txnif.TxnCmd, 0),
		CmdBufLimit: int64(maxSize - CmdBufReserved),
		LastPos:     -1, // init value.
	}
}

func (c *BaseCustomizedCmd) GetID() uint32 {
	return c.ID
}

func NewEmptyTxnStateCmd() *TxnStateCmd {
	return &TxnStateCmd{}
}

func NewTxnStateCmd(id string, state txnif.TxnState, cts types.TS) *TxnStateCmd {
	return &TxnStateCmd{
		ID:       id,
		State:    state,
		CommitTs: cts,
	}
}

func (c *TxnStateCmd) WriteTo(w io.Writer) (n int64, err error) {
	t := c.GetType()
	if _, err = w.Write(types.EncodeUint16(&t)); err != nil {
		return
	}
	n += 2
	ver := IOET_WALTxnCommand_TxnState_CurrVer
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return
	}
	n += 2
	var sn int64
	if sn, err = objectio.WriteString(c.ID, w); err != nil {
		return
	}
	n += sn
	state := int32(c.State)
	if _, err = w.Write(types.EncodeInt32(&state)); err != nil {
		return
	}
	n += 4
	if _, err = w.Write(c.CommitTs[:]); err != nil {
		return
	}
	n += types.TxnTsSize
	return
}

func (c *TxnStateCmd) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int64
	if c.ID, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn
	state := int32(0)
	if _, err = r.Read(types.EncodeInt32(&state)); err != nil {
		return
	}
	c.State = txnif.TxnState(state)
	n += 4
	if _, err = r.Read(c.CommitTs[:]); err != nil {
		return
	}
	n += types.TxnTsSize
	return
}
func (c *TxnStateCmd) MarshalBinary() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (c *TxnStateCmd) ApplyCommit()                  {}
func (c *TxnStateCmd) ApplyRollback()                {}
func (c *TxnStateCmd) SetReplayTxn(_ txnif.AsyncTxn) {}
func (c *TxnStateCmd) UnmarshalBinary(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = c.ReadFrom(bbuf)
	return err
}
func (c *TxnStateCmd) GetType() uint16 { return IOET_WALTxnCommand_TxnState }
func (c *TxnStateCmd) Desc() string {
	return fmt.Sprintf("Tid=%s,State=%s,Cts=%s", c.ID, txnif.TxnStrState(c.State), c.CommitTs.ToString())
}
func (c *TxnStateCmd) String() string {
	return fmt.Sprintf("Tid=%s,State=%v,Cts=%s", c.ID, txnif.TxnStrState(c.State), c.CommitTs.ToString())
}
func (c *TxnStateCmd) VerboseString() string {
	return fmt.Sprintf("Tid=%s,State=%v,Cts=%s", c.ID, txnif.TxnStrState(c.State), c.CommitTs.ToString())
}
func (c *TxnStateCmd) Close() {
}

func NewTxnCmd(maxMessageSize uint64) *TxnCmd {
	return &TxnCmd{
		ComposedCmd: NewComposedCmd(maxMessageSize),
		TxnCtx:      NewEmptyTxnCtx(),
	}
}
func NewEmptyTxnCmd() *TxnCmd {
	return &TxnCmd{
		TxnCtx: NewEmptyTxnCtx(),
	}
}
func NewLastTxnCmd() *TxnCmd {
	return &TxnCmd{
		isLast: true,
	}
}
func (c *TxnCmd) IsLastCmd() bool { return c.isLast }
func (c *TxnCmd) ApplyCommit() {
	c.ComposedCmd.ApplyCommit()
}
func (c *TxnCmd) ApplyRollback() {
	c.ComposedCmd.ApplyRollback()
}
func (c *TxnCmd) SetReplayTxn(txn txnif.AsyncTxn) {
	c.ComposedCmd.SetReplayTxn(txn)
}
func (c *TxnCmd) SetTxn(txn txnif.AsyncTxn) {
	c.Txn = txn
	c.ID = txn.GetID()
	c.StartTS = txn.GetStartTS()
	c.PrepareTS = txn.GetPrepareTS()
	c.Participants = txn.GetParticipants()
	c.Memo = txn.GetMemo()
}
func (c *TxnCmd) WriteTo(w io.Writer) (n int64, err error) {
	t := c.GetType()
	if _, err = w.Write(types.EncodeUint16(&t)); err != nil {
		return
	}
	n += 2
	v := IOET_WALTxnEntry_CurrVer
	if _, err = w.Write(types.EncodeUint16(&v)); err != nil {
		return
	}
	n += 2
	var sn int64
	buf, err := c.ComposedCmd.MarshalBinary()
	if err != nil {
		return
	}
	if sn, err = objectio.WriteBytes(buf, w); err != nil {
		return
	}
	n += sn
	if sn, err = objectio.WriteString(c.ID, w); err != nil {
		return
	}
	n += sn
	//start ts
	if _, err = w.Write(c.StartTS[:]); err != nil {
		return
	}
	n += types.TxnTsSize
	//prepare ts
	if _, err = w.Write(c.PrepareTS[:]); err != nil {
		return
	}
	n += types.TxnTsSize
	//participants
	length := uint32(len(c.Participants))
	if _, err = w.Write(types.EncodeUint32(&length)); err != nil {
		return
	}
	n += 4
	for _, p := range c.Participants {
		if _, err = w.Write(types.EncodeUint64(&p)); err != nil {
			return
		}
		n += 8
	}
	if sn, err = c.Memo.WriteTo(w); err != nil {
		return
	}
	n += sn
	return
}
func (c *TxnCmd) ReadFromWithVersion(r io.Reader, ver uint16) (n int64, err error) {
	var sn int64
	if c.ID, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn
	// start timestamp
	if _, err = r.Read(c.StartTS[:]); err != nil {
		return
	}
	n += types.TxnTsSize
	// prepare timestamp
	if _, err = r.Read(c.PrepareTS[:]); err != nil {
		return
	}
	n += types.TxnTsSize
	// participants
	num := uint32(0)
	if _, err = r.Read(types.EncodeUint32(&num)); err != nil {
		return
	}
	n += 4
	c.Participants = make([]uint64, num)
	for i := 0; i < int(num); i++ {
		id := uint64(0)
		if _, err = r.Read(types.EncodeUint64(&id)); err != nil {
			break
		} else {
			c.Participants = append(c.Participants, id)
			n += 8
		}
	}
	MemoVersion := model.MemoTreeVersion1
	if ver >= IOET_WALTxnEntry_V2 {
		MemoVersion = model.MemoTreeVersion2
	}
	if ver >= IOET_WALTxnEntry_V3 {
		MemoVersion = model.MemoTreeVersion3
	}

	if sn, err = c.Memo.ReadFromWithVersion(r, MemoVersion); err != nil {
		return
	}
	n += sn
	return
}

func (c *TxnCmd) MarshalBinary() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (c *TxnCmd) UnmarshalBinaryWithVersion(buf []byte, ver uint16) (err error) {
	c.ComposedCmd = NewComposedCmd(0)
	composeedCmdBufLength := types.DecodeUint32(buf[:4])
	n := 4
	cmd, err := BuildCommandFrom(buf[n : n+int(composeedCmdBufLength)])
	if err != nil {
		return
	}
	c.ComposedCmd = cmd.(*ComposedCmd)
	n += int(composeedCmdBufLength)
	bbuf := bytes.NewBuffer(buf[n:])
	_, err = c.ReadFromWithVersion(bbuf, ver)
	return err
}
func (c *TxnCmd) GetType() uint16 { return IOET_WALTxnEntry }
func (c *TxnCmd) Desc() string {
	return fmt.Sprintf("Tid=%X,Is2PC=%v,%s", c.ID, c.Is2PC(), c.ComposedCmd.Desc())
}
func (c *TxnCmd) String() string {
	return fmt.Sprintf("Tid=%X,Is2PC=%v,%s", c.ID, c.Is2PC(), c.ComposedCmd.String())
}
func (c *TxnCmd) VerboseString() string {
	return fmt.Sprintf("Tid=%X,Is2PC=%v,%s", c.ID, c.Is2PC(), c.ComposedCmd.VerboseString())
}
func (c *TxnCmd) Close() {
	c.ComposedCmd.Close()
}

func (cc *ComposedCmd) ApplyCommit() {
	for _, c := range cc.Cmds {
		c.ApplyCommit()
	}
}
func (cc *ComposedCmd) ApplyRollback() {
	for _, c := range cc.Cmds {
		c.ApplyRollback()
	}
}
func (cc *ComposedCmd) SetReplayTxn(txn txnif.AsyncTxn) {
	for _, c := range cc.Cmds {
		c.SetReplayTxn(txn)
	}
}
func (cc *ComposedCmd) Close() {
	for _, cmd := range cc.Cmds {
		cmd.Close()
	}
}
func (cc *ComposedCmd) GetType() uint16 {
	return IOET_WALTxnCommand_Composed
}

func (cc *ComposedCmd) MarshalBinary() (buf []byte, err error) {
	// cmdBuf only buffers the cmds.
	var cmdBuf bytes.Buffer

	if cc.LastPos < 0 {
		cc.LastPos = 0
	}
	prevLastPos := cc.LastPos
	if _, err = cc.WriteTo(&cmdBuf); err != nil {
		return
	}

	// headBuf buffers the header data.
	var headerBuf bytes.Buffer
	t := cc.GetType()
	if _, err = headerBuf.Write(types.EncodeUint16(&t)); err != nil {
		return
	}
	ver := IOET_WALTxnCommand_Composed_CurrVer
	if _, err = headerBuf.Write(types.EncodeUint16(&ver)); err != nil {
		return
	}
	var length uint32
	if cc.LastPos == 0 {
		length = uint32(len(cc.Cmds) - prevLastPos)
	} else {
		length = uint32(cc.LastPos - prevLastPos)
	}
	if _, err = headerBuf.Write(types.EncodeUint32(&length)); err != nil {
		return
	}

	buf = append(headerBuf.Bytes(), cmdBuf.Bytes()...)
	return
}

func (cc *ComposedCmd) UnmarshalBinary(buf []byte) (err error) {
	var n int64
	length := uint32(0)
	length = types.DecodeUint32(buf[n : n+4])
	n += 4
	for i := 0; i < int(length); i++ {
		bufLength := types.DecodeUint32(buf[n : n+4])
		n += 4
		subCmd, err := BuildCommandFrom(buf[n : n+int64(bufLength)])
		if err != nil {
			return err
		}
		n += int64(bufLength)
		cc.AddCmd(subCmd.(txnif.TxnCmd))
	}
	return err
}

func (cc *ComposedCmd) WriteTo(w io.Writer) (n int64, err error) {
	for idx, cmd := range cc.Cmds[cc.LastPos:] {
		var buf []byte
		var sn int64
		if buf, err = cmd.MarshalBinary(); err != nil {
			return
		}
		// If the size cmd buffer is bigger than the limit, stop push items into
		// the buffer and update cc.LastPos.
		// We do the check before write the cmd to writer, there must be cmds
		// that have not been pushed into the buffer. So do NOT need to set
		// cc.LastPos to zero.
		if n+int64(len(buf))+4 >= cc.CmdBufLimit {
			cc.LastPos += idx
			return
		}
		if sn, err = objectio.WriteBytes(buf, w); err != nil {
			return
		}
		n += sn
	}
	cc.LastPos = 0
	return
}

func (cc *ComposedCmd) AddCmd(cmd txnif.TxnCmd) {
	cc.Cmds = append(cc.Cmds, cmd)
}

func (cc *ComposedCmd) MoreCmds() bool {
	return cc.LastPos != 0
}

func (cc *ComposedCmd) ToString(prefix string) string {
	s := fmt.Sprintf("%sComposedCmd: Cnt=%d", prefix, len(cc.Cmds))
	for _, cmd := range cc.Cmds {
		s = fmt.Sprintf("%s\n%s\t%s", s, prefix, cmd.String())
	}
	return s
}

func (cc *ComposedCmd) ToDesc(prefix string) string {
	s := fmt.Sprintf("%sComposedCmd: Cnt=%d", prefix, len(cc.Cmds))
	for _, cmd := range cc.Cmds {
		s = fmt.Sprintf("%s\n%s\t%s", s, prefix, cmd.Desc())
	}
	return s
}
func (cc *ComposedCmd) ToVerboseString(prefix string) string {
	s := fmt.Sprintf("%sComposedCmd: Cnt=%d", prefix, len(cc.Cmds))
	for _, cmd := range cc.Cmds {
		s = fmt.Sprintf("%s\n%s\t%s", s, prefix, cmd.VerboseString())
	}
	return s
}

func (cc *ComposedCmd) VerboseString() string {
	return cc.ToVerboseString("")
}
func (cc *ComposedCmd) String() string {
	return cc.ToString("")
}
func (cc *ComposedCmd) Desc() string {
	return cc.ToDesc("")
}

func BuildCommandFrom(buf []byte) (cmd any, err error) {
	head := objectio.DecodeIOEntryHeader(buf)
	codec := objectio.GetIOEntryCodec(*head)
	return codec.Decode(buf[4:])
}
