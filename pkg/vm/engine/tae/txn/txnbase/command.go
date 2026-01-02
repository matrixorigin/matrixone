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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

const (
	IOET_WALTxnEntry            uint16 = 3000
	IOET_WALTxnCommand_Composed uint16 = 3001
	IOET_WALTxnCommand_TxnState uint16 = 3002

	IOET_WALTxnEntry_V3            uint16 = 3
	IOET_WALTxnEntry_V4            uint16 = 4
	IOET_WALTxnEntry_V5            uint16 = 5
	IOET_WALTxnCommand_Composed_V1 uint16 = 1
	IOET_WALTxnCommand_TxnState_V1 uint16 = 1

	IOET_WALTxnEntry_CurrVer            = IOET_WALTxnEntry_V5
	IOET_WALTxnCommand_Composed_CurrVer = IOET_WALTxnCommand_Composed_V1
	IOET_WALTxnCommand_TxnState_CurrVer = IOET_WALTxnCommand_TxnState_V1

	// CmdBufReserved is reserved size of cmd buffer, mainly the size of TxnCtx.Memo.
	// ComposedCmd.CmdBufLimit is the max buffer size that could be sent out to log-service.
	// This value is normally the max RPC message size which is configured in config of DN.
	// The message contains mainly commands, but also other information whose size is CmdBufReserved.
	// TODO(volgariver6): this buf size is about the max size of TxnCt.Memo, we need to calculate
	// the exact size of it.
	CmdBufReserved = 1024 * 1024 * 10

	// MaxComposedCmdBufSize is the maximum capacity of marshalBuf in ComposedCmd.
	// Buffers exceeding this size will be discarded to prevent large objects from staying in memory.
	MaxComposedCmdBufSize = 1 << 20 // 1MB

	// MaxTxnCmdBufSize is the maximum capacity of marshalBuf in TxnCmd.
	// Buffers exceeding this size will be discarded to prevent large objects from staying in memory.
	MaxTxnCmdBufSize = 1 << 20 // 1MB
)

func init() {
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnEntry,
			Version: IOET_WALTxnEntry_V4,
		},
		nil,
		func(b []byte) (any, error) {
			txnEntry := NewEmptyTxnCmd()
			err := txnEntry.UnmarshalBinaryWithVersion(b, IOET_WALTxnEntry_V4)
			return txnEntry, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnEntry,
			Version: IOET_WALTxnEntry_V5,
		},
		nil,
		func(b []byte) (any, error) {
			txnEntry := NewEmptyTxnCmd()
			err := txnEntry.UnmarshalBinaryWithVersion(b, IOET_WALTxnEntry_V5)
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
			txnEntry := NewComposedCmd()
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
	isEnd bool
	Lsn   uint64

	// marshalBuf is a reusable buffer for MarshalBinary to avoid repeated allocations.
	// It will be discarded if capacity exceeds MaxTxnCmdBufSize to prevent memory leaks.
	marshalBuf []byte
}

type TxnStateCmd struct {
	ID       string
	State    txnif.TxnState
	CommitTs types.TS
}

func (c *TxnStateCmd) ApproxSize() int64 {
	var size int64
	size += 2 // type
	size += 2 // version
	size += 4 // ID length prefix
	size += int64(len(c.ID)) // ID string
	size += 4 // state (int32)
	size += types.TxnTsSize // CommitTs
	return size
}

type ComposedCmd struct {
	Cmds []txnif.TxnCmd

	// marshalBuf is a reusable buffer for MarshalBinary to avoid repeated allocations.
	// It will be discarded if capacity exceeds MaxComposedCmdBufSize to prevent memory leaks.
	marshalBuf []byte

	// CmdBufLimit indicates max cmd buffer size. We can only send out
	// the cmd buffer whose size is less than it.
	//CmdBufLimit int64

	// lastPos is the position in the Cmds list, before which the cmds have
	// been marshalled into buffer.
	//LastPos int
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

func NewComposedCmd() *ComposedCmd {
	return &ComposedCmd{
		Cmds: make([]txnif.TxnCmd, 0),
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

func NewTxnCmd() *TxnCmd {
	return &TxnCmd{
		ComposedCmd: NewComposedCmd(),
		TxnCtx:      NewEmptyTxnCtx(),
	}
}
func NewEmptyTxnCmd() *TxnCmd {
	return &TxnCmd{
		TxnCtx: NewEmptyTxnCtx(),
	}
}
func NewEndCmd() *TxnCmd {
	return &TxnCmd{
		isEnd: true,
	}
}
func (c *TxnCmd) IsEnd() bool { return c.isEnd }
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

func (c *TxnCmd) ApproxSize() int64 {
	var (
		size int64
	)

	size += 2 // type
	size += 2 // version
	size += c.ComposedCmd.ApproxSize()
	size += c.TxnCtx.ApproxSize()

	return size
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
	MemoVersion := model.MemoTreeVersion2
	if ver >= IOET_WALTxnEntry_V3 {
		MemoVersion = model.MemoTreeVersion3
	}
	if ver >= IOET_WALTxnEntry_V5 {
		MemoVersion = model.MemoTreeVersion4
	}

	if sn, err = c.Memo.ReadFromWithVersion(r, MemoVersion); err != nil {
		return
	}
	n += sn
	return
}

func (c *TxnCmd) MarshalBinary() (buf []byte, err error) {
	// Capacity limit: discard buffer if it exceeds MaxTxnCmdBufSize to prevent memory leaks.
	if cap(c.marshalBuf) > MaxTxnCmdBufSize {
		c.marshalBuf = nil
	}

	// Estimate size: use ApproxSize() for more accurate estimation
	estimatedSize := int(c.ApproxSize())
	if estimatedSize < 256 {
		estimatedSize = 256 // Minimum capacity
	}

	// Reuse or allocate buffer
	if cap(c.marshalBuf) < estimatedSize {
		c.marshalBuf = make([]byte, 0, estimatedSize)
	} else {
		c.marshalBuf = c.marshalBuf[:0] // Reset length, keep capacity
	}

	// Write header (type 2 + version 2)
	t := c.GetType()
	c.marshalBuf = append(c.marshalBuf, types.EncodeUint16(&t)...)
	v := IOET_WALTxnEntry_CurrVer
	c.marshalBuf = append(c.marshalBuf, types.EncodeUint16(&v)...)

	// Write ComposedCmd (prefixed with length)
	var composedBuf []byte
	if composedBuf, err = c.ComposedCmd.MarshalBinary(); err != nil {
		return nil, err
	}
	composedLen := uint32(len(composedBuf))
	c.marshalBuf = append(c.marshalBuf, types.EncodeUint32(&composedLen)...)
	c.marshalBuf = append(c.marshalBuf, composedBuf...)

	// Write ID (prefixed with length)
	idLen := uint32(len(c.ID))
	c.marshalBuf = append(c.marshalBuf, types.EncodeUint32(&idLen)...)
	c.marshalBuf = append(c.marshalBuf, c.ID...)

	// Write StartTS
	c.marshalBuf = append(c.marshalBuf, c.StartTS[:]...)

	// Write PrepareTS
	c.marshalBuf = append(c.marshalBuf, c.PrepareTS[:]...)

	// Write Participants (length + data)
	participantsLen := uint32(len(c.Participants))
	c.marshalBuf = append(c.marshalBuf, types.EncodeUint32(&participantsLen)...)
	for _, p := range c.Participants {
		c.marshalBuf = append(c.marshalBuf, types.EncodeUint64(&p)...)
	}

	// Write Memo
	// Pre-allocate buffer capacity using ApproxSize to reduce reallocations
	memoSize := int(c.Memo.ApproxSize())
	if memoSize < 256 {
		memoSize = 256 // Minimum capacity
	}
	var memoBuf bytes.Buffer
	memoBuf.Grow(memoSize) // Pre-grow to reduce reallocations
	if _, err = c.Memo.WriteTo(&memoBuf); err != nil {
		return nil, err
	}
	c.marshalBuf = append(c.marshalBuf, memoBuf.Bytes()...)

	return c.marshalBuf, nil
}
func (c *TxnCmd) UnmarshalBinaryWithVersion(buf []byte, ver uint16) (err error) {
	c.ComposedCmd = NewComposedCmd()
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
	// Clear marshalBuf to release memory when the object is closed
	c.marshalBuf = nil
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
	// Clear marshalBuf to release memory when the object is closed
	cc.marshalBuf = nil
}
func (cc *ComposedCmd) GetType() uint16 {
	return IOET_WALTxnCommand_Composed
}

func (cc *ComposedCmd) MarshalBinary() (buf []byte, err error) {
	// Capacity limit: discard buffer if it exceeds MaxComposedCmdBufSize to prevent memory leaks.
	if cap(cc.marshalBuf) > MaxComposedCmdBufSize {
		cc.marshalBuf = nil
	}

	// Estimate size: use ApproxSize() for more accurate estimation
	// This reduces the number of reallocations.
	estimatedSize := int(cc.ApproxSize())
	if estimatedSize < 256 {
		estimatedSize = 256 // Minimum capacity
	}

	// Reuse or allocate buffer
	if cap(cc.marshalBuf) < estimatedSize {
		cc.marshalBuf = make([]byte, 0, estimatedSize)
	} else {
		cc.marshalBuf = cc.marshalBuf[:0] // Reset length, keep capacity
	}

	// Write header (8 bytes: type 2 + version 2 + length 4)
	t := cc.GetType()
	cc.marshalBuf = append(cc.marshalBuf, types.EncodeUint16(&t)...)
	ver := IOET_WALTxnCommand_Composed_CurrVer
	cc.marshalBuf = append(cc.marshalBuf, types.EncodeUint16(&ver)...)
	length := uint32(len(cc.Cmds))
	cc.marshalBuf = append(cc.marshalBuf, types.EncodeUint32(&length)...)

	// Write cmds (each cmd is prefixed with its length, same as WriteTo does)
	for _, cmd := range cc.Cmds {
		var cmdBuf []byte
		if cmdBuf, err = cmd.MarshalBinary(); err != nil {
			return nil, err
		}
		// Write length prefix (4 bytes) followed by cmd data, matching WriteTo behavior
		cmdLen := uint32(len(cmdBuf))
		cc.marshalBuf = append(cc.marshalBuf, types.EncodeUint32(&cmdLen)...)
		cc.marshalBuf = append(cc.marshalBuf, cmdBuf...)
	}

	return cc.marshalBuf, nil
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

func (cc *ComposedCmd) ApproxSize() int64 {
	var (
		size int64
	)

	size += 2 // type
	size += 2 // version
	size += 4 // len of cmd

	for i := range cc.Cmds {
		size += cc.Cmds[i].ApproxSize()
	}

	return size

}

func (cc *ComposedCmd) WriteTo(w io.Writer) (n int64, err error) {
	for _, cmd := range cc.Cmds {
		var buf []byte
		var sn int64
		if buf, err = cmd.MarshalBinary(); err != nil {
			return
		}
		if sn, err = objectio.WriteBytes(buf, w); err != nil {
			return
		}
		n += sn
	}
	return
}

func (cc *ComposedCmd) AddCmd(cmd txnif.TxnCmd) {
	cc.Cmds = append(cc.Cmds, cmd)
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
