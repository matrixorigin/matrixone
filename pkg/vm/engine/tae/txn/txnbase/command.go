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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

	// MaxSharedBufSize is the maximum capacity of sharedBuf in TxnCmd for cross-transaction reuse.
	// Buffers exceeding this size will be discarded to prevent large objects from staying in memory.
	// Buffers below this size can be reused across transactions (similar to HashMap Iterator pattern).
	MaxSharedBufSize = 2 << 20 // 2MB

	// MaxPooledBufSize is the maximum capacity of buffers in the sync.Pool.
	// Buffers exceeding this size will not be returned to the pool to prevent memory leaks.
	MaxPooledBufSize = 2 << 20 // 2MB
)

// marshalBufPool is a sync.Pool for reusing bytes.Buffer instances across transactions.
// This eliminates buffer allocations in the entire serialization chain.
var marshalBufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

// GetMarshalBuffer returns a bytes.Buffer from the pool.
// The buffer should be returned to the pool using PutMarshalBuffer after use.
// This function is exported so that other packages (like txnimpl) can use the same pool.
func GetMarshalBuffer() *bytes.Buffer {
	return marshalBufPool.Get().(*bytes.Buffer)
}

// PutMarshalBuffer returns a bytes.Buffer to the pool if its capacity is below MaxPooledBufSize.
// Buffers exceeding MaxPooledBufSize are discarded to prevent memory leaks.
// This function is exported so that other packages (like txnimpl) can use the same pool.
func PutMarshalBuffer(buf *bytes.Buffer) {
	if buf.Cap() > MaxPooledBufSize {
		return // Discard large buffers
	}
	buf.Reset() // Clear content but keep capacity
	marshalBufPool.Put(buf)
}

// getMarshalBuffer is an internal alias for GetMarshalBuffer.
func getMarshalBuffer() *bytes.Buffer {
	return GetMarshalBuffer()
}

// putMarshalBuffer is an internal alias for PutMarshalBuffer.
func putMarshalBuffer(buf *bytes.Buffer) {
	PutMarshalBuffer(buf)
}

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
}

type TxnStateCmd struct {
	ID       string
	State    txnif.TxnState
	CommitTs types.TS
}

func (c *TxnStateCmd) ApproxSize() int64 {
	var size int64
	size += 2                // type
	size += 2                // version
	size += 4                // ID length prefix
	size += int64(len(c.ID)) // ID string
	size += 4                // state (int32)
	size += types.TxnTsSize  // CommitTs
	return size
}

type ComposedCmd struct {
	Cmds []txnif.TxnCmd

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

func (c *TxnStateCmd) MarshalBinaryWithBuffer(buf *bytes.Buffer) error {
	_, err := c.WriteTo(buf)
	return err
}

func (c *TxnStateCmd) MarshalBinary() (buf []byte, err error) {
	poolBuf := getMarshalBuffer()
	// Reset buffer to ensure it's empty (pool buffers are reset when returned, but be safe)
	poolBuf.Reset()

	err = c.MarshalBinaryWithBuffer(poolBuf)
	if err != nil {
		putMarshalBuffer(poolBuf) // Return buffer on error
		return nil, err
	}

	data := poolBuf.Bytes()
	// Optimization: if buffer capacity exceeds MaxPooledBufSize, it won't be returned to pool.
	// In this case, we can directly return the underlying array without copy.
	if poolBuf.Cap() > MaxPooledBufSize {
		// No need to call putMarshalBuffer as it will be discarded anyway
		return data, nil
	}

	// Small buffer will be returned to pool and Reset, so we must copy
	result := make([]byte, len(data))
	copy(result, data)
	putMarshalBuffer(poolBuf)
	return result, nil
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
	c.Participants = make([]uint64, 0, num)
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

// MarshalBinaryWithBuffer serializes TxnCmd directly to the provided bytes.Buffer,
// avoiding buffer copying and allocations in the serialization chain.
// This method manually implements the serialization logic to use WriteToFull for ComposedCmd,
// avoiding the buffer allocation in ComposedCmd.MarshalBinary().
func (c *TxnCmd) MarshalBinaryWithBuffer(buf *bytes.Buffer) error {
	// Estimate total size and pre-grow buffer to reduce reallocations
	estimatedSize := int(c.ApproxSize())
	if estimatedSize < 256 {
		estimatedSize = 256 // Minimum capacity
	}
	if buf.Cap() < estimatedSize {
		buf.Grow(estimatedSize - buf.Len())
	}

	// Write header (type 2 + version 2)
	t := c.GetType()
	if _, err := buf.Write(types.EncodeUint16(&t)); err != nil {
		return err
	}
	v := IOET_WALTxnEntry_CurrVer
	if _, err := buf.Write(types.EncodeUint16(&v)); err != nil {
		return err
	}

	// Write ComposedCmd length prefix (we'll update this after writing ComposedCmd)
	lengthPrefixPos := buf.Len()
	composedLenPlaceholder := uint32(0)
	if _, err := buf.Write(types.EncodeUint32(&composedLenPlaceholder)); err != nil {
		return err
	}

	// Write ComposedCmd using MarshalBinaryWithBuffer (avoids MarshalBinary allocation)
	if c.ComposedCmd == nil {
		c.ComposedCmd = NewComposedCmd()
	}
	composedDataStart := buf.Len()
	if err := c.ComposedCmd.MarshalBinaryWithBuffer(buf); err != nil {
		return err
	}
	composedDataSize := buf.Len() - composedDataStart

	// Update ComposedCmd length prefix
	composedLen := uint32(composedDataSize)
	composedLenBytes := types.EncodeUint32(&composedLen)
	bufBytes := buf.Bytes()
	copy(bufBytes[lengthPrefixPos:lengthPrefixPos+4], composedLenBytes)

	// Write ID (prefixed with length)
	if _, err := objectio.WriteString(c.ID, buf); err != nil {
		return err
	}

	// Write StartTS
	if _, err := buf.Write(c.StartTS[:]); err != nil {
		return err
	}

	// Write PrepareTS
	if _, err := buf.Write(c.PrepareTS[:]); err != nil {
		return err
	}

	// Write Participants (length + data)
	participantsLen := uint32(len(c.Participants))
	if _, err := buf.Write(types.EncodeUint32(&participantsLen)); err != nil {
		return err
	}
	for _, p := range c.Participants {
		if _, err := buf.Write(types.EncodeUint64(&p)); err != nil {
			return err
		}
	}

	// Write Memo directly to buffer
	if _, err := c.Memo.WriteTo(buf); err != nil {
		return err
	}

	return nil
}

func (c *TxnCmd) MarshalBinary() (buf []byte, err error) {
	// Get buffer from pool (reused across transactions)
	poolBuf := getMarshalBuffer()
	// Reset buffer to ensure it's empty (pool buffers are reset when returned, but be safe)
	poolBuf.Reset()

	// Use MarshalBinaryWithBuffer to serialize directly to pooled buffer
	err = c.MarshalBinaryWithBuffer(poolBuf)
	if err != nil {
		putMarshalBuffer(poolBuf) // Return buffer on error
		return nil, err
	}

	data := poolBuf.Bytes()

	// Optimization: if buffer capacity exceeds MaxPooledBufSize, it won't be returned to pool.
	// In this case, we can directly return the underlying array without copy because:
	// 1. buffer.Bytes() returns a slice pointing to the buffer's underlying array
	// 2. The returned slice will keep the underlying array alive (prevent GC)
	// 3. The buffer object itself will be GC'd, but the underlying array won't be GC'd
	//    as long as the returned slice is referenced
	if poolBuf.Cap() > MaxPooledBufSize {
		// Large buffer won't be returned to pool, so we can return the underlying array directly
		// The returned slice will keep the underlying array alive
		// No need to call putMarshalBuffer as it will be discarded anyway
		return data, nil
	}

	// Small buffer will be returned to pool and Reset, so we must copy
	result := make([]byte, len(data))
	copy(result, data)
	putMarshalBuffer(poolBuf)
	return result, nil
}
func (c *TxnCmd) UnmarshalBinaryWithVersion(buf []byte, ver uint16) (err error) {
	c.ComposedCmd = NewComposedCmd()
	// Read ComposedCmd length (4 bytes)
	if len(buf) < 4 {
		return moerr.NewInternalErrorNoCtxf("buffer too short for ComposedCmd length")
	}
	composeedCmdBufLength := types.DecodeUint32(buf[0:4])
	n := 4
	// Read ComposedCmd data
	if len(buf) < n+int(composeedCmdBufLength) {
		return moerr.NewInternalErrorNoCtxf("buffer too short for ComposedCmd data: need %d, have %d", n+int(composeedCmdBufLength), len(buf))
	}
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

// MarshalBinaryWithBuffer serializes ComposedCmd directly to the provided bytes.Buffer,
// avoiding buffer copying and allocations. Sub-commands also use MarshalBinaryWithBuffer
// if available, ensuring zero allocations in the entire serialization chain.
func (cc *ComposedCmd) MarshalBinaryWithBuffer(buf *bytes.Buffer) error {
	// Estimate total size and pre-grow buffer to reduce reallocations
	estimatedSize := int(cc.ApproxSize())
	if estimatedSize < 256 {
		estimatedSize = 256 // Minimum capacity
	}
	if buf.Cap() < estimatedSize {
		buf.Grow(estimatedSize - buf.Len())
	}

	// Write header (8 bytes: type 2 + version 2 + length 4)
	t := cc.GetType()
	if _, err := buf.Write(types.EncodeUint16(&t)); err != nil {
		return err
	}
	ver := IOET_WALTxnCommand_Composed_CurrVer
	if _, err := buf.Write(types.EncodeUint16(&ver)); err != nil {
		return err
	}
	length := uint32(len(cc.Cmds))
	if _, err := buf.Write(types.EncodeUint32(&length)); err != nil {
		return err
	}

	// Pre-expand buffer before the loop to avoid growSlice allocations
	// Calculate total size needed for all commands (length prefix + cmd data)
	totalCmdsSize := 0
	for _, cmd := range cc.Cmds {
		cmdSize := int(cmd.ApproxSize())
		if cmdSize < 0 {
			cmdSize = 0
		}
		totalCmdsSize += 4 + cmdSize // length prefix (4 bytes) + cmd size
	}

	// Ensure capacity is sufficient for all commands
	currentLen := buf.Len()
	requiredCap := currentLen + totalCmdsSize
	if buf.Cap() < requiredCap {
		buf.Grow(requiredCap - buf.Len())
	}

	// Write cmds (each cmd is prefixed with its length, same as WriteTo does)
	// All commands must implement MarshalBinaryWithBuffer to avoid allocations
	for _, cmd := range cc.Cmds {
		// Check if cmd implements MarshalBinaryWithBuffer interface
		type marshalBinaryWithBuffer interface {
			MarshalBinaryWithBuffer(buf *bytes.Buffer) error
		}
		cmdWithBuf, ok := cmd.(marshalBinaryWithBuffer)
		if !ok {
			// All commands must implement MarshalBinaryWithBuffer
			panic(fmt.Sprintf("cmd %T does not implement MarshalBinaryWithBuffer", cmd))
		}
		// Use MarshalBinaryWithBuffer to write directly to shared buffer
		// Write placeholder for length prefix
		lengthPrefixPos := buf.Len()
		cmdLenPlaceholder := uint32(0)
		if _, err := buf.Write(types.EncodeUint32(&cmdLenPlaceholder)); err != nil {
			return err
		}
		// Write command data
		cmdDataStart := buf.Len()
		if err := cmdWithBuf.MarshalBinaryWithBuffer(buf); err != nil {
			return err
		}
		cmdDataSize := buf.Len() - cmdDataStart
		// Update length prefix
		cmdLen := uint32(cmdDataSize)
		cmdLenBytes := types.EncodeUint32(&cmdLen)
		bufBytes := buf.Bytes()
		copy(bufBytes[lengthPrefixPos:lengthPrefixPos+4], cmdLenBytes)
	}

	return nil
}

func (cc *ComposedCmd) MarshalBinary() (buf []byte, err error) {
	// Get buffer from pool
	poolBuf := getMarshalBuffer()
	// Reset buffer to ensure it's empty (pool buffers are reset when returned, but be safe)
	poolBuf.Reset()

	// Use MarshalBinaryWithBuffer to serialize directly to pooled buffer
	err = cc.MarshalBinaryWithBuffer(poolBuf)
	if err != nil {
		putMarshalBuffer(poolBuf) // Return buffer on error
		return nil, err
	}

	data := poolBuf.Bytes()

	// Optimization: if buffer capacity exceeds MaxPooledBufSize, it won't be returned to pool.
	// In this case, we can directly return the underlying array without copy because
	// the returned slice will keep the underlying array alive (prevent GC).
	if poolBuf.Cap() > MaxPooledBufSize {
		// No need to call putMarshalBuffer as it will be discarded anyway
		return data, nil
	}

	// Small buffer will be returned to pool and Reset, so we must copy
	result := make([]byte, len(data))
	copy(result, data)
	putMarshalBuffer(poolBuf)
	return result, nil
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
	// Define interface for MarshalBinaryWithBuffer to avoid allocations
	type marshalBinaryWithBuffer interface {
		MarshalBinaryWithBuffer(buf *bytes.Buffer) error
	}

	for _, cmd := range cc.Cmds {
		var sn int64
		// Optimization: if writer is *bytes.Buffer, we can write placeholder length first,
		// then write command data, then update the length prefix to avoid temporary buffer
		if sharedBuf, ok := w.(*bytes.Buffer); ok {
			// Write placeholder length (will be updated later)
			lengthPrefixPos := sharedBuf.Len()
			cmdLenPlaceholder := uint32(0)
			if _, err = sharedBuf.Write(types.EncodeUint32(&cmdLenPlaceholder)); err != nil {
				return
			}
			// Write command data
			cmdDataStart := sharedBuf.Len()

			// All commands must implement MarshalBinaryWithBuffer
			cmdWithBuf, ok := cmd.(marshalBinaryWithBuffer)
			if !ok {
				panic(fmt.Sprintf("cmd %T does not implement MarshalBinaryWithBuffer", cmd))
			}
			if err = cmdWithBuf.MarshalBinaryWithBuffer(sharedBuf); err != nil {
				return
			}
			sn = int64(sharedBuf.Len() - cmdDataStart)

			cmdDataSize := sharedBuf.Len() - cmdDataStart
			// Update length prefix
			cmdLen := uint32(cmdDataSize)
			cmdLenBytes := types.EncodeUint32(&cmdLen)
			bufBytes := sharedBuf.Bytes()
			copy(bufBytes[lengthPrefixPos:lengthPrefixPos+4], cmdLenBytes)
			n += sn + 4 // +4 for length prefix
		} else {
			// For non-buffer writers, use MarshalBinary (will use sync.Pool internally)
			var buf []byte
			if buf, err = cmd.MarshalBinary(); err != nil {
				return
			}
			if sn, err = objectio.WriteBytes(buf, w); err != nil {
				return
			}
			n += sn
		}
	}
	return
}

// WriteToFull writes the complete ComposedCmd serialization (including header) to the writer.
// This avoids the need to call MarshalBinary() which would allocate its own buffer.
// Header format: type (2) + version (2) + command count (4) + commands data.
func (cc *ComposedCmd) WriteToFull(w io.Writer) (n int64, err error) {
	// Write header: type (2) + version (2) + command count (4)
	t := cc.GetType()
	if _, err = w.Write(types.EncodeUint16(&t)); err != nil {
		return
	}
	n += 2
	ver := IOET_WALTxnCommand_Composed_CurrVer
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return
	}
	n += 2
	cmdCount := uint32(len(cc.Cmds))
	if _, err = w.Write(types.EncodeUint32(&cmdCount)); err != nil {
		return
	}
	n += 4

	// Write commands using WriteTo (which writes length prefix + data for each command)
	var sn int64
	if sn, err = cc.WriteTo(w); err != nil {
		return
	}
	n += sn
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
