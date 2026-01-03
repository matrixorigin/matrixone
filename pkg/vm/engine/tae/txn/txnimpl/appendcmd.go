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

package txnimpl

import (
	"bytes"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

const (
	IOET_WALTxnCommand_Append uint16 = 3008

	ioet_WALTxnCommand_Append_V3 uint16 = 3

	IOET_WALTxnCommand_Append_CurrVer = ioet_WALTxnCommand_Append_V3

	// MaxAppendCmdBufSize is the maximum capacity of marshalBuf in AppendCmd.
	// Buffers exceeding this size will be discarded to prevent large objects from staying in memory.
	MaxAppendCmdBufSize = 1 << 20 // 1MB
)

func init() {
	objectio.RegisterIOEnrtyCodec(objectio.IOEntryHeader{
		Type:    IOET_WALTxnCommand_Append,
		Version: ioet_WALTxnCommand_Append_V3,
	}, nil,
		func(b []byte) (any, error) {
			cmd := NewEmptyAppendCmd()
			err := cmd.UnmarshalBinary(b)
			return cmd, err
		})
}

type AppendCmd struct {
	*txnbase.BaseCustomizedCmd
	Data        *containers.Batch
	Infos       []*appendInfo
	Ts          types.TS
	Node        *anode
	IsTombstone bool
}

func NewEmptyAppendCmd() *AppendCmd {
	cmd := &AppendCmd{}
	cmd.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(0, cmd)
	return cmd
}

func NewAppendCmd(id uint32, node *anode, data *containers.Batch, isTombstone bool) *AppendCmd {
	impl := &AppendCmd{
		Node:        node,
		Infos:       node.GetAppends(),
		Data:        data,
		IsTombstone: isTombstone,
	}
	impl.BaseCustomizedCmd = txnbase.NewBaseCustomizedCmd(id, impl)
	return impl
}
func (c *AppendCmd) Desc() string {
	s := fmt.Sprintf("CmdName=InsertNode;ID=%d;TS=%v;Dests=[", c.ID, c.Ts.ToString())
	for _, info := range c.Infos {
		s = fmt.Sprintf("%s %s", s, info.Desc())
	}
	s = fmt.Sprintf("%s;R]ows=%d", s, c.Data.Length())
	if c.Data.HasDelete() {
		s = fmt.Sprintf("%s;DelCnt=%d", s, c.Data.DeleteCnt())
	}
	return s
}
func (c *AppendCmd) String() string {
	s := fmt.Sprintf("CmdName=InsertNode;ID=%d;TS=%v;Dests=[", c.ID, c.Ts.ToString())
	for _, info := range c.Infos {
		s = fmt.Sprintf("%s%s", s, info.String())
	}
	s = fmt.Sprintf("%s];Rows=%d", s, c.Data.Length())
	if c.Data.HasDelete() {
		s = fmt.Sprintf("%s;DelCnt=%d", s, c.Data.DeleteCnt())
	}
	return s
}
func (c *AppendCmd) VerboseString() string {
	s := fmt.Sprintf("CmdName=InsertNode;ID=%d;TS=%d;Dests=", c.ID, c.Ts)
	for _, info := range c.Infos {
		s = fmt.Sprintf("%s%s", s, info.String())
	}
	s = fmt.Sprintf("%s];Rows=%d", s, c.Data.Length())
	if c.Data.HasDelete() {
		s = fmt.Sprintf("%s;DelCnt=%d", s, c.Data.DeleteCnt())
	}
	return s
}
func (c *AppendCmd) Close() {
	c.Data.Close()
	c.Data = nil
}
func (c *AppendCmd) GetType() uint16 { return IOET_WALTxnCommand_Append }

func (c *AppendCmd) ApproxSize() int64 {
	size := int64(2 + 2 + 4 + 4 + types.TxnTsSize + 1) // type, version, id, len, ts, isTombstone
	size += int64(len(c.Infos)) * AppendInfoSize
	size += int64(c.Data.ApproxSize())
	return size
}

func (c *AppendCmd) WriteTo(w io.Writer) (n int64, err error) {
	t := c.GetType()
	if _, err = w.Write(types.EncodeUint16(&t)); err != nil {
		return
	}
	ver := IOET_WALTxnCommand_Append_CurrVer
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeUint32(&c.ID)); err != nil {
		return
	}
	length := uint32(len(c.Infos))
	if _, err = w.Write(types.EncodeUint32(&length)); err != nil {
		return
	}
	var sn int64
	n = 10
	for _, info := range c.Infos {
		if sn, err = info.WriteTo(w); err != nil {
			return
		}
		n += sn
	}
	sn, err = c.Data.WriteTo(w)
	n += sn
	if err != nil {
		return n, err
	}
	var ts types.TS
	if c.Node != nil {
		ts = c.Node.GetTxn().GetPrepareTS()
	} else {
		ts = c.Ts // Fallback to c.Ts if Node is nil (e.g., in tests)
	}
	if _, err = w.Write(ts[:]); err != nil {
		return
	}
	n += 16
	if _, err = w.Write(types.EncodeBool(&c.IsTombstone)); err != nil {
		return
	}
	n += 1
	return
}

func (c *AppendCmd) ReadFrom(r io.Reader) (n int64, err error) {
	if _, err = r.Read(types.EncodeUint32(&c.ID)); err != nil {
		return
	}
	length := uint32(0)
	if _, err = r.Read(types.EncodeUint32(&length)); err != nil {
		return
	}
	var sn int64
	n = 8
	c.Infos = make([]*appendInfo, length)
	for i := 0; i < int(length); i++ {
		c.Infos[i] = &appendInfo{}
		if sn, err = c.Infos[i].ReadFrom(r); err != nil {
			return
		}
		n += sn
	}
	c.Data = containers.NewBatch()
	c.Data.ReadFrom(r)
	n += sn
	if err != nil {
		return n, err
	}
	if _, err = r.Read(c.Ts[:]); err != nil {
		return
	}
	n += 16
	if _, err = r.Read(
		types.EncodeBool(&c.IsTombstone),
	); err != nil {
		return
	}
	n += 1
	return
}

// bytesWriter is a simple writer that appends to a byte slice.
type bytesWriter struct {
	buf []byte
}

func (w *bytesWriter) Write(p []byte) (n int, err error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// MarshalBinaryWithBuffer serializes AppendCmd directly to the provided bytes.Buffer,
// avoiding buffer copying and allocations.
func (c *AppendCmd) MarshalBinaryWithBuffer(buf *bytes.Buffer) ([]byte, error) {
	// Estimate total size and pre-grow buffer to reduce reallocations
	estimatedSize := int(c.ApproxSize())
	if estimatedSize < 256 {
		estimatedSize = 256 // Minimum capacity
	}
	if buf.Cap() < estimatedSize {
		buf.Grow(estimatedSize - buf.Len())
	}

	// Use WriteTo to write directly to the shared buffer
	if _, err := c.WriteTo(buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (c *AppendCmd) MarshalBinary() (buf []byte, err error) {
	// Get buffer from pool (reused across transactions)
	poolBuf := txnbase.GetMarshalBuffer()

	// Use MarshalBinaryWithBuffer to serialize directly to pooled buffer
	data, err := c.MarshalBinaryWithBuffer(poolBuf)
	if err != nil {
		txnbase.PutMarshalBuffer(poolBuf) // Return buffer on error
		return nil, err
	}

	// Optimization: if buffer capacity exceeds MaxPooledBufSize, it won't be returned to pool.
	// In this case, we can directly return the underlying array without copy because
	// the returned slice will keep the underlying array alive (prevent GC).
	if poolBuf.Cap() > txnbase.MaxPooledBufSize {
		txnbase.PutMarshalBuffer(poolBuf) // Will discard, but safe to call
		return data, nil
	}

	// Small buffer will be returned to pool and Reset, so we must copy
	result := make([]byte, len(data))
	copy(result, data)
	txnbase.PutMarshalBuffer(poolBuf)
	return result, nil
}

func (c *AppendCmd) UnmarshalBinary(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := c.ReadFrom(bbuf)
	return err
}

func (c *AppendCmd) ApplyCommit()                  {}
func (c *AppendCmd) ApplyRollback()                {}
func (c *AppendCmd) SetReplayTxn(_ txnif.AsyncTxn) {}
