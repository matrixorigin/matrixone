// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"io"
	"net"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	// the default message buffer size, 8K.
	defaultBufLen = 8192
	// MySQL header length is 4 bytes, with 3 bytes data length
	// and 1 byte sequence number.
	mysqlHeadLen = 4
	// MySQL query cmd is 1 byte.
	cmdLen = 1
	// The header and cmd must be received first.
	preRecvLen = mysqlHeadLen + cmdLen
)

// txnTag indicates the txn state.
type txnTag uint8

const (
	// txnBegin means the statement is 'begin'.
	txnBegin txnTag = 0
	// txnEnd means the statement is 'commit' or 'rollback'.
	txnEnd txnTag = 1
	// txnBegin means the statement is others.
	txnOther txnTag = 2
)

// MySQLCmd is the type indicate the cmd of statement.
type MySQLCmd byte

// cmdQuery is a query cmd.
const cmdQuery MySQLCmd = 0x03

// MySQLConn contains a buffer to save data which may be only part
// of a packet.
type MySQLConn struct {
	net.Conn
	*msgBuf
}

// newMySQLConn creates a new MySQLConn.
func newMySQLConn(name string, c net.Conn, sz int) *MySQLConn {
	return &MySQLConn{
		Conn:   c,
		msgBuf: newMsgBuf(c, sz, name),
	}
}

// msgBuf holds a buffer to save MySQL packets. It is mainly used to
// identify what kind the statement is, and to check whether it is safe
// to transfer a connection.
type msgBuf struct {
	src io.Reader
	// buf keeps message which is read from src. It can contain multiple messages.
	buf []byte
	// begin, end is the range that the data is available in the buf.
	begin, end int
	// for debug
	name string
}

// newMsgBuf creates a new message buffer.
func newMsgBuf(src io.Reader, bufLen int, name string) *msgBuf {
	if bufLen < mysqlHeadLen {
		bufLen = defaultBufLen
	}
	return &msgBuf{
		src:  src,
		buf:  make([]byte, bufLen),
		name: name,
	}
}

// readAvail returns the position that is available to read.
func (b *msgBuf) readAvail() int {
	return b.end - b.begin
}

// writeAvail returns the position that is available to write.
func (b *msgBuf) writeAvail() int {
	return len(b.buf) - b.end
}

// preRecv tries to receive a MySQL packet from remote. It receives
// a packet header at least, and it blocks when there are nothing to
// receive.
func (b *msgBuf) preRecv() (int, txnTag, error) {
	// First we try to receive at least preRecvLen data and put it into
	// the buffer.
	if err := b.receiveAtLeast(preRecvLen); err != nil {
		return 0, 0, err
	}

	cmd := b.buf[b.begin+mysqlHeadLen]
	bodyLen := int(uint32(b.buf[b.begin]) | uint32(b.buf[b.begin+1])<<8 | uint32(b.buf[b.begin+2])<<16)

	// Max length is 3 bytes.
	if bodyLen < 1 || bodyLen >= 1<<24-1 {
		return 0, txnOther, moerr.NewInternalErrorNoCtx("mysql protocol error: body length %d", bodyLen)
	}

	// Recognize the statement to mark the transaction status of the pipe.
	txnRet := txnOther
	if cmd == byte(cmdQuery) {
		if bodyLen == 7 && isStmtCommit(b.buf[b.begin+preRecvLen:b.end]) {
			txnRet = txnEnd
		} else if bodyLen == 9 && isStmtRollback(b.buf[b.begin+preRecvLen:b.end]) {
			txnRet = txnEnd
		} else if bodyLen == 6 && isStmtBegin(b.buf[b.begin+preRecvLen:b.end]) {
			txnRet = txnBegin
		}
	}

	// Data length does not count header length, so header length is added to it.
	return bodyLen + mysqlHeadLen, txnRet, nil
}

// sendTo sends the data in buffer to destination.
func (b *msgBuf) sendTo(dst io.Writer) (int, error) {
	l, _, err := b.preRecv()
	if err != nil {
		return 0, err
	}
	readPos := b.begin
	writePos := readPos + l
	dataLeft := 0
	if writePos > b.end {
		dataLeft = writePos - b.end
		writePos = b.end
	}
	b.begin = writePos

	// Write the data in buffer.
	n, err := dst.Write(b.buf[readPos:writePos])
	if err != nil {
		return n, err
	}
	if n < writePos-readPos {
		return n, io.ErrShortWrite
	}

	// The buffer does not hold all packet data, so continue to read the packet.
	if dataLeft > 0 {
		m, err := io.CopyN(dst, b.src, int64(dataLeft))
		n += int(m)
		if err != nil {
			return n, err
		}
		if int(m) < dataLeft {
			return n, io.ErrShortWrite
		}
	}
	return n, err
}

// receive receives a MySQL packet. This is used in test only.
func (b *msgBuf) receive() ([]byte, error) {
	// Receive header of the current packet.
	size, _, err := b.preRecv()
	if err != nil {
		return nil, err
	}

	if size <= len(b.buf) {
		if err := b.receiveAtLeast(size); err != nil {
			return nil, err
		}
		retBuf := b.buf[b.begin : b.begin+size]
		b.begin += size
		return retBuf, nil
	}

	// Packet cannot fit, so we will have to allocate new space.
	msg := make([]byte, size)
	// Copy bytes which have already been read.
	n := copy(msg, b.buf[b.begin:b.end])
	b.begin += n

	// Read more bytes.
	if _, err := io.ReadFull(b.src, msg[n:]); err != nil {
		return nil, err
	}

	return msg, nil
}

func (b *msgBuf) receiveAtLeast(n int) error {
	if n < 0 || n > len(b.buf) {
		return moerr.NewInternalErrorNoCtx("invalid receive bytes size %d", n)
	}
	// Buffer already has n bytes.
	if b.readAvail() >= n {
		return nil
	}
	minReadSize := n - b.readAvail()
	if b.writeAvail() < minReadSize {
		b.end = copy(b.buf, b.buf[b.begin:b.end])
		b.begin = 0
	}
	c, err := io.ReadAtLeast(b.src, b.buf[b.end:], minReadSize)
	b.end += c
	return err
}
