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
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
)

const (
	// the default message buffer size, 8K.
	defaultBufLen = 8192
	// defaultExtraBufLen is the default extra buffer size, 2K.
	defaultExtraBufLen = 2048
	// MySQL header length is 4 bytes, with 3 bytes data length
	// and 1 byte sequence number.
	mysqlHeadLen = 4
	// MySQL query cmd is 1 byte.
	cmdLen = 1
	// The header and cmd must be received first.
	preRecvLen = mysqlHeadLen + cmdLen
)

// MySQLCmd is the type indicate the cmd of statement.
type MySQLCmd byte

// cmdQuery is a query cmd.
const (
	cmdQuery  MySQLCmd = 0x03
	cmdInitDB MySQLCmd = 0x02
	// For stmt prepare and execute cmd from JDBC.
	cmdStmtPrepare MySQLCmd = 0x16
	cmdStmtClose   MySQLCmd = 0x19
)

// MySQLConn contains a buffer to save data which may be only part
// of a packet.
type MySQLConn struct {
	net.Conn
	*msgBuf
}

// newMySQLConn creates a new MySQLConn. reqC and respC are used for client
// connection to handle events from client.
func newMySQLConn(
	name string, c net.Conn, sz int, reqC chan IEvent, respC chan []byte,
) *MySQLConn {
	return &MySQLConn{
		Conn:   c,
		msgBuf: newMsgBuf(name, c, sz, reqC, respC),
	}
}

// msgBuf holds a buffer to save MySQL packets. It is mainly used to
// identify what kind the statement is, and to check whether it is safe
// to transfer a connection.
type msgBuf struct {
	// for debug
	name string
	src  io.Reader

	peer *msgBuf
	// buf keeps message which is read from src. It can contain multiple messages.
	// The default available part of buffer is only [0:defaultBufLen]. The rest part
	// [defaultBufLen:] is used to save data to handle events when the first part is full.
	//
	// TODO(volgariver6): As a result, only the event statement whose length is less
	// than defaultExtraBufLen is supported.
	buf []byte
	// availLen is the available length of the buffer.
	availLen int
	extraLen int
	// begin, end is the range that the data is available in the buf.
	begin, end int
	// writeMu controls the mutex to lock when write a MySQL packet with net.Write().
	writeMu sync.Mutex
	// reqC is the channel of event request.
	reqC chan IEvent
	// respC is the channel of event response.
	respC chan []byte
	// inTxn is the session txn state which is updated by the OK and EOF packet from server.
	// It is used to check if we should start a connection transfer.
	mu struct {
		sync.Mutex
		inTxn bool
		// prepared is true means that client just send a prepared cmd and not
		// execute it yet. After it is executed, set to false.
		prepared bool
	}
}

// newMsgBuf creates a new message buffer.
func newMsgBuf(
	name string, src io.Reader, bufLen int, reqC chan IEvent, respC chan []byte,
) *msgBuf {
	var availLen, extraLen int
	if bufLen < mysqlHeadLen {
		availLen = defaultBufLen
		extraLen = defaultExtraBufLen
		bufLen = availLen + extraLen
	} else {
		availLen = bufLen
	}
	return &msgBuf{
		src:      src,
		buf:      make([]byte, bufLen),
		availLen: availLen,
		extraLen: extraLen,
		name:     name,
		reqC:     reqC,
		respC:    respC,
	}
}

func setPeer(p1, p2 *msgBuf) {
	p1.peer = p2
	p2.peer = p1
}

// readAvail returns the position that is available to read.
func (b *msgBuf) readAvail() int {
	return b.end - b.begin
}

// writeAvail returns the position that is available to write.
func (b *msgBuf) writeAvail() int {
	return b.availLen - b.end
}

// preRecv tries to receive a MySQL packet from remote. It receives
// a packet header at least, and it blocks when there are nothing to
// receive.
func (b *msgBuf) preRecv() (int, error) {
	// First we try to receive at least preRecvLen data and put it into
	// the buffer.
	if err := b.receiveAtLeast(mysqlHeadLen); err != nil {
		return 0, err
	}

	bodyLen := int(uint32(b.buf[b.begin]) | uint32(b.buf[b.begin+1])<<8 | uint32(b.buf[b.begin+2])<<16)
	if bodyLen == 0 {
		return mysqlHeadLen, nil
	}

	// Max length is 3 bytes. 26MB-1 is the legal max length of a MySQL packet.
	// Reference To : https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_packets.html
	if bodyLen < 1 || bodyLen > 1<<24-1 {
		return 0, moerr.NewInternalErrorNoCtx("mysql protocol error: body length %d", bodyLen)
	}

	// Data length does not count header length, so header length is added to it.
	return bodyLen + mysqlHeadLen, nil
}

// consumeMsg consumes the MySQL packet in the buffer, handles it by event
// mechanism. Returns true if the command is handled, means it does not need
// to be sent through tunnel anymore; false otherwise.
func (b *msgBuf) consumeMsg(msg []byte) bool {
	if b.reqC == nil {
		return false
	}

	// For the client->server pipe, we catch some statements to do some more actions.
	if b.name == connClientName {
		e, r := makeEvent(msg, b)
		if e == nil {
			return false
		}
		sendReq(e, b.reqC)
		// We cannot write to b.src directly here. The response has
		// to go to the server conn buf, and lock writeMu then
		// write to client.

		return r
	}

	// For the server->client pipe, we should the transaction status from the
	// OK and EOF packet, which is used in connection transfer. If the session
	// is in a transaction, a transfer should not start.
	if b.name == connServerName {
		if isOKPacket(msg) {
			b.handleOKPacket(msg)
		} else if isEOFPacket(msg) {
			b.handleEOFPacket(msg)
		}
	}
	return false
}

// handleOKPacket handles the OK packet from server to update the txn state.
func (b *msgBuf) handleOKPacket(msg []byte) {
	var mp *frontend.MysqlProtocolImpl
	pos := 5
	_, pos, ok := mp.ReadIntLenEnc(msg, pos)
	if !ok {
		return
	}
	_, pos, ok = mp.ReadIntLenEnc(msg, pos)
	if !ok {
		return
	}
	// FIXME: the result set packet may pretend as OK packet if the first field is null.
	// The example is the line 16 in file test/distributed/resources/load_data/char_varchar_1.csv.
	// After fix, remove the following 3 lines.
	if len(msg[pos:]) < 2 {
		return
	}
	status := binary.LittleEndian.Uint16(msg[pos:])
	b.setTxnStatus(status)
}

// handleEOFPacket handles the EOF packet from server to update the txn state.
func (b *msgBuf) handleEOFPacket(msg []byte) {
	status := binary.LittleEndian.Uint16(msg[7:])
	b.setTxnStatus(status)
}

// setTxnStatus sets the txn state according to the incoming status.
func (b *msgBuf) setTxnStatus(status uint16) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.inTxn = status&frontend.SERVER_STATUS_IN_TRANS != 0

	if b.peer != nil {
		b.peer.mu.Lock()
		defer b.peer.mu.Unlock()
		b.peer.mu.inTxn = b.mu.inTxn
	}
}

// isInTxn returns if the session is in a transaction.
func (b *msgBuf) isInTxn() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mu.inTxn
}

// setPrepared sets the prepared state.
func (b *msgBuf) setPrepared(p bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.prepared = p
}

// isInTxn returns if the session is just prepared and not executed yet.
func (b *msgBuf) isPrepared() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mu.prepared
}

// sendTo sends the data in buffer to destination.
func (b *msgBuf) sendTo(dst io.Writer) (bool, error) {
	l, err := b.preRecv()
	if err != nil {
		return false, err
	}
	readPos := b.begin
	writePos := readPos + l
	dataLeft := 0
	if writePos > b.end {
		dataLeft = writePos - b.end
		writePos = b.end
	}
	b.begin = writePos
	if writePos-readPos < mysqlHeadLen {
		panic(fmt.Sprintf("%d bytes have to be read", mysqlHeadLen))
	}

	// Try to consume the message synchronously.
	extraLen := 0
	if dataLeft > 0 && dataLeft < b.extraLen {
		// If the available part can hold the left data, receive it and save
		// the data at the position of writePos.
		extraLen, err = io.ReadFull(b.src, b.buf[writePos:writePos+dataLeft])
		if err != nil {
			return false, err
		}
		if extraLen < dataLeft {
			return false, io.ErrShortWrite
		}
		writePos += extraLen
		dataLeft = 0
	}
	if dataLeft == 0 && b.consumeMsg(b.buf[readPos:writePos]) {
		return false, nil
	}

	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	// Write the data in buffer.
	n, err := dst.Write(b.buf[readPos:writePos])
	if err != nil {
		return false, err
	}
	if n < writePos-readPos {
		return false, io.ErrShortWrite
	}

	// The buffer does not hold all packet data, so continue to read the packet.
	if dataLeft > 0 {
		m, err := io.CopyN(dst, b.src, int64(dataLeft))
		if err != nil {
			return false, err
		}
		if int(m) < dataLeft {
			return false, io.ErrShortWrite
		}
	}
	return false, err
}

// receive receives a MySQL packet. This is used in test only.
func (b *msgBuf) receive() ([]byte, error) {
	// Receive header of the current packet.
	size, err := b.preRecv()
	if err != nil {
		return nil, err
	}

	if size <= b.availLen {
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
	if n < 0 || n > b.availLen {
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
	c, err := io.ReadAtLeast(b.src, b.buf[b.end:b.availLen], minReadSize)
	b.end += c
	return err
}

// writeDataDirectly writes data to dst directly without put it into buffer.
// This operation happens in server connection, and the dst is client conn.
//
// A call (net.Conn).Write() is go-routine safe, so we need to make sure the
// data we are trying to write is a whole MySQL packet.
//
// NB: The write operation needs a lock to keep safe because in sendTo method,
// the write operation of a whole MySQL packet may be divided in two steps.
// The data must is a whole MySQL packet, otherwise it is not safe to write it.
func (b *msgBuf) writeDataDirectly(dst io.Writer, data []byte) error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	_, err := dst.Write(data)
	if err != nil {
		return err
	}
	return nil
}
