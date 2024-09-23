// Copyright 2021 - 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"container/list"
	"context"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
)

const (
	fixBufferSize = 1024 * 1024
)

type SessionConn interface {
	ID() uint64
	RawConn() net.Conn
	UseConn(net.Conn)
	Disconnect() error
	Close() error
	Read() ([]byte, error)
	Append(...byte) error
	BeginPacket() error
	FinishedPacket() error
	Flush() error
	Write([]byte) error
	RemoteAddress() string
}

type BufferAllocator struct {
	allocator *SessionAllocator
}

func (ba *BufferAllocator) Alloc(size int) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	return ba.allocator.Alloc(size)
}

func (ba *BufferAllocator) Free(buf []byte) {
	if buf == nil || cap(buf) == 0 {
		return
	}
	ba.allocator.Free(buf)
}

type ListBlock struct {
	data       []byte
	writeIndex int
	readIndex  int
}
type Conn struct {
	id                    uint64
	conn                  net.Conn
	localAddr, remoteAddr string
	sequenceId            uint8
	header                [4]byte
	// static buffer block
	fixBuf *ListBlock
	// dynamic buffer block is organized by a list
	dynamicBuf *list.List
	// just for load local
	loadLocalBuf []byte
	// current block pointer being written
	curBuf *ListBlock
	// current packet header pointer
	curHeader []byte
	// current block write pointer
	// buffer data size, used to check maxBytesToFlush
	bufferLength int
	// packet data size, used to count header
	packetLength      int
	maxBytesToFlush   int
	packetInBuf       int
	allowedPacketSize int
	timeout           time.Duration
	allocator         *BufferAllocator
	ses               *Session
	mu                sync.Mutex
	closeFunc         sync.Once
}

// NewIOSession create a new io session
func NewIOSession(conn net.Conn, pu *config.ParameterUnit) (*Conn, error) {
	// just for ut
	_, ok := globalSessionAlloc.Load().(*SessionAllocator)
	if !ok {
		allocator := NewSessionAllocator(pu)
		setGlobalSessionAlloc(allocator)
	}

	c := &Conn{
		conn:              conn,
		localAddr:         conn.LocalAddr().String(),
		remoteAddr:        conn.RemoteAddr().String(),
		fixBuf:            &ListBlock{},
		dynamicBuf:        list.New(),
		allocator:         &BufferAllocator{allocator: getGlobalSessionAlloc()},
		timeout:           pu.SV.SessionTimeout.Duration,
		maxBytesToFlush:   int(pu.SV.MaxBytesInOutbufToFlush * 1024),
		allowedPacketSize: int(MaxPayloadSize),
	}
	var err error
	c.fixBuf.data, err = c.allocator.Alloc(fixBufferSize)
	if err != nil {
		return nil, err
	}

	c.curHeader = c.fixBuf.data[:HeaderLengthOfTheProtocol]
	c.curBuf = c.fixBuf
	return c, err
}

func (c *Conn) ID() uint64 {
	return c.id
}

func (c *Conn) RawConn() net.Conn {
	return c.conn
}

func (c *Conn) UseConn(conn net.Conn) {
	c.conn = conn
}

func (c *Conn) Close() error {
	var err error
	c.closeFunc.Do(func() {
		defer func() {
			if c.fixBuf != nil && len(c.fixBuf.data) > 0 {
				// Free all allocated memory
				c.allocator.Free(c.fixBuf.data)
				c.fixBuf.data = nil
			}
			if c.dynamicBuf != nil {
				for e := c.dynamicBuf.Front(); e != nil; e = e.Next() {
					c.allocator.Free(e.Value.([]byte))
				}
				c.dynamicBuf.Init()
			}
		}()

		err = c.closeConn()
		if err != nil {
			return
		}
		c.ses = nil
		rm := getGlobalRtMgr()
		if rm != nil {
			rm.Closed(c)
		}
	})
	return err
}

func (c *Conn) CheckAllowedPacketSize(totalLength int) error {
	var err error
	if totalLength > c.allowedPacketSize {
		errMsg := moerr.MysqlErrorMsgRefer[moerr.ER_SERVER_NET_PACKET_TOO_LARGE]
		err = c.ses.GetResponser().MysqlRrWr().WriteERR(errMsg.ErrorCode, strings.Join(errMsg.SqlStates, ","), errMsg.ErrorMsgOrFormat)
		if err != nil {
			return err
		}
		return moerr.NewInternalError(context.Background(), errMsg.ErrorMsgOrFormat)
	}
	return nil
}

// ReadLoadLocalPacket just for processLoadLocal, reuse memory, and not merge 16MB packets
func (c *Conn) ReadLoadLocalPacket() ([]byte, error) {

	var err error
	var packetLength int
	defer func() {
		if err != nil {
			c.allocator.Free(c.loadLocalBuf)
			c.loadLocalBuf = nil
		}
	}()
	err = c.ReadIntoSlices(c.header[:], HeaderLengthOfTheProtocol)
	if err != nil {
		return nil, err
	}
	packetLength = int(uint32(c.header[0]) | uint32(c.header[1])<<8 | uint32(c.header[2])<<16)
	sequenceId := c.header[3]
	c.sequenceId = sequenceId + 1

	if c.loadLocalBuf == nil {
		c.loadLocalBuf, err = c.allocator.Alloc(packetLength)
		if err != nil {
			return nil, err
		}
	} else if len(c.loadLocalBuf) < packetLength {
		c.allocator.Free(c.loadLocalBuf)
		c.loadLocalBuf = nil
		c.loadLocalBuf, err = c.allocator.Alloc(packetLength)
		if err != nil {
			return nil, err
		}
	}

	err = c.ReadIntoSlices(c.loadLocalBuf, packetLength)
	if err != nil {
		return nil, err
	}
	return c.loadLocalBuf[:packetLength], nil
}

// Read reads the complete packet including process the > 16MB packet. return the payload
func (c *Conn) Read() ([]byte, error) {
	// Requests > 16MB
	payloads := make([][]byte, 0)
	var firstPayload []byte
	var finalPayload []byte
	var payload []byte
	var err error
	var packetLength, totalLength int
	var headerPrepared bool
	defer func() {
		for i := range payloads {
			c.allocator.Free(payloads[i])
		}
	}()
	packetLength, headerPrepared = c.GetPacketLengthFromReadBuf()

	if c.ReadBufLen() < HeaderLengthOfTheProtocol+packetLength {
		c.CleanConsumedPacket()
	}

	// Read at least 1 packet or until there is no space in the buffer
	for c.ReadBufLen() < HeaderLengthOfTheProtocol+packetLength && !c.ReadBufIsFull() {
		err = c.ReadIntoBuf()
		if err != nil {
			return nil, err
		}
		// Check if the packet header has been read into buffer
		if !headerPrepared {
			packetLength, headerPrepared = c.GetPacketLengthFromReadBuf()
		}
	}

	totalLength += packetLength
	err = c.CheckAllowedPacketSize(totalLength)
	if err != nil {
		return nil, err
	}

	firstPayload = c.CopyFromBuf(packetLength)

	if packetLength+HeaderLengthOfTheProtocol < c.ReadBufLen() {
		// CASE 1: packet length < 1MB, fixBuf have more than 1 packet, c.fixBuf.writeIndex > HeaderLengthOfTheProtocol + packetLength
		// So we keep next packet data
		c.IncReadIndex(packetLength + HeaderLengthOfTheProtocol)
	} else if packetLength+HeaderLengthOfTheProtocol == c.ReadBufLen() {
		// CASE 2: packet length < 1MB, fixBuf just have 1 packet, c.fixBuf.writeIndex > HeaderLengthOfTheProtocol = packetLength
		// indicates that fixBuf have no data, clean it
		c.ResetReadBufIndex()
	} else {
		// CASE 3: packet length > 1MB, c.fixBuf.writeIndex < HeaderLengthOfTheProtocol + packetLength
		// NOTE: only read the remaining bytes of the current packet, do not read the next packet
		hasPayloadLen := c.ReadBufLen() - HeaderLengthOfTheProtocol
		err = c.ReadIntoSlices(firstPayload[hasPayloadLen:], packetLength-hasPayloadLen)
		if err != nil {
			return nil, err
		}
		c.ResetReadBufIndex()
	}

	if packetLength != int(MaxPayloadSize) {
		return firstPayload, nil
	}

	// ======================== Packet > 16MB ========================
	for {
		// If total package length > 16MB, only one package will be read each iter
		err = c.ReadIntoSlices(c.header[:], HeaderLengthOfTheProtocol)
		if err != nil {
			return nil, err
		}
		packetLength = int(uint32(c.header[0]) | uint32(c.header[1])<<8 | uint32(c.header[2])<<16)
		c.sequenceId = c.header[3] + 1

		if packetLength == 0 {
			break
		}
		totalLength += packetLength
		err = c.CheckAllowedPacketSize(totalLength)
		if err != nil {
			return nil, err
		}

		payload, err = c.ReadOnePayload(packetLength)
		if err != nil {
			return nil, err
		}

		payloads = append(payloads, payload)

		if uint32(packetLength) != MaxPayloadSize {
			break
		}
	}

	if totalLength > 0 {
		finalPayload = make([]byte, totalLength)
	}

	copyIndex := 0
	copy(finalPayload[copyIndex:], firstPayload)
	copyIndex += len(firstPayload)
	for _, eachPayload := range payloads {
		copy(finalPayload[copyIndex:], eachPayload)
		copyIndex += len(eachPayload)
	}
	return finalPayload, nil
}

// IncReadIndex increase the read index of read buffer
func (c *Conn) IncReadIndex(n int) {
	c.fixBuf.readIndex += n
}

// ReadBufLen get unconsumed byte from the read buf
func (c *Conn) ReadBufLen() int {
	return c.fixBuf.writeIndex - c.fixBuf.readIndex
}

// GetPacketLengthFromReadBuf try to get packet header from read buf
func (c *Conn) GetPacketLengthFromReadBuf() (int, bool) {
	if c.ReadBufLen() < HeaderLengthOfTheProtocol {
		return int(MaxPayloadSize), false
	}
	header := c.fixBuf.data[c.fixBuf.readIndex : c.fixBuf.readIndex+HeaderLengthOfTheProtocol]
	packetLength := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	c.sequenceId = header[3] + 1
	return packetLength, true
}

// CleanConsumedPacket clean up the packet that have been consumed and move that have not been consumed forward
func (c *Conn) CleanConsumedPacket() {
	if c.fixBuf.readIndex != 0 {
		copy(c.fixBuf.data, c.fixBuf.data[c.fixBuf.readIndex:c.fixBuf.writeIndex])
		c.fixBuf.writeIndex -= c.fixBuf.readIndex
		c.fixBuf.readIndex = 0
	}
}

// ResetReadBufIndex set read and write index = 0
func (c *Conn) ResetReadBufIndex() {
	c.fixBuf.readIndex = 0
	c.fixBuf.writeIndex = 0
}

// ReadBufIsFull check read buf full or not
func (c *Conn) ReadBufIsFull() bool {
	return c.fixBuf.writeIndex == fixBufferSize
}

// CopyFromBuf make memory for a payload and copy from read buf
func (c *Conn) CopyFromBuf(packetLength int) []byte {
	data := make([]byte, packetLength)
	copy(data, c.fixBuf.data[c.fixBuf.readIndex+HeaderLengthOfTheProtocol:])
	return data
}

// ReadOnePayload allocates memory for a payload and reads it
func (c *Conn) ReadOnePayload(packetLength int) ([]byte, error) {
	var err error
	var payload []byte
	defer func(payload []byte, err error) {
		if err != nil {
			c.allocator.Free(payload)
		}
	}(payload, err)

	if packetLength == 0 {
		return nil, nil
	}

	payload, err = c.allocator.Alloc(packetLength)
	if err != nil {
		return nil, err
	}

	err = c.ReadIntoSlices(payload, packetLength)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

// ReadIntoSlices reads specified bytes from the network
func (c *Conn) ReadIntoSlices(buf []byte, Length int) error {
	var err error
	var n int
	var readLength int
	for readLength < Length {
		n, err = c.ReadFromConn(buf[readLength:Length])
		if err != nil {
			return err
		}
		readLength += n

	}
	return err
}

func (c *Conn) ReadIntoBuf() error {
	n, err := c.ReadFromConn(c.fixBuf.data[c.fixBuf.writeIndex:])
	if err != nil {
		return err
	}
	c.fixBuf.writeIndex += n
	return nil
}

// ReadFromConn is the base method for receiving from network, calling net.Conn.Read().
// The maximum read length is len(buf)
func (c *Conn) ReadFromConn(buf []byte) (int, error) {
	var err error
	if c.timeout > 0 {
		err = c.conn.SetReadDeadline(time.Now().Add(c.timeout))
		if err != nil {
			return 0, err
		}
	}
	return c.conn.Read(buf)
}

// Append Add bytes to buffer
func (c *Conn) Append(elems ...byte) error {
	var err error
	defer func(err error) {
		if err != nil {
			c.Reset()
		}
	}(err)
	cutIndex := 0
	for cutIndex < len(elems) {
		// if bufferLength > 16MB, split packet
		remainPacketSpace := int(MaxPayloadSize) - c.packetLength
		writeLength := Min(remainPacketSpace, len(elems[cutIndex:]))
		err = c.AppendPart(elems[cutIndex : cutIndex+writeLength])
		if err != nil {
			return err
		}
		if c.packetLength == int(MaxPayloadSize) {
			err = c.FinishedPacket()
			if err != nil {
				return err
			}

			err = c.BeginPacket()
			if err != nil {
				return err
			}
		}

		cutIndex += writeLength
	}

	return err
}

// AppendPart is the base method of adding bytes to buffer
func (c *Conn) AppendPart(elems []byte) error {

	var err error
	curBufRemainSpace := len(c.curBuf.data) - c.curBuf.writeIndex
	if len(elems) > curBufRemainSpace {
		if curBufRemainSpace > 0 {
			copy(c.curBuf.data[c.curBuf.writeIndex:], elems[:curBufRemainSpace])
			c.curBuf.writeIndex += curBufRemainSpace
		}
		curElemsRemainSpace := len(elems) - curBufRemainSpace

		allocLength := Max(fixBufferSize, curElemsRemainSpace)
		if allocLength%fixBufferSize != 0 {
			allocLength += fixBufferSize - allocLength%fixBufferSize
		}

		err = c.PushNewBlock(allocLength)
		if err != nil {
			return err
		}
		copy(c.curBuf.data[c.curBuf.writeIndex:], elems[curBufRemainSpace:])
		c.curBuf.writeIndex += len(elems[curBufRemainSpace:])
	} else {
		copy(c.curBuf.data[c.curBuf.writeIndex:], elems)
		c.curBuf.writeIndex += len(elems)
	}
	c.bufferLength += len(elems)
	c.packetLength += len(elems)
	return err
}

// PushNewBlock allocates memory and push it into the dynamic buffer
func (c *Conn) PushNewBlock(allocLength int) error {

	var err error
	var buf []byte

	defer func(buf []byte, err error) {
		if err != nil {
			c.allocator.Free(buf)
		}
	}(buf, err)

	buf, err = c.allocator.Alloc(allocLength)
	if err != nil {
		return err
	}
	newBlock := &ListBlock{}
	newBlock.data = buf
	c.dynamicBuf.PushBack(newBlock)
	c.curBuf = newBlock
	return nil
}

// BeginPacket Reserve Header in the buffer
func (c *Conn) BeginPacket() error {
	if len(c.curBuf.data)-c.curBuf.writeIndex < HeaderLengthOfTheProtocol {
		err := c.PushNewBlock(fixBufferSize)
		if err != nil {
			return err
		}
	}
	c.curHeader = c.curBuf.data[c.curBuf.writeIndex : c.curBuf.writeIndex+HeaderLengthOfTheProtocol]
	c.curBuf.writeIndex += HeaderLengthOfTheProtocol
	c.bufferLength += HeaderLengthOfTheProtocol
	return nil
}

// FinishedPacket Fill in the header and flush if buffer full
func (c *Conn) FinishedPacket() error {
	if c.bufferLength < 0 {
		return moerr.NewInternalError(moerr.Context(), "buffer length must >= 0")
	}
	binary.LittleEndian.PutUint32(c.curHeader, uint32(c.packetLength))
	c.curHeader[3] = c.sequenceId
	c.sequenceId += 1
	c.packetInBuf += 1
	c.packetLength = 0
	err := c.FlushIfFull()
	if err != nil {
		return err
	}

	return nil
}

func (c *Conn) FlushIfFull() error {
	var err error
	if c.bufferLength >= c.maxBytesToFlush {
		err = c.Flush()
		if err != nil {
			return err
		}
	}
	return err
}

// Flush Send buffer to the network
func (c *Conn) Flush() error {
	if c.bufferLength == 0 {
		return nil
	}
	var err error
	defer c.Reset()
	err = c.WriteToConn(c.fixBuf.data[:c.fixBuf.writeIndex])
	if err != nil {
		return err
	}

	for node := c.dynamicBuf.Front(); node != nil; node = node.Next() {
		block := node.Value.(*ListBlock)
		err = c.WriteToConn(block.data[:block.writeIndex])
		if err != nil {
			return err
		}
	}
	c.ses.CountPacket(1)
	c.packetInBuf = 0
	return err
}

// Write Only OK, EOF, ERROR needs to be sent immediately
func (c *Conn) Write(payload []byte) error {
	defer c.Reset()

	var err error
	var header [4]byte
	length := len(payload)
	binary.LittleEndian.PutUint32(header[:], uint32(length))
	if payload[0] == 0xFF {
		if c.packetInBuf != 0 && len(c.fixBuf.data) >= HeaderLengthOfTheProtocol {
			c.sequenceId = c.fixBuf.data[3]
		}
		header[3] = c.sequenceId
		c.sequenceId += 1
		err = c.WriteToConn(append(header[:], payload...))
		return err
	}
	header[3] = c.sequenceId
	c.sequenceId += 1
	err = c.Append(append(header[:], payload...)...)
	if err != nil {
		return err
	}
	err = c.Flush()
	if err != nil {
		return err
	}
	return nil
}

// WriteToConn is the base method for write data to network, calling net.Conn.Write().
func (c *Conn) WriteToConn(buf []byte) error {
	sendLength := 0
	for sendLength < len(buf) {
		n, err := c.conn.Write(buf[sendLength:])
		if err != nil {
			return err
		}
		sendLength += n
	}
	return nil
}

func (c *Conn) RemoteAddress() string {
	return c.remoteAddr
}

func (c *Conn) closeConn() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	if c.conn != nil {
		if err = c.conn.Close(); err != nil {
			return err
		}
	}
	return err
}

func (c *Conn) Reset() {
	c.bufferLength = 0
	c.packetLength = 0
	c.curBuf = c.fixBuf
	c.fixBuf.writeIndex = 0
	c.fixBuf.readIndex = 0
	for node := c.dynamicBuf.Front(); node != nil; node = node.Next() {
		c.allocator.Free(node.Value.(*ListBlock).data)
	}
	c.dynamicBuf.Init()
	c.packetInBuf = 0
	c.allocator.Free(c.loadLocalBuf)
	c.loadLocalBuf = nil

}
