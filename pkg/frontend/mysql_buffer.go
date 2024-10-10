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
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
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

var _ Allocator = new(BufferAllocator)

type BufferAllocator struct {
	allocator Allocator
}

func (ba *BufferAllocator) Alloc(size int) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	return ba.allocator.Alloc(size)
}

func (ba *BufferAllocator) Free(buf []byte) {
	if len(buf) == 0 || cap(buf) == 0 {
		return
	}
	ba.allocator.Free(buf)
}

type MemBlock struct {
	data       []byte
	writeIndex int
	readIndex  int
}

func (block *MemBlock) freeBuffUnsafe(alloc Allocator) {
	if block == nil {
		return
	}
	// Free all allocated memory
	alloc.Free(block.data)
	block.data = nil
}

// IncReadIndex increase the read index of read buffer
func (block *MemBlock) IncReadIndex(n int) {
	block.readIndex += n
}

func (block *MemBlock) IncWriteIndex(len int) {
	block.writeIndex += len
}

// ResetIndices set read and write index = 0
func (block *MemBlock) ResetIndices() {
	block.readIndex = 0
	block.writeIndex = 0
}

// IsFull check read buf full or not
func (block *MemBlock) IsFull() bool {
	return block.writeIndex == fixBufferSize
}

func (block *MemBlock) BufferLen() int {
	return len(block.data)
}

// AvailableDataLen get unconsumed byte from the read buf
func (block *MemBlock) AvailableDataLen() int {
	return block.writeIndex - block.readIndex
}

/*
AvailableData return the slice of data[readIndex : writeIndex]
*/
func (block *MemBlock) AvailableData() []byte {
	return block.data[block.readIndex:block.writeIndex]
}

func (block *MemBlock) Head() []byte {
	return block.data[block.readIndex : block.readIndex+HeaderLengthOfTheProtocol]
}

func (block *MemBlock) AvailableDataAfterHead() []byte {
	return block.data[block.readIndex+HeaderLengthOfTheProtocol : block.writeIndex]
}

func (block *MemBlock) AvailableSpace() []byte {
	return block.data[block.writeIndex:]
}

func (block *MemBlock) AvailableSpaceLen() int {
	return len(block.data) - block.writeIndex
}

func (block *MemBlock) ReserveHead() []byte {
	return block.data[block.writeIndex : block.writeIndex+HeaderLengthOfTheProtocol]
}

// CopyDataAfterHeadOutTo copy the data in read buf to output buffer
func (block *MemBlock) CopyDataAfterHeadOutTo(output []byte) {
	copy(output, block.AvailableDataAfterHead())
}

func (block *MemBlock) CopyDataIn(input []byte) {
	copy(block.AvailableSpace(), input)
}

// Adjust clean up the packet that have been consumed and
// move the data forward that has not been consumed
func (block *MemBlock) Adjust() {
	if block.readIndex != 0 {
		copy(block.data, block.AvailableData())
		block.writeIndex -= block.readIndex
		block.readIndex = 0
	}
}

type Conn struct {
	id                    uint64
	conn                  net.Conn
	localAddr, remoteAddr string
	sequenceId            uint8
	header                [4]byte
	// static buffer block for read & write in general cases
	fixBuf MemBlock
	// dynamic write buffer block is organized by a list
	dynamicWrBuf *list.List
	// just for load local read
	loadLocalBuf MemBlock
	// current block pointer being written
	curBuf *MemBlock
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
	closeFunc         sync.Once
}

// NewIOSession create a new io session
func NewIOSession(conn net.Conn, pu *config.ParameterUnit) (_ *Conn, err error) {
	// just for ut
	_, ok := globalSessionAlloc.Load().(Allocator)
	if !ok {
		allocator := NewSessionAllocator(pu)
		setGlobalSessionAlloc(allocator)
	}

	c := &Conn{
		conn:              conn,
		localAddr:         conn.LocalAddr().String(),
		remoteAddr:        conn.RemoteAddr().String(),
		fixBuf:            MemBlock{},
		dynamicWrBuf:      list.New(),
		allocator:         &BufferAllocator{allocator: getGlobalSessionAlloc()},
		timeout:           pu.SV.SessionTimeout.Duration,
		maxBytesToFlush:   int(pu.SV.MaxBytesInOutbufToFlush * 1024),
		allowedPacketSize: int(MaxPayloadSize),
	}

	defer func() {
		if err != nil {
			c.freeBuffUnsafe()
		}
	}()

	c.fixBuf.data, err = c.allocator.Alloc(fixBufferSize)
	if err != nil {
		return nil, err
	}

	c.curHeader = c.fixBuf.data[:HeaderLengthOfTheProtocol]
	c.curBuf = &c.fixBuf
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

func (c *Conn) freeDynamicBuffUnsafe() {
	if c != nil && c.dynamicWrBuf != nil {
		for e := c.dynamicWrBuf.Front(); e != nil; e = e.Next() {
			if block, ok := e.Value.(*MemBlock); ok && block != nil {
				block.freeBuffUnsafe(c.allocator)
				block.data = nil
			}
		}
		c.dynamicWrBuf.Init()
	}
}

func (c *Conn) freeBuffUnsafe() {
	if c == nil {
		return
	}
	c.fixBuf.freeBuffUnsafe(c.allocator)
	c.loadLocalBuf.freeBuffUnsafe(c.allocator)
	c.freeDynamicBuffUnsafe()
}

func (c *Conn) freeNoFixBuffUnsafe() {
	if c == nil {
		return
	}
	c.loadLocalBuf.freeBuffUnsafe(c.allocator)
	c.freeDynamicBuffUnsafe()
}

func (c *Conn) Close() error {
	var err error
	c.closeFunc.Do(func() {
		defer func() {
			c.freeBuffUnsafe()
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
func (c *Conn) ReadLoadLocalPacket() (_ []byte, err error) {
	var packetLength int
	defer func() {
		if rErr := recover(); rErr != nil {
			_, ok := rErr.(*moerr.Error)
			if !ok {
				err = errors.Join(err, moerr.ConvertPanicError(context.Background(), rErr))
			} else {
				err = errors.Join(err, rErr.(error))
			}
		}
		if err != nil {
			c.FreeLoadLocal()
		}
	}()
	err = c.ReadNBytesIntoBuf(c.header[:], HeaderLengthOfTheProtocol)
	if err != nil {
		return
	}
	packetLength = int(uint32(c.header[0]) | uint32(c.header[1])<<8 | uint32(c.header[2])<<16)
	sequenceId := c.header[3]
	c.sequenceId = sequenceId + 1

	if c.loadLocalBuf.data == nil {
		c.loadLocalBuf.data, err = c.allocator.Alloc(packetLength)
		if err != nil {
			return
		}
	} else if len(c.loadLocalBuf.data) < packetLength {
		c.loadLocalBuf.freeBuffUnsafe(c.allocator)
		c.loadLocalBuf.data, err = c.allocator.Alloc(packetLength)
		if err != nil {
			return
		}
	}

	err = c.ReadNBytesIntoBuf(c.loadLocalBuf.data, packetLength)
	if err != nil {
		return
	}
	return c.loadLocalBuf.data[:packetLength], err
}

func (c *Conn) FreeLoadLocal() {
	c.loadLocalBuf.freeBuffUnsafe(c.allocator)
}

// Read reads the complete packet including process the > 16MB packet. return the payload
// packet format:
//
// |------packet------------------------------------------------------------------|
// |---3-bytes-payload length---+---1 byte sequence_id---+----------payload-------|
// if the length pf packet is more that 16MB. there are multiple packets for the same packet.
func (c *Conn) Read() (_ []byte, err error) {
	// Requests > 16MB
	payloads := make([][]byte, 0)
	var firstPayload []byte
	var finalPayload []byte
	var payload []byte
	var payloadLength, totalLength int
	var headerPrepared bool
	defer func() {
		if rErr := recover(); rErr != nil {
			_, ok := rErr.(*moerr.Error)
			if !ok {
				err = errors.Join(err, moerr.ConvertPanicError(context.Background(), rErr))
			} else {
				err = errors.Join(err, rErr.(error))
			}
		}
		for i := range payloads {
			c.allocator.Free(payloads[i])
		}
		payloads = nil
		//release related buffer if there is an error
		if err != nil {
			firstPayload = nil
			finalPayload = nil
			payload = nil
			c.fixBuf.ResetIndices()
		}
	}()
	payloadLength, headerPrepared = c.GetPayloadLength()

	if c.fixBuf.AvailableDataLen() < HeaderLengthOfTheProtocol+payloadLength {
		c.fixBuf.Adjust()
	}

	// Read at least 1 packet or until there is no space in the read buffer
	for c.fixBuf.AvailableDataLen() < HeaderLengthOfTheProtocol+payloadLength && !c.fixBuf.IsFull() {
		err = c.ReadIntoReadBuf()
		if err != nil {
			return nil, err
		}
		// Check if the packet header has been read into buffer
		if !headerPrepared {
			payloadLength, headerPrepared = c.GetPayloadLength()
		}
	}

	totalLength += payloadLength
	err = c.CheckAllowedPacketSize(totalLength)
	if err != nil {
		return nil, err
	}

	firstPayload = make([]byte, payloadLength)
	c.fixBuf.CopyDataAfterHeadOutTo(firstPayload)

	if payloadLength+HeaderLengthOfTheProtocol < c.fixBuf.AvailableDataLen() {
		// CASE 1: packet length < 1MB, fixBuf have more than 1 packet, c.fixBuf.writeIndex > HeaderLengthOfTheProtocol + payloadLength
		// So we keep next packet data
		c.fixBuf.IncReadIndex(payloadLength + HeaderLengthOfTheProtocol)
	} else if payloadLength+HeaderLengthOfTheProtocol == c.fixBuf.AvailableDataLen() {
		// CASE 2: packet length < 1MB, fixBuf just have 1 packet, c.fixBuf.writeIndex > HeaderLengthOfTheProtocol = payloadLength
		// indicates that fixBuf have no data, clean it
		c.fixBuf.ResetIndices()
	} else {
		// CASE 3: packet length > 1MB, c.fixBuf.writeIndex < HeaderLengthOfTheProtocol + payloadLength
		// NOTE: only read the remaining bytes of the current packet, do not read the next packet
		hasPayloadLen := c.fixBuf.AvailableDataLen() - HeaderLengthOfTheProtocol
		err = ReadNBytesIntoBuf(c, firstPayload[hasPayloadLen:], payloadLength-hasPayloadLen)
		if err != nil {
			return nil, err
		}
		c.fixBuf.ResetIndices()
	}

	if payloadLength != int(MaxPayloadSize) {
		return firstPayload, nil
	}

	// ======================== Packet > 16MB ========================
	for {
		// If total package length > 16MB, only one package will be read each iter
		err = ReadNBytesIntoBuf(c, c.header[:], HeaderLengthOfTheProtocol)
		if err != nil {
			return nil, err
		}
		payloadLength = int(uint32(c.header[0]) | uint32(c.header[1])<<8 | uint32(c.header[2])<<16)
		c.sequenceId = c.header[3] + 1

		if payloadLength == 0 {
			break
		}
		totalLength += payloadLength
		err = c.CheckAllowedPacketSize(totalLength)
		if err != nil {
			return nil, err
		}

		payload, err = ReadOnePayload(c, payloadLength)
		if err != nil {
			return nil, err
		}

		payloads = append(payloads, payload)

		if uint32(payloadLength) != MaxPayloadSize {
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

// GetPayloadLength try to get packet header from read buf
// return:
//
//	the packet length
//	the header is ready or not
func (c *Conn) GetPayloadLength() (int, bool) {
	if c.fixBuf.AvailableDataLen() < HeaderLengthOfTheProtocol {
		return int(MaxPayloadSize), false
	}
	header := c.fixBuf.Head()
	payloadLength := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	c.sequenceId = header[3] + 1
	return payloadLength, true
}

var ReadOnePayload = func(c *Conn, payloadLength int) ([]byte, error) {
	return c.ReadOnePayload(payloadLength)
}

// ReadOnePayload allocates memory for a payload and reads it
func (c *Conn) ReadOnePayload(payloadLength int) (payload []byte, err error) {
	defer func() {
		if rErr := recover(); rErr != nil {
			_, ok := rErr.(*moerr.Error)
			if !ok {
				err = errors.Join(err, moerr.ConvertPanicError(context.Background(), rErr))
			} else {
				err = errors.Join(err, rErr.(error))
			}
		}
		if err != nil {
			c.allocator.Free(payload)
			payload = nil
		}
	}()

	if payloadLength == 0 {
		return
	}

	payload, err = c.allocator.Alloc(payloadLength)
	if err != nil {
		return
	}

	err = ReadNBytesIntoBuf(c, payload, payloadLength)
	if err != nil {
		return
	}

	return
}

var ReadNBytesIntoBuf = func(c *Conn, buf []byte, n int) error {
	return c.ReadNBytesIntoBuf(buf, n)
}

// ReadNBytesIntoBuf reads specified bytes from the network
func (c *Conn) ReadNBytesIntoBuf(buf []byte, n int) error {
	var err error
	var read int
	var readLength int
	for readLength < n {
		read, err = c.ReadFromConn(buf[readLength:n])
		if err != nil {
			return err
		}
		readLength += read

	}
	return err
}

func (c *Conn) ReadIntoReadBuf() error {
	n, err := c.ReadFromConn(c.fixBuf.AvailableSpace())
	if err != nil {
		return err
	}
	c.fixBuf.IncWriteIndex(n)
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
func (c *Conn) Append(elems ...byte) (err error) {
	defer func() {
		if err != nil {
			c.Reset()
		}
	}()
	cutIndex := 0
	for cutIndex < len(elems) {
		// if bufferLength > 16MB, split packet
		remainPacketSpace := int(MaxPayloadSize) - c.packetLength
		writeLength := Min(remainPacketSpace, len(elems[cutIndex:]))
		err = AppendPart(c, elems[cutIndex:cutIndex+writeLength])
		if err != nil {
			return err
		}
		if c.packetLength == int(MaxPayloadSize) {
			err = FinishedPacket(c)
			if err != nil {
				return err
			}

			err = BeginPacket(c)
			if err != nil {
				return err
			}
		}

		cutIndex += writeLength
	}

	return err
}

var AppendPart = func(c *Conn, elems []byte) error {
	return c.AppendPart(elems)
}

// AppendPart is the base method of adding bytes to buffer
func (c *Conn) AppendPart(elems []byte) error {

	var err error
	curBufRemainSpace := len(c.curBuf.data) - c.curBuf.writeIndex
	if len(elems) > curBufRemainSpace {
		if curBufRemainSpace > 0 {
			c.curBuf.CopyDataIn(elems[:curBufRemainSpace])
			c.curBuf.IncWriteIndex(curBufRemainSpace)
		}
		curElemsRemainSpace := len(elems) - curBufRemainSpace

		allocLength := Max(fixBufferSize, curElemsRemainSpace)
		if allocLength%fixBufferSize != 0 {
			allocLength += fixBufferSize - allocLength%fixBufferSize
		}

		err = AllocNewBlock(c, allocLength)
		if err != nil {
			return err
		}
		c.curBuf.CopyDataIn(elems[curBufRemainSpace:])
		c.curBuf.IncWriteIndex(len(elems[curBufRemainSpace:]))
	} else {
		c.curBuf.CopyDataIn(elems)
		c.curBuf.IncWriteIndex(len(elems))
	}
	c.bufferLength += len(elems)
	c.packetLength += len(elems)
	return err
}

var AllocNewBlock = func(c *Conn, allocLength int) error {
	return c.AllocNewBlock(allocLength)
}

// AllocNewBlock allocates memory and push it into the dynamic buffer
func (c *Conn) AllocNewBlock(allocLength int) (err error) {
	var buf []byte

	defer func() {
		if rErr := recover(); rErr != nil {
			_, ok := rErr.(*moerr.Error)
			if !ok {
				err = errors.Join(err, moerr.ConvertPanicError(context.Background(), rErr))
			} else {
				err = errors.Join(err, rErr.(error))
			}
		}

		if err != nil {
			c.allocator.Free(buf)
		}
	}()

	buf, err = c.allocator.Alloc(allocLength)
	if err != nil {
		return
	}
	newBlock := &MemBlock{}
	newBlock.data = buf
	c.dynamicWrBuf.PushBack(newBlock)
	c.curBuf = newBlock
	return
}

var BeginPacket = func(c *Conn) error {
	return c.BeginPacket()
}

// BeginPacket Reserve Header in the buffer
func (c *Conn) BeginPacket() error {
	if c.curBuf.AvailableSpaceLen() < HeaderLengthOfTheProtocol {
		err := AllocNewBlock(c, fixBufferSize)
		if err != nil {
			return err
		}
	}
	c.curHeader = c.curBuf.ReserveHead()
	c.curBuf.IncWriteIndex(HeaderLengthOfTheProtocol)
	c.bufferLength += HeaderLengthOfTheProtocol
	return nil
}

var FinishedPacket = func(c *Conn) error {
	return c.FinishedPacket()
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
	err = c.WriteToConn(c.fixBuf.AvailableData())
	if err != nil {
		return err
	}

	for node := c.dynamicWrBuf.Front(); node != nil; node = node.Next() {
		block := node.Value.(*MemBlock)
		err = c.WriteToConn(block.AvailableData())
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
	if payload[0] == defines.ErrHeader {
		if c.packetInBuf != 0 && c.fixBuf.BufferLen() >= HeaderLengthOfTheProtocol {
			c.sequenceId = c.fixBuf.data[3]
		}
		header[3] = c.sequenceId
		c.sequenceId += 1
		return c.WriteToConn(append(header[:], payload...))
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
	var err error
	if c.conn != nil {
		if err = c.conn.Close(); err != nil {
			return err
		}
	}
	return err
}

// Reset does not release fix buffer but release dynamical buffer
// and load data local buffer
func (c *Conn) Reset() {
	c.bufferLength = 0
	c.packetLength = 0
	c.curBuf = &c.fixBuf
	c.fixBuf.ResetIndices()
	c.freeDynamicBuffUnsafe()
	c.packetInBuf = 0
	c.loadLocalBuf.freeBuffUnsafe(c.allocator)
}
