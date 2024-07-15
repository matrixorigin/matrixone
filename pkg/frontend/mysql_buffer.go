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
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"net"
	"time"
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
}
type Conn struct {
	id                    uint64
	conn                  net.Conn
	localAddr, remoteAddr string
	connected             bool
	sequenceId            uint8
	header                [4]byte
	// static buffer block
	fixBuf *ListBlock
	// dynamic buffer block is organized by a list
	dynamicBuf *list.List
	// current block pointer being written
	curBuf *ListBlock
	// current packet header pointer
	curHeader []byte
	// current block write pointer
	// buffer data size, used to check maxBytesToFlush
	bufferLength int
	// packet data size, used to count header
	packetLength    int
	maxBytesToFlush int
	packetInBuf     int
	timeout         time.Duration
	allocator       *BufferAllocator
	ses             *Session
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
		conn:            conn,
		localAddr:       conn.RemoteAddr().String(),
		remoteAddr:      conn.LocalAddr().String(),
		connected:       true,
		fixBuf:          &ListBlock{},
		dynamicBuf:      list.New(),
		allocator:       &BufferAllocator{allocator: getGlobalSessionAlloc()},
		timeout:         pu.SV.SessionTimeout.Duration,
		maxBytesToFlush: int(pu.SV.MaxBytesInOutbufToFlush * 1024),
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
func (c *Conn) Disconnect() error {
	if !c.connected {
		return nil
	}
	return c.closeConn()
}

func (c *Conn) Close() error {
	if !c.connected {
		return nil
	}

	err := c.closeConn()
	if err != nil {
		return err
	}
	c.connected = false

	// Free all allocated memory
	c.allocator.Free(c.fixBuf.data)
	for e := c.dynamicBuf.Front(); e != nil; e = e.Next() {
		c.allocator.Free(e.Value.([]byte))
	}
	c.ses = nil
	getGlobalRtMgr().Closed(c)
	return nil
}

// Read reads the complete packet including process the > 16MB packet. return the payload
func (c *Conn) Read() ([]byte, error) {
	// Requests > 16MB
	payloads := make([][]byte, 0)
	var finalPayload []byte
	var err error
	defer func(payloads [][]byte, err error) {
		for _, payload := range payloads {
			c.allocator.Free(payload)
		}
	}(payloads, err)

	for {
		if !c.connected {
			return nil, moerr.NewInternalError(moerr.Context(), "The IOSession connection has been closed")
		}
		var packetLength int
		err = c.ReadBytes(c.header[:], HeaderLengthOfTheProtocol)
		if err != nil {
			return nil, err
		}
		packetLength = int(uint32(c.header[0]) | uint32(c.header[1])<<8 | uint32(c.header[2])<<16)
		sequenceId := c.header[3]
		c.sequenceId = sequenceId + 1

		if packetLength == 0 {
			break
		}
		// Read bytes based on packet length
		payload, err := c.ReadOnePayload(packetLength)
		if err != nil {
			return nil, err
		}

		if uint32(packetLength) == MaxPayloadSize {
			payloads = append(payloads, payload)
			continue
		}

		if len(payloads) == 0 {
			finalPayload = make([]byte, len(payload))
			copy(finalPayload, payload)
			c.allocator.Free(payload)
			return finalPayload, nil
		} else {
			payloads = append(payloads, payload)
			break
		}
	}

	totalLength := 0
	for _, payload := range payloads {
		totalLength += len(payload)
	}
	if totalLength > 0 {
		finalPayload = make([]byte, totalLength)
	}

	copyIndex := 0
	for _, payload := range payloads {
		copy(finalPayload[copyIndex:], payload)
		copyIndex += len(payload)
	}
	return finalPayload, nil
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

	err = c.ReadBytes(payload, packetLength)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

// ReadBytes reads specified bytes from the network
func (c *Conn) ReadBytes(buf []byte, Length int) error {
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

	n, err := c.conn.Read(buf)
	if err != nil {
		return 0, err
	}
	return n, nil
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
	c.packetInBuf += 1
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
	if !c.connected {
		return moerr.NewInternalError(moerr.Context(), "The IOSession connection has been closed")
	}

	var err error

	if payload[0] != 0xFF {
		err = c.Flush()
		if err != nil {
			return err
		}
	}

	var header [4]byte

	length := len(payload)
	binary.LittleEndian.PutUint32(header[:], uint32(length))
	header[3] = c.sequenceId
	c.sequenceId += 1

	err = c.WriteToConn(append(header[:], payload...))
	if err != nil {
		return err
	}
	c.ses.CountPacket(1)
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
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) Reset() {
	c.bufferLength = 0
	c.packetLength = 0
	c.curBuf = c.fixBuf
	c.fixBuf.writeIndex = 0
	for node := c.dynamicBuf.Front(); node != nil; node = node.Next() {
		c.allocator.Free(node.Value.(*ListBlock).data)
	}
	c.dynamicBuf.Init()

}
