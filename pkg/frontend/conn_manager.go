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
	timeout         time.Duration
	allocator       *SessionAllocator
}

// NewIOSession create a new io session
func NewIOSession(conn net.Conn, pu *config.ParameterUnit) (*Conn, error) {

	c := &Conn{
		conn:            conn,
		localAddr:       conn.RemoteAddr().String(),
		remoteAddr:      conn.LocalAddr().String(),
		connected:       true,
		fixBuf:          &ListBlock{},
		dynamicBuf:      list.New(),
		allocator:       NewSessionAllocator(pu),
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
	if c.connected == false {
		return nil
	}
	return c.closeConn()
}

func (c *Conn) Close() error {
	if c.connected == false {
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

	getGlobalRtMgr().Closed(c)
	return nil
}

func (c *Conn) Read() ([]byte, error) {
	// Requests > 16MB
	payloads := make([][]byte, 0)
	defer func() {
		if payloads != nil {
			for _, payload := range payloads {
				c.allocator.Free(payload)
			}
		}
	}()
	var err error
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
			return payload, err
		} else {
			payloads = append(payloads, payload)
			break
		}
	}

	totalLength := 0
	for _, payload := range payloads {
		totalLength += len(payload)
	}
	finalPayload, err := c.allocator.Alloc(totalLength)

	if err != nil {
		return nil, err
	}

	copyIndex := 0
	for _, payload := range payloads {
		copy(finalPayload[copyIndex:], payload)
		c.allocator.Free(payload)
		copyIndex += len(payload)
	}
	payloads = nil
	return finalPayload, nil
}

func (c *Conn) ReadOnePayload(packetLength int) ([]byte, error) {
	var buf []byte
	var err error

	if packetLength > fixBufferSize {
		buf, err = c.allocator.Alloc(packetLength)
		if err != nil {
			return nil, err
		}
		defer c.allocator.Free(buf)
	} else {
		buf = c.fixBuf.data
	}
	err = c.ReadBytes(buf[:packetLength], packetLength)
	if err != nil {
		return nil, err
	}

	payload, err := c.allocator.Alloc(packetLength)

	if err != nil {
		return nil, err
	}

	copy(payload, buf[:packetLength])
	return payload, nil
}

func (c *Conn) ReadBytes(buf []byte, Length int) error {
	var err error
	var n int
	var readLength int
	for readLength < Length {
		n, err = c.ReadFromConn(buf[readLength:])
		if err != nil {
			return err
		}
		readLength += n

	}
	return err
}
func (c *Conn) ReadFromConn(buf []byte) (int, error) {
	err := c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return 0, err
	}
	n, err := c.conn.Read(buf)
	return n, err
}

// Append Add bytes to buffer
func (c *Conn) Append(elems ...byte) error {
	var err error

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

func (c *Conn) AppendPart(elems []byte) error {
	var buf []byte
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

		buf, err = c.allocator.Alloc(allocLength)
		if err != nil {
			return err
		}

		copy(buf, elems[curBufRemainSpace:])
		newBlock := &ListBlock{}
		newBlock.data = buf
		newBlock.writeIndex = curElemsRemainSpace
		c.dynamicBuf.PushBack(newBlock)
		c.curBuf = newBlock

	} else {
		copy(c.curBuf.data[c.curBuf.writeIndex:], elems)
		c.curBuf.writeIndex += len(elems)
	}
	c.bufferLength += len(elems)
	c.packetLength += len(elems)
	return err
}

// BeginPacket Reserve Header in the buffer
func (c *Conn) BeginPacket() error {
	if len(c.curBuf.data)-c.curBuf.writeIndex < HeaderLengthOfTheProtocol {
		buf, err := c.allocator.Alloc(fixBufferSize)
		if err != nil {
			return err
		}
		newBlock := &ListBlock{}
		newBlock.data = buf
		c.dynamicBuf.PushBack(newBlock)
		c.curBuf = newBlock
	}
	c.curHeader = c.curBuf.data[c.curBuf.writeIndex : c.curBuf.writeIndex+HeaderLengthOfTheProtocol]
	c.curBuf.writeIndex += HeaderLengthOfTheProtocol
	c.bufferLength += HeaderLengthOfTheProtocol
	return nil
}

// FinishedPacket Fill in the header and flush if buffer full
func (c *Conn) FinishedPacket() error {
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

	for c.dynamicBuf.Len() > 0 {
		node := c.dynamicBuf.Front()
		block := node.Value.(*ListBlock)
		err = c.WriteToConn(block.data[:block.writeIndex])
		if err != nil {
			return err
		}
		c.allocator.Free(block.data)
		c.dynamicBuf.Remove(node)
	}

	return err

}

// Write Only OK, EOF, ERROR needs to be sent immediately
func (c *Conn) Write(payload []byte) error {
	defer c.Reset()
	if !c.connected {
		return moerr.NewInternalError(moerr.Context(), "The IOSession connection has been closed")
	}

	var err error

	err = c.Flush()
	if err != nil {
		return err
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
	return nil
}

func (c *Conn) WriteToConn(buf []byte) error {
	sendlength := 0
	for sendlength != len(buf) {
		n, err := c.conn.Write(buf[sendlength:])
		sendlength += n
		if err != nil {
			return err
		}
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
