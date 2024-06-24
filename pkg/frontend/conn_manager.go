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

type Allocator interface {
	// Alloc allocate a []byte with len(data) >= size, and the returned []byte cannot
	// be expanded in use.
	Alloc(capacity int) []byte
	// Free free the allocated memory
	Free([]byte)
}

type Conn struct {
	id                    uint64
	conn                  net.Conn
	localAddr, remoteAddr string
	connected             bool
	sequenceId            uint8
	header                [4]byte
	fixBuf                []byte
	curBuf                []byte
	curHeader             []byte
	dynamicBuf            *list.List
	writeIndex            int
	bufferLength          int
	packetLength          int
	maxBytesToFlush       int
	timeout               time.Duration
	allocator             Allocator
}

// NewIOSession create a new io session
func NewIOSession(conn net.Conn, pu *config.ParameterUnit) *Conn {

	bio := &Conn{
		conn:            conn,
		localAddr:       conn.RemoteAddr().String(),
		remoteAddr:      conn.LocalAddr().String(),
		connected:       true,
		dynamicBuf:      list.New(),
		allocator:       NewSessionAllocator(pu),
		timeout:         pu.SV.SessionTimeout.Duration,
		maxBytesToFlush: fixBufferSize,
	}
	bio.fixBuf = bio.allocator.Alloc(fixBufferSize)
	bio.curHeader = bio.fixBuf[:HeaderLengthOfTheProtocol]
	bio.curBuf = bio.fixBuf
	return bio
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
	return c.closeConn()
}

func (c *Conn) Close() error {
	err := c.closeConn()
	if err != nil {
		return err
	}
	c.connected = false

	c.allocator.Free(c.fixBuf)

	for e := c.dynamicBuf.Front(); e != nil; e = e.Next() {
		c.allocator.Free(e.Value.([]byte))
	}

	getGlobalRtMgr().Closed(c)
	return nil
}

func (c *Conn) Read() ([]byte, error) {
	payloads := make([][]byte, 0)
	defer func() {
		for _, payload := range payloads {
			c.allocator.Free(payload)
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
	finalPayload := c.allocator.Alloc(totalLength)

	copyIndex := 0
	for _, payload := range payloads {
		copy(finalPayload[copyIndex:], payload)
		c.allocator.Free(payload)
		copyIndex += len(payload)
	}
	return finalPayload, nil
}

func (c *Conn) ReadOnePayload(packetLength int) ([]byte, error) {
	var buf []byte
	var err error

	if packetLength > fixBufferSize {
		buf = c.allocator.Alloc(packetLength)
		defer c.allocator.Free(buf)
	} else {
		buf = c.fixBuf
	}
	err = c.ReadBytes(buf[:packetLength], packetLength)
	if err != nil {
		return nil, err
	}

	payload := c.allocator.Alloc(packetLength)
	copy(payload, buf[:packetLength])
	return payload, nil
}

func (c *Conn) ReadBytes(buf []byte, Length int) error {
	var err error
	var n int
	var readLength int
	for {
		n, err = c.ReadFromConn(buf[readLength:])
		if err != nil {
			return err
		}
		readLength += n

		if readLength == Length {
			return nil
		}
	}
}
func (c *Conn) ReadFromConn(buf []byte) (int, error) {
	err := c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return 0, err
	}
	n, err := c.conn.Read(buf)
	return n, err
}

func (c *Conn) Append(elems ...byte) {
	cutIndex := 0
	for {
		remainPacketLength := int(MaxPayloadSize) - c.bufferLength
		if len(elems[cutIndex:]) <= remainPacketLength {
			break
		}

		c.AppendPart(elems[cutIndex:remainPacketLength])
		err := c.FinishedPacket()
		if err != nil {
			panic(err)
		}
		cutIndex += remainPacketLength
	}

	c.AppendPart(elems)
}

func (c *Conn) AppendPart(elems []byte) {
	var buf []byte
	remainBuf := fixBufferSize - c.writeIndex
	remainBuf = Max(0, remainBuf)
	if len(elems) > remainBuf {
		if remainBuf > 0 {
			copy(c.curBuf[c.writeIndex:], elems[:remainBuf])
		}

		if len(elems)-remainBuf < fixBufferSize {
			buf = c.allocator.Alloc(fixBufferSize)

		} else {
			buf = c.allocator.Alloc(len(elems) - remainBuf)
		}

		copy(buf, elems[remainBuf:])
		c.writeIndex = len(elems) - remainBuf
		c.dynamicBuf.PushBack(buf)
		c.curBuf = buf

	} else {
		copy(c.curBuf[c.writeIndex:], elems)
		c.writeIndex += len(elems)
	}
	c.bufferLength += len(elems)
	c.packetLength += len(elems)
	return
}
func (c *Conn) BeginPacket() {
	c.curHeader = c.curBuf[c.writeIndex : c.writeIndex+HeaderLengthOfTheProtocol]
	c.writeIndex += HeaderLengthOfTheProtocol
}

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

func (c *Conn) Flush() error {
	if c.bufferLength == 0 {
		return nil
	}
	var err error
	defer c.Reset()
	if c.dynamicBuf.Len() == 0 {
		err = c.WriteToConn(c.fixBuf[:c.writeIndex])
		if err != nil {
			return err
		}
	} else {
		err = c.WriteToConn(c.fixBuf)
		if err != nil {
			return err
		}
		for c.dynamicBuf.Len() > 0 {
			node := c.dynamicBuf.Front()
			data := node.Value.([]byte)
			if node.Next() == nil {
				err = c.WriteToConn(data[:c.writeIndex])
				if err != nil {
					return err
				}
			} else {
				err = c.WriteToConn(data)
				if err != nil {
					return err
				}
			}
			c.allocator.Free(node.Value.([]byte))
			c.dynamicBuf.Remove(node)
		}
	}
	return err

}

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
	c.writeIndex = 0
	c.curHeader = c.fixBuf[:HeaderLengthOfTheProtocol]
	c.curBuf = c.fixBuf
	for node := c.dynamicBuf.Front(); node != nil; node = node.Next() {
		c.allocator.Free(node.Value.([]byte))
	}
	c.dynamicBuf.Init()

}
