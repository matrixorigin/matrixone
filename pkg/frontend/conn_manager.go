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

type ConnManager struct {
	id                    uint64
	conn                  net.Conn
	localAddr, remoteAddr string
	connected             bool
	sequenceId            uint8
	header                []byte
	fixBuf                []byte
	dynamicBuf            *list.List
	fixIndex              int
	length                int
	preLength             int
	preLengths            *list.List
	maxBytesToFlush       int
	timeout               time.Duration
	allocator             Allocator
}

// NewIOSession create a new io session
func NewIOSession(conn net.Conn, pu *config.ParameterUnit) *ConnManager {

	bio := &ConnManager{
		conn:            conn,
		localAddr:       conn.RemoteAddr().String(),
		remoteAddr:      conn.LocalAddr().String(),
		connected:       true,
		dynamicBuf:      list.New(),
		preLengths:      list.New(),
		allocator:       NewSessionAllocator(pu),
		timeout:         pu.SV.SessionTimeout.Duration,
		maxBytesToFlush: int(pu.SV.MaxBytesInOutbufToFlush) * 1024,
	}
	bio.header = bio.allocator.Alloc(HeaderLengthOfTheProtocol)
	bio.fixBuf = bio.allocator.Alloc(fixBufferSize)

	return bio
}

func (cm *ConnManager) ID() uint64 {
	return cm.id
}

func (cm *ConnManager) RawConn() net.Conn {
	return cm.conn
}

func (cm *ConnManager) UseConn(conn net.Conn) {
	cm.conn = conn
}
func (cm *ConnManager) Disconnect() error {
	return cm.closeConn()
}

func (cm *ConnManager) Close() error {
	err := cm.closeConn()
	if err != nil {
		return err
	}
	cm.connected = false

	cm.allocator.Free(cm.header)
	cm.allocator.Free(cm.fixBuf)

	for e := cm.dynamicBuf.Front(); e != nil; e = e.Next() {
		cm.allocator.Free(e.Value.([]byte))
	}
	getGlobalRtMgr().Closed(cm)
	return nil
}

func (cm *ConnManager) Read() ([]byte, error) {
	payloads := make([][]byte, 0)
	defer func() {
		for _, payload := range payloads {
			cm.allocator.Free(payload)
		}
	}()
	var err error
	for {
		if !cm.connected {
			return nil, moerr.NewInternalError(moerr.Context(), "The IOSession connection has been closed")
		}
		var packetLength int
		err = cm.ReadBytes(cm.header, HeaderLengthOfTheProtocol)
		if err != nil {
			return nil, err
		}
		packetLength = int(uint32(cm.header[0]) | uint32(cm.header[1])<<8 | uint32(cm.header[2])<<16)
		sequenceId := cm.header[3]
		cm.sequenceId = sequenceId + 1

		if packetLength == 0 {
			break
		}

		payload, err := cm.ReadOnePayload(packetLength)
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
	finalPayload := cm.allocator.Alloc(totalLength)

	copyIndex := 0
	for _, payload := range payloads {
		copy(finalPayload[copyIndex:], payload)
		cm.allocator.Free(payload)
		copyIndex += len(payload)
	}
	return finalPayload, nil
}

func (cm *ConnManager) ReadOnePayload(packetLength int) ([]byte, error) {
	var buf []byte
	var err error

	if packetLength > fixBufferSize {
		buf = cm.allocator.Alloc(packetLength)
		defer cm.allocator.Free(buf)
	} else {
		buf = cm.fixBuf
	}
	err = cm.ReadBytes(buf[:packetLength], packetLength)
	if err != nil {
		return nil, err
	}

	payload := cm.allocator.Alloc(packetLength)
	copy(payload, buf[:packetLength])
	return payload, nil
}

func (cm *ConnManager) ReadBytes(buf []byte, Length int) error {
	var err error
	var n int
	var readLength int
	for {
		n, err = cm.ReadFromConn(buf[readLength:])
		if err != nil {
			return err
		}
		readLength += n

		if readLength == Length {
			return nil
		}
	}
}
func (cm *ConnManager) ReadFromConn(buf []byte) (int, error) {
	err := cm.conn.SetReadDeadline(time.Now().Add(cm.timeout))
	if err != nil {
		return 0, err
	}
	n, err := cm.conn.Read(buf)
	return n, err
}

func (cm *ConnManager) Append(elems ...byte) {
	cutIndex := 0
	for {
		remainPacketLength := int(MaxPayloadSize) - cm.length
		if len(elems[cutIndex:]) <= remainPacketLength {
			break
		}

		cm.AppendPart(elems[cutIndex:remainPacketLength])
		err := cm.FinishedPacket()
		if err != nil {
			panic(err)
		}
		cutIndex += remainPacketLength
	}

	cm.AppendPart(elems)
}

func (cm *ConnManager) AppendPart(elems []byte) {
	remainFixBuf := fixBufferSize - cm.fixIndex
	if len(elems) > remainFixBuf {
		buf := cm.allocator.Alloc(len(elems))
		copy(buf, elems)
		cm.dynamicBuf.PushBack(buf)
	} else {
		copy(cm.fixBuf[cm.fixIndex:], elems)
		cm.fixIndex += len(elems)
	}
	cm.length += len(elems)
	return
}

func (cm *ConnManager) FinishedPacket() error {
	rowLength := cm.length - cm.preLength
	cm.preLength += rowLength
	header := cm.allocator.Alloc(HeaderLengthOfTheProtocol)

	binary.LittleEndian.PutUint32(header[:], uint32(rowLength))
	header[3] = cm.sequenceId
	cm.sequenceId += 1

	cm.preLengths.PushBack(header)
	err := cm.Flush()

	if err != nil {
		return err
	}
	return nil
}

func (cm *ConnManager) FlushIfFull() error {
	var err error
	if cm.length >= cm.maxBytesToFlush {
		err = cm.Flush()
		if err != nil {
			return err
		}
	}
	return err
}

func (cm *ConnManager) Flush() error {
	var err error
	defer cm.Reset()
	for cm.preLengths.Len() > 0 {
		node := cm.preLengths.Front()
		header := node.Value.([]byte)
		packetLength := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16
		err = cm.WriteToConn(header)
		if err != nil {
			return err
		}
		cm.preLengths.Remove(node)
		cm.allocator.Free(node.Value.([]byte))

		err = cm.FlushLength(int(packetLength))
		if err != nil {
			return err
		}
	}
	return err

}

func (cm *ConnManager) FlushLength(length int) error {
	var err error

	if cm.fixIndex != 0 {
		err = cm.WriteToConn(cm.fixBuf[:cm.fixIndex])

		if err != nil {
			return err
		}
		length -= cm.fixIndex
		cm.length -= cm.fixIndex
		cm.fixIndex = 0

	}

	for length != 0 {
		node := cm.dynamicBuf.Front()
		data := node.Value.([]byte)
		nodeLength := len(data)

		err = cm.WriteToConn(data)

		if err != nil {
			return err
		}

		length -= nodeLength
		cm.length -= nodeLength
		cm.dynamicBuf.Remove(node)
		cm.allocator.Free(data)
	}

	return nil
}

func (cm *ConnManager) Write(payload []byte) error {
	defer cm.Reset()
	if !cm.connected {
		return moerr.NewInternalError(moerr.Context(), "The IOSession connection has been closed")
	}

	var err error

	err = cm.Flush()
	if err != nil {
		return err
	}

	var header [4]byte

	length := len(payload)
	binary.LittleEndian.PutUint32(header[:], uint32(length))
	header[3] = cm.sequenceId
	cm.sequenceId += 1

	err = cm.WriteToConn(header[:])
	if err != nil {
		return err
	}

	err = cm.WriteToConn(payload)
	if err != nil {
		return err
	}
	return nil
}

func (cm *ConnManager) WriteToConn(buf []byte) error {
	sendlength := 0
	for sendlength != len(buf) {
		n, err := cm.conn.Write(buf[sendlength:])
		sendlength += n
		if err != nil {
			return err
		}
	}
	return nil
}

func (cm *ConnManager) RemoteAddress() string {
	return cm.remoteAddr
}

func (cm *ConnManager) closeConn() error {
	if cm.conn != nil {
		if err := cm.conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (cm *ConnManager) Reset() {
	cm.length = 0
	cm.fixIndex = 0
	cm.preLength = 0

	for node := cm.dynamicBuf.Front(); node != nil; node = node.Next() {
		cm.allocator.Free(node.Value.([]byte))
	}
	cm.dynamicBuf.Init()

	for node := cm.preLengths.Front(); node != nil; node = node.Next() {
		cm.allocator.Free(node.Value.([]byte))
	}
	cm.preLengths.Init()
}
