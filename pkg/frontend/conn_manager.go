package frontend

import (
	"container/list"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"net"
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

	header     []byte // buf data, auto +/- size
	fixBuf     []byte
	dynamicBuf *list.List
	fixIndex   int
	cutIndex   int
	length     int

	allocator Allocator
}

// NewIOSession create a new io session
func NewIOSession(conn net.Conn, allocator Allocator) *ConnManager {
	bio := &ConnManager{
		conn:       conn,
		localAddr:  conn.RemoteAddr().String(),
		remoteAddr: conn.LocalAddr().String(),
		connected:  true,
		dynamicBuf: list.New(),
		allocator:  allocator,
	}
	bio.header = bio.allocator.Alloc(HeaderLengthOfTheProtocol)
	bio.fixBuf = bio.allocator.Alloc(fixBufferSize)

	return bio
}

func (bio *ConnManager) ID() uint64 {
	return bio.id
}

func (bio *ConnManager) RawConn() net.Conn {
	return bio.conn
}

func (bio *ConnManager) UseConn(conn net.Conn) {
	bio.conn = conn
}
func (bio *ConnManager) Disconnect() error {
	return bio.closeConn()
}

func (bio *ConnManager) Close() error {
	err := bio.closeConn()
	if err != nil {
		return err
	}
	bio.connected = false

	bio.allocator.Free(bio.header)
	bio.allocator.Free(bio.fixBuf)

	for e := bio.dynamicBuf.Front(); e != nil; e = e.Next() {
		bio.allocator.Free(e.Value.([]byte))
	}
	getGlobalRtMgr().Closed(bio)
	return nil
}

func (bio *ConnManager) Read() ([]byte, error) {
	payloads := make([][]byte, 0)
	defer func() {
		for _, payload := range payloads {
			bio.allocator.Free(payload)
		}
	}()
	var err error
	for {
		if !bio.connected {
			return nil, moerr.NewInternalError(moerr.Context(), "The IOSession connection has been closed")
		}
		var packetLength int
		err = bio.ReadBytes(bio.header, HeaderLengthOfTheProtocol)
		if err != nil {
			return nil, err
		}
		packetLength = int(uint32(bio.header[0]) | uint32(bio.header[1])<<8 | uint32(bio.header[2])<<16)
		sequenceId := bio.header[3]
		bio.sequenceId = sequenceId + 1

		if packetLength == 0 {
			break
		}

		payload, err := bio.ReadOnePayload(packetLength)
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
			break
		}
	}

	totalLength := 0
	for _, payload := range payloads {
		totalLength += len(payload)
	}
	//TODO: check life cycle
	finalPayload := bio.allocator.Alloc(totalLength)

	copyIndex := 0
	for _, payload := range payloads {
		copy(finalPayload[copyIndex:], payload)
		bio.allocator.Free(payload)
		copyIndex += len(payload)
	}
	return finalPayload, nil
}

func (bio *ConnManager) ReadOnePayload(packetLength int) ([]byte, error) {
	var buf []byte
	var err error

	if packetLength > fixBufferSize {
		buf = bio.allocator.Alloc(packetLength)
		defer bio.allocator.Free(buf)
	} else {
		buf = bio.fixBuf
	}
	err = bio.ReadBytes(buf[:packetLength], packetLength)
	if err != nil {
		return nil, err
	}

	payload := bio.allocator.Alloc(packetLength)
	copy(payload, buf[:packetLength])
	return payload, nil
}

func (bio *ConnManager) ReadBytes(buf []byte, Length int) error {
	var err error
	var n int
	var readLength int
	for {
		n, err = bio.conn.Read(buf[readLength:])
		if err != nil {
			return err
		}
		readLength += n

		if readLength == Length {
			return nil
		}
	}
}

func (bio *ConnManager) Append(elems ...byte) {
	remainFixBuf := fixBufferSize - bio.fixIndex
	if len(elems) > remainFixBuf {
		buf := bio.allocator.Alloc(len(elems))
		copy(buf, elems)
		bio.dynamicBuf.PushBack(buf)
	} else {
		copy(bio.fixBuf[bio.fixIndex:], elems)
		bio.fixIndex += len(elems)
	}
	bio.length += len(elems)
	return
}

func (bio *ConnManager) Flush() error {
	if bio.length == 0 {
		return nil
	}
	defer bio.Reset()
	var err error
	dataLength := bio.length
	for dataLength > int(MaxPayloadSize) {
		err = bio.FlushLength(int(MaxPayloadSize))
		if err != nil {
			return err
		}
		dataLength -= int(MaxPayloadSize)
	}

	err = bio.FlushLength(dataLength)

	if err != nil {
		return err
	}

	bio.dynamicBuf.Init()

	return err

}

func (bio *ConnManager) FlushLength(length int) error {
	var header [4]byte
	var err error
	binary.LittleEndian.PutUint32(header[:], uint32(length))
	header[3] = bio.sequenceId
	bio.sequenceId += 1

	_, err = bio.conn.Write(header[:])
	if err != nil {
		return err
	}

	if bio.fixIndex != 0 {
		_, err = bio.conn.Write(bio.fixBuf[:bio.fixIndex])

		if err != nil {
			return err
		}
		length -= bio.fixIndex
		bio.length -= bio.fixIndex
		bio.fixIndex = 0

	}

	for length != 0 {
		node := bio.dynamicBuf.Front()
		data := node.Value.([]byte)
		nodeLength := len(data) - bio.cutIndex
		if nodeLength > length {
			_, err = bio.conn.Write(data[bio.cutIndex : bio.cutIndex+length])

			if err != nil {
				return err
			}

			bio.cutIndex += length
			bio.length -= length
			length = 0

		} else {
			_, err = bio.conn.Write(data[bio.cutIndex:])

			if err != nil {
				return err
			}

			length -= nodeLength
			bio.length -= nodeLength
			bio.dynamicBuf.Remove(node)
			bio.allocator.Free(data)
			bio.cutIndex = 0
		}
	}

	return nil
}

func (bio *ConnManager) Write(payload []byte) error {
	defer bio.Reset()
	if !bio.connected {
		return moerr.NewInternalError(moerr.Context(), "The IOSession connection has been closed")
	}
	var err error
	var header [4]byte

	length := len(payload)
	binary.LittleEndian.PutUint32(header[:], uint32(length))
	header[3] = bio.sequenceId
	bio.sequenceId += 1

	_, err = bio.conn.Write(header[:])
	if err != nil {
		return err
	}

	_, err = bio.conn.Write(payload)
	if err != nil {
		return err
	}
	return nil
}

func (bio *ConnManager) RemoteAddress() string {
	return bio.remoteAddr
}

func (bio *ConnManager) closeConn() error {
	if bio.conn != nil {
		if err := bio.conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (bio *ConnManager) Reset() {
	bio.length = 0
	bio.fixIndex = 0
	bio.cutIndex = 0
	for node := bio.dynamicBuf.Front(); node != nil; node = node.Next() {
		bio.allocator.Free(node.Value.([]byte))
	}
	bio.dynamicBuf.Init()
}
