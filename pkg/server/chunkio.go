package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/pingcap/parser/mysql"
)

const defaultWriterSize = 16 * 1024

// packetIO is a helper to read and write data in packet format.
type packetIO struct {
	bufReader   *bufferedConnReader
	bufWriter   *bufio.Writer
	sequence    uint8
	readTimeout time.Duration
}

func newPacketIO(bufReader *bufferedConnReader) *packetIO {
	p := &packetIO{sequence: 0}
	p.setBufReader(bufReader)
	return p
}

func (p *packetIO) setBufReader(bufReader *bufferedConnReader) {
	p.bufReader = bufReader
	p.bufWriter = bufio.NewWriterSize(bufReader, defaultWriterSize)
}

func (p *packetIO) setReadTimeout(timeout time.Duration) {
	p.readTimeout = timeout
}

func (p *packetIO) readOne() ([]byte, error) {
	var header [4]byte
	if p.readTimeout > 0 {
		if err := p.bufReader.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			return nil, err
		}
	}
	if _, err := io.ReadFull(p.bufReader, header[:]); err != nil {
		return nil, err
	}

	sequence := header[3]
	if sequence != p.sequence {
		return nil, errors.New(fmt.Sprintf("invalid sequence %d != %d", sequence, p.sequence))
	}

	p.sequence++

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

	data := make([]byte, length)
	if p.readTimeout > 0 {
		if err := p.bufReader.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			return nil, err
		}
	}
	if _, err := io.ReadFull(p.bufReader, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (p *packetIO) readFull() ([]byte, error) {
	data, err := p.readOne()
	if err != nil {
		return nil, err
	}

	if len(data) < mysql.MaxPayloadLen {
		return data, nil
	}

	// handle multi-chunk
	for {
		buf, err := p.readOne()
		if err != nil {
			return nil, err
		}

		data = append(data, buf...)

		if len(buf) < mysql.MaxPayloadLen {
			break
		}
	}

	return data, nil
}

// write writes data that already have header
func (p *packetIO) write(data []byte) error {
	length := len(data) - 4

	for length >= mysql.MaxPayloadLen {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = p.sequence

		if n, err := p.bufWriter.Write(data[:4+mysql.MaxPayloadLen]); err != nil {
			return mysql.ErrBadConn
		} else if n != (4 + mysql.MaxPayloadLen) {
			return mysql.ErrBadConn
		} else {
			p.sequence++
			length -= mysql.MaxPayloadLen
			data = data[mysql.MaxPayloadLen:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.sequence

	if n, err := p.bufWriter.Write(data); err != nil {
		return mysql.ErrBadConn
	} else if n != len(data) {
		return mysql.ErrBadConn
	} else {
		p.sequence++
		return nil
	}
}

func (p *packetIO) flush() error {
	err := p.bufWriter.Flush()
	if err != nil {
		return err
	}
	return err
}
