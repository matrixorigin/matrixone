package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
	"io"
	"net"
	"time"
)

// WriteOptions write options
type WriteOptions struct {
	// Timeout deadline for write
	Timeout time.Duration
	// Flush flush data to net.Conn
	Flush bool
}

// ReadOptions read options
type ReadOptions struct {
	// Timeout deadline for read
	Timeout time.Duration
}

type IOSession interface {
	ID() uint64

	Write(msg any, options WriteOptions) error

	Read(option ReadOptions) (any, error)

	Flush(timeout time.Duration) error

	OutBuf() *ByteBuf

	RemoteAddress() string

	Disconnect() error
}

type baseIO struct {
	id                    uint64
	conn                  net.Conn
	localAddr, remoteAddr string
	connected             bool
	in                    *ByteBuf
	out                   *ByteBuf
	disableConnect        bool
	logger                *zap.Logger
	readCopyBuf           []byte
	writeCopyBuf          []byte
	codec                 Codec
	allocator             Allocator
}

// NewIOSession create a new io session
func NewIOSession(conn net.Conn, allocator Allocator) *baseIO {
	bio := &baseIO{
		conn:           conn,
		localAddr:      conn.RemoteAddr().String(),
		remoteAddr:     conn.LocalAddr().String(),
		connected:      true,
		in:             nil,
		out:            nil,
		disableConnect: false,
		logger:         logutil.GetGlobalLogger(),
		readCopyBuf:    make([]byte, DefaultRpcBufferSize),
		writeCopyBuf:   make([]byte, DefaultRpcBufferSize),
		codec:          NewMysqlCodec(),
		allocator:      allocator,
	}

	return bio
}

func (bio *baseIO) ID() uint64 {
	return bio.id
}

func (bio *baseIO) RawConn() net.Conn {
	return bio.conn
}

func (bio *baseIO) UseConn(conn net.Conn) {
	bio.conn = conn
}
func (bio *baseIO) Disconnect() error {
	return bio.closeConn()
}

func (bio *baseIO) Close() error {
	err := bio.closeConn()
	if err != nil {
		return err
	}
	bio.connected = false

	if bio.out != nil {
		bio.out.Close()
	}
	if bio.in != nil {
		bio.in.Close()
	}

	getGlobalRtMgr().Closed(bio)
	bio.logger.Debug("IOSession closed")
	return nil
}

func (bio *baseIO) Read(options ReadOptions) (any, error) {
	for {
		if !bio.connected {
			return nil, moerr.NewInternalError(moerr.Context(), "The IOSession connection has been closed")
		}

		var msg any
		var err error
		var complete bool
		for {
			if bio.in.Readable() > 0 {
				msg, complete, err = bio.codec.Decode(bio.in)
				if !complete && err == nil {
					msg, complete, err = bio.readFromConn(options.Timeout)
				}
			} else {
				msg, complete, err = bio.readFromConn(options.Timeout)
			}

			if nil != err {
				bio.in.Reset()
				return nil, err
			}

			if complete {
				return msg, nil
			}
		}
	}
}

func (bio *baseIO) Write(msg any, options WriteOptions) error {
	if !bio.connected {
		return moerr.NewInternalError(moerr.Context(), "The IOSession connection has been closed")
	}

	err := bio.codec.Encode(msg, bio.out, bio.conn)
	if err != nil {
		return err
	}

	if bio.out.Readable() > 0 {
		err = bio.Flush(0)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bio *baseIO) Flush(timeout time.Duration) error {
	defer bio.out.Reset()
	if !bio.connected {
		return moerr.NewInternalError(moerr.Context(), "The IOSession connection has been closed")
	}

	if timeout != 0 {
		bio.conn.SetWriteDeadline(time.Now().Add(timeout))
	} else {
		bio.conn.SetWriteDeadline(time.Time{})
	}

	_, err := io.CopyBuffer(bio.conn, bio.out, bio.writeCopyBuf)
	if err == nil || err == io.EOF {
		return nil
	}
	return err
}

func (bio *baseIO) RemoteAddress() string {
	return bio.remoteAddr
}

func (bio *baseIO) OutBuf() *ByteBuf {
	return bio.out
}

func (bio *baseIO) InBuf() *ByteBuf {
	return bio.in
}

func (bio *baseIO) readFromConn(timeout time.Duration) (any, bool, error) {
	if timeout != 0 {
		bio.conn.SetReadDeadline(time.Now().Add(timeout))
	} else {
		bio.conn.SetReadDeadline(time.Time{})
	}

	n, err := io.CopyBuffer(bio.in, bio.conn, bio.readCopyBuf)
	if err != nil {
		return nil, false, err
	}
	if n == 0 {
		return nil, false, io.EOF
	}
	return bio.codec.Decode(bio.in)
}

func (bio *baseIO) closeConn() error {
	if bio.conn != nil {
		if err := bio.conn.Close(); err != nil {
			bio.logger.Error("close conneciton failed",
				zap.Error(err))
			return err
		}
		bio.logger.Debug("conneciton disconnected")
	}
	return nil
}
