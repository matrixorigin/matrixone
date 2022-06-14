// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"bytes"
	"context"
	"io"
	"net"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

var (
	ErrInvalidBuffer   = moerr.NewError(moerr.INVALID_STATE, "invalid buffer size")
	ErrBadMessage      = moerr.NewError(moerr.INVALID_INPUT, "invalid message")
	errPoisonReceived  = moerr.NewError(moerr.INVALID_STATE, "poison received")
	ErrDeadlineNotSet  = moerr.NewError(moerr.INVALID_INPUT, "deadline not set")
	ErrInvalidDeadline = moerr.NewError(moerr.INVALID_INPUT, "invalid deadline")

	magicNumber          = [2]byte{0x2A, 0xBD}
	poisonNumber         = [2]byte{0x2B, 0xBE}
	requestSendDuration  = 2 * time.Second
	requestReadDuration  = 2 * time.Second
	responseSendDuration = 2 * time.Second
	responseReadDuration = 2 * time.Second
	writeDuration        = 10 * time.Second
	readDuration         = 10 * time.Second
	keepAlivePeriod      = 5 * time.Second

	reqBufSize  = 2 * 1024
	recvBufSize = 512 * 1024
)

func sendPoison(conn net.Conn, poison []byte) error {
	tt := time.Now().Add(requestSendDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := conn.Write(poison); err != nil {
		return err
	}
	return nil
}

func sendPoisonAck(conn net.Conn, poisonAck []byte) error {
	return sendPoison(conn, poisonAck)
}

func waitPoisonAck(conn net.Conn) error {
	ack := make([]byte, len(poisonNumber))
	tt := time.Now().Add(keepAlivePeriod)
	if err := conn.SetReadDeadline(tt); err != nil {
		return err
	}
	if _, err := io.ReadFull(conn, ack); err != nil {
		return err
	}
	return nil
}

func readSize(conn net.Conn, buf []byte) (int, error) {
	if len(buf) < 4 {
		buf = make([]byte, 4)
	}
	szbuf := buf[:4]
	tt := time.Now().Add(requestReadDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return 0, err
	}
	if _, err := io.ReadFull(conn, szbuf); err != nil {
		return 0, err
	}
	return int(binaryEnc.Uint32(szbuf)), nil
}

// FIXME: add data corruption check
func writeRequest(conn net.Conn,
	req pb.Request, buf []byte, payload []byte) error {
	if len(buf) < req.Size()+4+len(magicNumber[:]) {
		buf = make([]byte, req.Size()+4+len(magicNumber[:]))
	}
	n, err := req.MarshalTo(buf[4+len(magicNumber[:]):])
	if err != nil {
		panic(err)
	}

	copy(buf, magicNumber[:])
	binaryEnc.PutUint32(buf[len(magicNumber[:]):], uint32(n))
	buf = buf[:n+4+len(magicNumber[:])]

	tt := time.Now().Add(requestSendDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	if len(payload) > 0 {
		tt = time.Now().Add(writeDuration)
		if err := conn.SetWriteDeadline(tt); err != nil {
			return err
		}
		if _, err := conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func readRequest(conn net.Conn,
	buf []byte, payload []byte) (pb.Request, []byte, error) {
	size, err := readSize(conn, buf)
	if err != nil {
		return pb.Request{}, nil, err
	}
	if len(buf) < size {
		buf = make([]byte, size)
	}
	buf = buf[:size]
	if _, err := io.ReadFull(conn, buf); err != nil {
		return pb.Request{}, nil, err
	}
	var req pb.Request
	MustUnmarshal(&req, buf)
	if req.PayloadSize > 0 {
		if uint64(len(payload)) < req.PayloadSize {
			payload = make([]byte, req.PayloadSize)
		}
		payload = payload[:req.PayloadSize]
		tt := time.Now().Add(readDuration)
		if err := conn.SetWriteDeadline(tt); err != nil {
			return pb.Request{}, nil, err
		}
		if _, err := io.ReadFull(conn, payload); err != nil {
			return pb.Request{}, nil, err
		}
	}
	return req, payload, nil
}

func writeResponse(conn net.Conn, resp pb.Response,
	records []byte, buf []byte) error {
	if len(buf) < resp.Size()+4 {
		buf = make([]byte, resp.Size()+4)
	}
	if resp.PayloadSize != uint64(len(records)) {
		panic("Payload size not set")
	}

	n, err := resp.MarshalTo(buf[4:])
	if err != nil {
		panic(err)
	}
	buf = buf[:n+4]
	binaryEnc.PutUint32(buf, uint32(n))

	tt := time.Now().Add(responseSendDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := conn.Write(buf); err != nil {
		return err
	}

	if len(records) > 0 {
		sent := 0
		bufSize := int(recvBufSize)
		for sent < len(records) {
			if sent+bufSize > len(records) {
				bufSize = len(records) - sent
			}
			tt = time.Now().Add(writeDuration)
			if err := conn.SetWriteDeadline(tt); err != nil {
				return err
			}
			if _, err := conn.Write(records[sent : sent+bufSize]); err != nil {
				return err
			}
			sent += bufSize
		}
	}

	return nil
}

func readResponse(conn net.Conn,
	buf []byte) (pb.Response, pb.LogRecordResponse, error) {
	size, err := readSize(conn, buf)
	if err != nil {
		return pb.Response{}, pb.LogRecordResponse{}, err
	}

	if len(buf) < size {
		buf = make([]byte, size)
	}
	buf = buf[:size]

	tt := time.Now().Add(responseReadDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return pb.Response{}, pb.LogRecordResponse{}, err
	}
	if _, err := io.ReadFull(conn, buf); err != nil {
		return pb.Response{}, pb.LogRecordResponse{}, err
	}
	var resp pb.Response
	MustUnmarshal(&resp, buf)
	size = int(resp.PayloadSize)
	if size > len(buf) {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	received := uint64(0)
	var recvBuf []byte
	if size < recvBufSize {
		recvBuf = buf[:size]
	} else {
		recvBuf = buf[:recvBufSize]
	}
	toRead := size
	for toRead > 0 {
		tt = time.Now().Add(readDuration)
		if err := conn.SetReadDeadline(tt); err != nil {
			return pb.Response{}, pb.LogRecordResponse{}, err
		}
		if _, err := io.ReadFull(conn, recvBuf); err != nil {
			return pb.Response{}, pb.LogRecordResponse{}, err
		}
		toRead -= len(recvBuf)
		received += uint64(len(recvBuf))
		if toRead < recvBufSize {
			recvBuf = buf[received : received+uint64(toRead)]
		} else {
			recvBuf = buf[received : received+uint64(recvBufSize)]
		}
	}

	var logs pb.LogRecordResponse
	MustUnmarshal(&logs, buf)

	return resp, logs, nil
}

func readMagicNumber(conn net.Conn, buf []byte) error {
	tt := time.Now().Add(requestReadDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return err
	}
	if len(buf) < len(magicNumber[:]) {
		buf = make([]byte, len(magicNumber[:]))
	}
	buf = buf[:len(magicNumber[:])]
	if _, err := io.ReadFull(conn, buf); err != nil {
		return err
	}
	if bytes.Equal(buf, poisonNumber[:]) {
		return errPoisonReceived
	}
	if !bytes.Equal(buf, magicNumber[:]) {
		return ErrBadMessage
	}
	return nil
}

func getConnection(ctx context.Context, target string) (net.Conn, error) {
	timeout, err := getTimeoutFromContext(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		return nil, err
	}
	tcpconn, ok := conn.(*net.TCPConn)
	if ok {
		if err := setTCPConn(tcpconn); err != nil {
			return nil, err
		}
	} else {
		panic("unexpected connection type")
	}
	return conn, nil
}

func setTCPConn(conn *net.TCPConn) error {
	if err := conn.SetLinger(0); err != nil {
		return err
	}
	if err := conn.SetKeepAlive(true); err != nil {
		return err
	}
	return conn.SetKeepAlivePeriod(keepAlivePeriod)
}

func getTimeoutFromContext(ctx context.Context) (time.Duration, error) {
	d, ok := ctx.Deadline()
	if !ok {
		return 0, ErrDeadlineNotSet
	}
	now := time.Now()
	if now.After(d) {
		return 0, ErrInvalidDeadline
	}
	return d.Sub(now), nil
}
