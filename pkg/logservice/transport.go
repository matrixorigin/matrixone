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
	"github.com/matrixorigin/matrixone/pkg/logservice/pb/rpc"
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

func waitPoisonAck(conn net.Conn) {
	ack := make([]byte, len(poisonNumber))
	tt := time.Now().Add(keepAlivePeriod)
	if err := conn.SetReadDeadline(tt); err != nil {
		return
	}
	if _, err := io.ReadFull(conn, ack); err != nil {
		// TODO: log the error
		return
	}
}

func readSize(conn net.Conn, buf []byte) (int, error) {
	if len(buf) < 4 {
		buf = make([]byte, 4)
	}
	szbuf := buf[4:]
	tt := time.Now().Add(requestReadDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return 0, err
	}
	if _, err := io.ReadFull(conn, szbuf); err != nil {
		return 0, err
	}
	return int(binaryEnc.Uint32(szbuf)), nil
}

func writeRequest(conn net.Conn,
	req rpc.Request, buf []byte, payload []byte) error {
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
	tt = time.Now().Add(writeDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := conn.Write(payload); err != nil {
		return err
	}
	return nil
}

func readRequest(conn net.Conn, buf []byte, payload []byte) (rpc.Request, []byte, error) {
	size, err := readSize(conn, buf)
	if err != nil {
		return rpc.Request{}, nil, err
	}
	if len(buf) < size {
		buf = make([]byte, size)
	}
	buf = buf[:size]
	if _, err := io.ReadFull(conn, buf); err != nil {
		return rpc.Request{}, nil, err
	}
	var req rpc.Request
	if err := req.Unmarshal(buf); err != nil {
		panic(err)
	}

	if uint64(len(payload)) < req.PayloadSize {
		payload = make([]byte, req.PayloadSize)
	}
	tt := time.Now().Add(readDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return rpc.Request{}, nil, err
	}
	if _, err := io.ReadFull(conn, payload); err != nil {
		return rpc.Request{}, nil, err
	}
	return req, payload, nil
}

func writeResponse(conn net.Conn, resp rpc.Response,
	records rpc.LogRecordResponse, buf []byte) error {
	if resp.Size() < len(buf) {
		buf = make([]byte, resp.Size()+4)
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

	if len(records.Records) > 0 {
		data, err := records.Marshal()
		if err != nil {
			panic(err)
		}
		binaryEnc.PutUint32(buf, uint32(len(data)))
		if _, err := conn.Write(buf); err != nil {
			return err
		}
		sent := 0
		bufSize := int(recvBufSize)
		for sent < len(buf) {
			if sent+bufSize > len(data) {
				bufSize = len(buf) - sent
			}
			tt = time.Now().Add(writeDuration)
			if err := conn.SetWriteDeadline(tt); err != nil {
				return err
			}
			if _, err := conn.Write(data[sent : sent+bufSize]); err != nil {
				return err
			}
			sent += bufSize
		}
	} else {
		binaryEnc.PutUint32(buf, 0)
		if _, err := conn.Write(buf); err != nil {
			return err
		}
	}

	return nil
}

func readResponse(conn net.Conn,
	buf []byte) (rpc.Response, rpc.LogRecordResponse, error) {
	size, err := readSize(conn, buf)
	if err != nil {
		return rpc.Response{}, rpc.LogRecordResponse{}, err
	}

	if len(buf) < size {
		buf = make([]byte, size)
	}
	buf = buf[:size]

	tt := time.Now().Add(responseReadDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return rpc.Response{}, rpc.LogRecordResponse{}, err
	}
	if _, err := io.ReadFull(conn, buf); err != nil {
		return rpc.Response{}, rpc.LogRecordResponse{}, err
	}
	var resp rpc.Response
	if err := resp.Unmarshal(buf); err != nil {
		panic(err)
	}

	size, err = readSize(conn, buf)
	if err != nil {
		return rpc.Response{}, rpc.LogRecordResponse{}, err
	}

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
			return rpc.Response{}, rpc.LogRecordResponse{}, err
		}
		if _, err := io.ReadFull(conn, recvBuf); err != nil {
			return rpc.Response{}, rpc.LogRecordResponse{}, err
		}
		toRead -= len(recvBuf)
		received += uint64(len(recvBuf))
		if toRead < recvBufSize {
			recvBuf = buf[received : received+uint64(toRead)]
		} else {
			recvBuf = buf[received : received+uint64(recvBufSize)]
		}
	}

	var logs rpc.LogRecordResponse
	if err := logs.Unmarshal(buf); err != nil {
		panic(err)
	}

	return resp, logs, nil
}

func readMagicNumber(conn net.Conn, buf []byte) error {
	tt := time.Now().Add(requestReadDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return err
	}
	if len(buf) > len(magicNumber[:]) {
		buf = buf[:len(magicNumber[:])]
	}
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
