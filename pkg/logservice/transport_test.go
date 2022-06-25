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
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type testConn struct {
	buf bytes.Buffer
}

var _ net.Conn = (*testConn)(nil)

func newTestConn() *testConn {
	return &testConn{}
}

func (t *testConn) Read(data []byte) (int, error) {
	return t.buf.Read(data)
}

func (t *testConn) Write(data []byte) (int, error) {
	return t.buf.Write(data)
}

func (t *testConn) Close() error {
	return nil
}

func (t *testConn) LocalAddr() net.Addr {
	panic("not implemented")
}

func (t *testConn) RemoteAddr() net.Addr {
	panic("not implemented")
}

func (t *testConn) SetDeadline(tt time.Time) error {
	return nil
}

func (t *testConn) SetReadDeadline(tt time.Time) error {
	return nil
}

func (t *testConn) SetWriteDeadline(tt time.Time) error {
	return nil
}

func TestSendPoisonAndAck(t *testing.T) {
	conn := newTestConn()
	assert.NoError(t, sendPoison(conn, poisonNumber[:]))
	data := conn.buf.Bytes()
	assert.Equal(t, poisonNumber[:], data)

	conn = newTestConn()
	assert.NoError(t, sendPoisonAck(conn, poisonNumber[:]))
	data = conn.buf.Bytes()
	assert.Equal(t, poisonNumber[:], data)
}

func TestReadSize(t *testing.T) {
	conn := newTestConn()
	szbuf := make([]byte, 4)
	sz := uint32(12345)
	binaryEnc.PutUint32(szbuf, sz)
	_, err := conn.Write(szbuf)
	assert.NoError(t, err)

	result, err := readSize(conn, nil)
	assert.Equal(t, int(sz), result)
	assert.NoError(t, err)
}

func TestWriteRequest(t *testing.T) {
	conn := newTestConn()
	req := pb.Request{
		Method:      pb.APPEND,
		Name:        "test",
		DNID:        1234567890,
		PayloadSize: 32,
	}
	payload := make([]byte, 32)
	rand.Read(payload)
	err := writeRequest(conn, req, nil, payload)
	assert.NoError(t, err)

	result := make([]byte, req.Size()+4+len(magicNumber[:]))
	n, err := conn.Read(result)
	assert.NoError(t, err)
	assert.Equal(t, req.Size()+4+len(magicNumber[:]), n)

	assert.Equal(t, magicNumber[:], result[:len(magicNumber[:])])
	sz := binaryEnc.Uint32(result[len(magicNumber[:]):])
	assert.Equal(t, int(sz), req.Size())

	data := MustMarshal(&req)
	assert.Equal(t, data, result[len(magicNumber[:])+4:])
}

func TestReadRequest(t *testing.T) {
	conn := newTestConn()
	req := pb.Request{
		Method:      pb.APPEND,
		Name:        "test",
		PayloadSize: 32,
	}
	payload := make([]byte, 32)
	rand.Read(payload)
	err := writeRequest(conn, req, nil, payload)
	assert.NoError(t, err)

	mn := make([]byte, len(magicNumber[:]))
	_, err = conn.Read(mn)
	assert.NoError(t, err)
	assert.Equal(t, magicNumber[:], mn)

	reqResult, payloadResult, err := readRequest(conn, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, req, reqResult)
	assert.Equal(t, payload, payloadResult)
}

func TestWriteResponse(t *testing.T) {
	testWriteResponse(t, recvBufSize)
	testWriteResponse(t, recvBufSize-1)
	testWriteResponse(t, recvBufSize+1)
	testWriteResponse(t, recvBufSize*3)
	testWriteResponse(t, recvBufSize*3-1)
	testWriteResponse(t, recvBufSize*3+1)
}

func testWriteResponse(t *testing.T, sz int) {
	conn := newTestConn()
	resp := pb.Response{
		Method:    pb.APPEND,
		ShardID:   1234567890,
		LastIndex: 234567890,
	}
	assert.NoError(t, writeResponse(conn, resp, nil, nil))

	szbuf := make([]byte, 4)
	_, err := conn.Read(szbuf)
	assert.NoError(t, err)
	assert.Equal(t, resp.Size(), int(binaryEnc.Uint32(szbuf)))
	data := make([]byte, binaryEnc.Uint32(szbuf))
	_, err = conn.Read(data)
	assert.NoError(t, err)
	var respResult pb.Response
	assert.NoError(t, respResult.Unmarshal(data))
	assert.Equal(t, resp, respResult)

	records := pb.LogRecordResponse{
		Records: []pb.LogRecord{
			{Data: make([]byte, sz)},
		},
	}
	rand.Read(records.Records[0].Data)
	conn = newTestConn()
	recs := MustMarshal(&records)
	resp.PayloadSize = uint64(len(recs))
	assert.NoError(t, writeResponse(conn, resp, recs, nil))

	ignored := make([]byte, 4+resp.Size())
	_, err = conn.Read(ignored)
	assert.NoError(t, err)

	data = make([]byte, resp.PayloadSize)
	_, err = conn.Read(data)
	assert.NoError(t, err)
	recordsResult := pb.LogRecordResponse{}
	assert.NoError(t, recordsResult.Unmarshal(data))
	assert.Equal(t, records, recordsResult)
}

func TestReadResponse(t *testing.T) {
	testReadResponse(t, recvBufSize-1)
	testReadResponse(t, recvBufSize+1)
	testReadResponse(t, recvBufSize*3)
	testReadResponse(t, recvBufSize*3-1)
	testReadResponse(t, recvBufSize*3+1)
}

func testReadResponse(t *testing.T, sz int) {
	conn := newTestConn()
	resp := pb.Response{
		Method:    pb.APPEND,
		ShardID:   1234567890,
		LastIndex: 234567890,
	}
	assert.NoError(t, writeResponse(conn, resp, nil, nil))
	respResult, recordsResult, err := readResponse(conn, nil)
	assert.NoError(t, err)
	assert.Equal(t, resp, respResult)
	assert.Equal(t, pb.LogRecordResponse{}, recordsResult)

	records := pb.LogRecordResponse{
		Records: []pb.LogRecord{
			{Data: make([]byte, sz)},
		},
	}
	recs := MustMarshal(&records)
	resp.PayloadSize = uint64(len(recs))
	assert.NoError(t, writeResponse(conn, resp, recs, nil))
	respResult, recordsResult, err = readResponse(conn, nil)
	assert.NoError(t, err)
	assert.Equal(t, resp, respResult)
	assert.Equal(t, records, recordsResult)
}

func TestReadMagicNumber(t *testing.T) {
	conn := newTestConn()
	_, err := conn.Write([]byte{0, 0})
	assert.NoError(t, err)
	assert.Equal(t, errBadMessage, readMagicNumber(conn, nil))

	conn = newTestConn()
	_, err = conn.Write(magicNumber[:])
	assert.NoError(t, err)
	assert.NoError(t, readMagicNumber(conn, nil))

	conn = newTestConn()
	_, err = conn.Write(poisonNumber[:])
	assert.NoError(t, err)
	assert.Equal(t, errPoisonReceived, readMagicNumber(conn, nil))
}

func TestGetTimeoutFromContext(t *testing.T) {
	_, err := getTimeoutFromContext(context.Background())
	assert.Equal(t, ErrDeadlineNotSet, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	d, err := getTimeoutFromContext(ctx)
	assert.NoError(t, err)
	assert.True(t, d > 55*time.Second)
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel1()
	time.Sleep(time.Millisecond)
	_, err = getTimeoutFromContext(ctx1)
	assert.Equal(t, ErrInvalidDeadline, err)
}
