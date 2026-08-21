// Copyright 2026 Matrix Origin
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

package external

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	datastream "github.com/matrixorigin/matrixone/pkg/datastream/v1"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// fakeDataStreamServer streams a scripted sequence of responses.
type fakeDataStreamServer struct {
	datastream.UnimplementedDataStreamServer
	script      func(req *datastream.ReadRequest) []*datastream.ReadResponse
	lastRequest *datastream.ReadRequest
}

func (s *fakeDataStreamServer) Read(req *datastream.ReadRequest, stream grpc.ServerStreamingServer[datastream.ReadResponse]) error {
	s.lastRequest = req
	for _, resp := range s.script(req) {
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func chunk(data string) *datastream.ReadResponse {
	return &datastream.ReadResponse{Payload: &datastream.ReadResponse_Chunk{
		Chunk: &datastream.Chunk{Data: []byte(data)},
	}}
}

func streamErr(code datastream.ErrorCode, msg string) *datastream.ReadResponse {
	return &datastream.ReadResponse{Payload: &datastream.ReadResponse_Error{
		Error: &datastream.Error{Code: code, Message: msg},
	}}
}

func startFakeServer(t *testing.T, fake *fakeDataStreamServer) (host string, port int32) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	datastream.RegisterDataStreamServer(server, fake)
	go server.Serve(lis)
	t.Cleanup(server.Stop)
	addr := lis.Addr().(*net.TCPAddr)
	return "127.0.0.1", int32(addr.Port)
}

func newDatastreamTestParam(t *testing.T, host string, port int32, filter string) (*ExternalParam, *process.Process, *batch.Batch) {
	t.Helper()
	proc := testutil.NewProc(t)
	cols := []*plan.ColDef{
		{Name: "a", Typ: plan.Type{Id: int32(types.T_int32)}},
		{Name: "s", Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
	}
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Attrs: []plan.ExternAttr{
				{ColName: "a", ColIndex: 0, ColFieldIndex: 0},
				{ColName: "s", ColIndex: 1, ColFieldIndex: 1},
			},
			Cols:   cols,
			Extern: DatastreamExternParam(),
			DatastreamScan: &plan.DataStreamScan{
				Server:       host,
				Port:         port,
				Table:        "src",
				Recheck:      true,
				PushedFilter: filter,
			},
			maxBatchSize: 1 << 20,
		},
		ExParam: ExParam{
			Fileparam: &ExFileparam{FileCnt: 1},
			Filter:    &FilterParam{},
		},
	}
	bat := batch.NewOffHeap([]string{"a", "s"})
	for i := range cols {
		bat.Vecs[i] = vector.NewOffHeapVecWithType(makeType(&cols[i].Typ, false))
	}
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	return param, proc, bat
}

func TestDataStreamReaderHappyPath(t *testing.T) {
	fake := &fakeDataStreamServer{script: func(*datastream.ReadRequest) []*datastream.ReadResponse {
		return []*datastream.ReadResponse{
			chunk("1,foo\n2,\"with,comma\"\n"),
			chunk("3,\\N\n"),
		}
	}}
	host, port := startFakeServer(t, fake)
	param, proc, bat := newDatastreamTestParam(t, host, port, "(`a` > 0)")

	r := NewDataStreamReader(param)
	fileEmpty, err := r.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)

	finished, err := r.ReadBatch(proc.Ctx, bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 3, bat.RowCount())

	a := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
	require.Equal(t, []int32{1, 2, 3}, a[:3])
	require.Equal(t, "foo", bat.Vecs[1].GetStringAt(0))
	require.Equal(t, "with,comma", bat.Vecs[1].GetStringAt(1))
	require.True(t, bat.Vecs[1].GetNulls().Contains(2))

	require.NoError(t, r.Close())

	// the pushed filter travels in the request
	require.Equal(t, "src", fake.lastRequest.GetTable())
	require.Equal(t, "(`a` > 0)", fake.lastRequest.GetFilter())
}

func TestDataStreamReaderServerErrorFrame(t *testing.T) {
	fake := &fakeDataStreamServer{script: func(*datastream.ReadRequest) []*datastream.ReadResponse {
		return []*datastream.ReadResponse{
			chunk("1,foo\n"),
			streamErr(datastream.ErrorCode_ERROR_DATASOURCE_ERROR, "jdbc broke"),
		}
	}}
	host, port := startFakeServer(t, fake)
	param, proc, bat := newDatastreamTestParam(t, host, port, "")

	r := NewDataStreamReader(param)
	_, err := r.Open(param, proc)
	require.NoError(t, err)
	_, err = r.ReadBatch(proc.Ctx, bat, proc, nil)
	require.ErrorContains(t, err, "jdbc broke")
	require.NoError(t, r.Close())
}

func TestDataStreamReaderTableNotFound(t *testing.T) {
	fake := &fakeDataStreamServer{script: func(*datastream.ReadRequest) []*datastream.ReadResponse {
		return []*datastream.ReadResponse{
			streamErr(datastream.ErrorCode_ERROR_TABLE_NOT_FOUND, "no datasource named src"),
		}
	}}
	host, port := startFakeServer(t, fake)
	param, proc, bat := newDatastreamTestParam(t, host, port, "")

	r := NewDataStreamReader(param)
	_, err := r.Open(param, proc)
	require.NoError(t, err)
	_, err = r.ReadBatch(proc.Ctx, bat, proc, nil)
	require.ErrorContains(t, err, "no datasource named src")
	require.NoError(t, r.Close())
}

func TestDataStreamReaderConnectionRefused(t *testing.T) {
	// grab a port that is guaranteed closed
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := int32(lis.Addr().(*net.TCPAddr).Port)
	lis.Close()

	param, proc, bat := newDatastreamTestParam(t, "127.0.0.1", port, "")
	r := NewDataStreamReader(param)
	_, err = r.Open(param, proc)
	// grpc dialing is lazy: the failure surfaces at the first read
	if err == nil {
		_, err = r.ReadBatch(proc.Ctx, bat, proc, nil)
	}
	require.Error(t, err)
	require.NoError(t, r.Close())
}

func TestDataStreamReaderManyChunks(t *testing.T) {
	// hundreds of small chunks, one per record
	const rows = 500
	fake := &fakeDataStreamServer{script: func(*datastream.ReadRequest) []*datastream.ReadResponse {
		responses := make([]*datastream.ReadResponse, 0, rows)
		for i := 0; i < rows; i++ {
			responses = append(responses, chunk(fmt.Sprintf("%d,row-%d\n", i, i)))
		}
		return responses
	}}
	host, port := startFakeServer(t, fake)
	param, proc, bat := newDatastreamTestParam(t, host, port, "")

	r := NewDataStreamReader(param)
	_, err := r.Open(param, proc)
	require.NoError(t, err)

	total := 0
	for {
		bat.CleanOnlyData()
		finished, err := r.ReadBatch(proc.Ctx, bat, proc, nil)
		require.NoError(t, err)
		total += bat.RowCount()
		if finished {
			break
		}
	}
	require.Equal(t, rows, total)

	a := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
	last := bat.RowCount() - 1
	require.Equal(t, int32(rows-1), a[last])
	require.Equal(t, fmt.Sprintf("row-%d", rows-1), bat.Vecs[1].GetStringAt(last))
	require.NoError(t, r.Close())
}

func TestDataStreamReaderRecordSpansChunks(t *testing.T) {
	// The proto contract says servers must not split a record across chunks,
	// but the reader consumes the stream as plain bytes, so it tolerates a
	// misbehaving server rather than corrupting rows.
	fake := &fakeDataStreamServer{script: func(*datastream.ReadRequest) []*datastream.ReadResponse {
		return []*datastream.ReadResponse{
			chunk("1,fo"),
			chunk("o\n2,"),
			chunk("\"split,"),
			chunk("value\"\n"),
		}
	}}
	host, port := startFakeServer(t, fake)
	param, proc, bat := newDatastreamTestParam(t, host, port, "")

	r := NewDataStreamReader(param)
	_, err := r.Open(param, proc)
	require.NoError(t, err)
	finished, err := r.ReadBatch(proc.Ctx, bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, "foo", bat.Vecs[1].GetStringAt(0))
	require.Equal(t, "split,value", bat.Vecs[1].GetStringAt(1))
	require.NoError(t, r.Close())
}

func TestDataStreamReaderEmptyStream(t *testing.T) {
	fake := &fakeDataStreamServer{script: func(*datastream.ReadRequest) []*datastream.ReadResponse {
		return nil
	}}
	host, port := startFakeServer(t, fake)
	param, proc, bat := newDatastreamTestParam(t, host, port, "")

	r := NewDataStreamReader(param)
	_, err := r.Open(param, proc)
	require.NoError(t, err)
	finished, err := r.ReadBatch(proc.Ctx, bat, proc, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 0, bat.RowCount())
	require.NoError(t, r.Close())
}
