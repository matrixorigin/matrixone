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
	"context"
	"io"
	"net"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	datastream "github.com/matrixorigin/matrixone/pkg/datastream/v1"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqldatastream "github.com/matrixorigin/matrixone/pkg/sql/datastream"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// datastreamMaxRecvSize leaves generous headroom over the ~1MB chunks the
// server contract suggests.
const datastreamMaxRecvSize = 32 * 1024 * 1024

// DatastreamExternParam builds the synthetic tree.ExternParam a datastream
// scan runs with: a virtual single "file" (INLINE convention) of CSV in the
// fixed dialect documented in proto/datastream/v1/datastream.proto.
func DatastreamExternParam() *tree.ExternParam {
	return &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INLINE,
			Format:   tree.CSV,
			Tail:     new(tree.TailParameter),
		},
		ExParam: tree.ExParam{
			ExternType: int32(plan.ExternType_DATASTREAM_TB),
		},
	}
}

// DataStreamReader is an ExternalFileReader whose byte source is a gRPC
// chunk stream from a datastream server.  CSV parsing and batch building are
// reused from CsvReader.
type DataStreamReader struct {
	csv    CsvReader
	conn   *grpc.ClientConn
	cancel context.CancelFunc
}

func NewDataStreamReader(param *ExternalParam) *DataStreamReader {
	return &DataStreamReader{}
}

func (r *DataStreamReader) Open(param *ExternalParam, proc *process.Process) (fileEmpty bool, err error) {
	ds := param.DatastreamScan
	if ds == nil {
		return false, moerr.NewInternalError(proc.Ctx, "datastream reader without scan metadata")
	}
	// JoinHostPort (not "server:port") so an IPv6 literal like ::1 becomes
	// [::1]:4444 rather than the ambiguous ::1:4444
	// Fail closed on the retired "env:NAME" apikey syntax in a pre-existing
	// table definition instead of silently sending the literal string as the
	// credential.
	if err := sqldatastream.RejectEnvAPIKeyRef(proc.Ctx, ds.ApiKey); err != nil {
		return false, err
	}
	target := net.JoinHostPort(ds.Server, strconv.Itoa(int(ds.Port)))
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(datastreamMaxRecvSize)),
	)
	if err != nil {
		return false, moerr.NewInternalErrorf(proc.Ctx, "datastream: cannot create client for %s: %v", target, err)
	}
	ctx, cancel := context.WithCancel(proc.Ctx)
	stream, err := datastream.NewDataStreamClient(conn).Read(ctx, &datastream.ReadRequest{
		Table:  ds.Table,
		Filter: ds.PushedFilter,
		ApiKey: ds.ApiKey,
	})
	if err != nil {
		cancel()
		conn.Close()
		return false, moerr.NewInternalErrorf(proc.Ctx, "datastream: cannot open stream to %s: %v", target, err)
	}
	chunkReader := &grpcChunkReader{ctx: ctx, target: target, stream: stream, cancel: cancel}

	parser, err := newCSVParserFromReader(param.Extern, chunkReader)
	if err != nil {
		cancel()
		conn.Close()
		return false, err
	}
	r.conn = conn
	r.cancel = cancel
	r.csv.param = param
	r.csv.reader = chunkReader
	r.csv.plh = &ParseLineHandler{csvReader: parser}
	return false, nil
}

func (r *DataStreamReader) ReadBatch(
	ctx context.Context, buf *batch.Batch,
	proc *process.Process, analyzer process.Analyzer,
) (fileFinished bool, err error) {
	_, span := trace.Start(ctx, "DataStreamReader.ReadBatch")
	defer span.End()

	return r.csv.makeBatchRows(proc, buf)
}

func (r *DataStreamReader) Close() error {
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	err := r.csv.Close()
	if r.conn != nil {
		if closeErr := r.conn.Close(); err == nil {
			err = closeErr
		}
		r.conn = nil
	}
	return err
}

// grpcChunkReader adapts the Read response stream to an io.ReadCloser.  The
// server guarantees each chunk holds only complete CSV records, but the
// consumer is a plain buffered parser, so record alignment is not required
// here — bytes are handed over as they arrive.
type grpcChunkReader struct {
	ctx    context.Context
	target string
	stream grpc.ServerStreamingClient[datastream.ReadResponse]
	cancel context.CancelFunc
	buf    []byte
	err    error
}

func (r *grpcChunkReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		resp, err := r.stream.Recv()
		if err == io.EOF {
			r.err = io.EOF
			return 0, io.EOF
		}
		if err != nil {
			r.err = moerr.NewInternalErrorf(r.ctx, "datastream: stream from %s failed: %v", r.target, err)
			return 0, r.err
		}
		if respErr := resp.GetError(); respErr != nil {
			r.err = datastreamServerError(r.ctx, r.target, respErr)
			return 0, r.err
		}
		r.buf = resp.GetChunk().GetData()
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *grpcChunkReader) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}

func datastreamServerError(ctx context.Context, target string, respErr *datastream.Error) error {
	switch respErr.GetCode() {
	case datastream.ErrorCode_ERROR_TABLE_NOT_FOUND:
		return moerr.NewInvalidInputf(ctx, "datastream: table not found on %s: %s", target, respErr.GetMessage())
	case datastream.ErrorCode_ERROR_UNAUTHENTICATED:
		return moerr.NewInvalidInputf(ctx, "datastream: authentication failed for %s: %s (check the table's 'apikey' option)", target, respErr.GetMessage())
	default:
		return moerr.NewInternalErrorf(ctx, "datastream: server %s reported %s: %s",
			target, respErr.GetCode(), respErr.GetMessage())
	}
}
