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

package sidecarflight

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type testFlightService interface{}

type testFlightServer struct {
	schema              []byte
	header              []byte
	body                []byte
	ticket              []byte
	hash                []byte
	cancels             atomic.Int32
	badInfo             bool
	cancelErr           error
	cancelByIdempotency atomic.Int32
	prepareStarted      chan struct{}
	blockPrepare        chan struct{}
	prepareOnce         sync.Once
	cancelStarted       chan struct{}
	blockCancel         chan struct{}
	cancelOnce          sync.Once
}

func TestExecutionIdempotencyKeyMatchesProtocolVector(t *testing.T) {
	key := executionIdempotencyKey(42, []byte("qqqqqqqqqqqqqqqq"), []byte("plan"))
	require.Equal(t, "6acdd6974ba32f76809a1a11b372aa531a06dbe3aeb95e4c3dfb6075ce2de177", hex.EncodeToString(key[:]))
}

func TestRuntimeRejectsTLSVerificationBypass(t *testing.T) {
	_, err := NewRuntime(context.Background(), Config{
		Address: "sidecar.invalid:32010",
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // the production constructor must reject this test input
			Certificates:       []tls.Certificate{{}},
			RootCAs:            x509.NewCertPool(),
		},
		MaxBatchBytes: 1 << 20, RequestTimeout: time.Second, CleanupTimeout: time.Second,
	}, "contract")
	require.ErrorContains(t, err, "server CA")
}

func TestExecutionStreamsOneOwnedBatchAndCancelsOnWriterFailure(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), header: mustHex(t, fixtureHeaderHex), body: mustHex(t, fixtureBodyHex),
		ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
	}
	conn := testFlightConnection(t, server)
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Minute, CleanupTimeout: time.Second},
		conn:   conn, executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()

	execution, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	rows := 0
	err = execution.Run(context.Background(), mp, nil, func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
		rows += bat.RowCount()
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, rows)
	require.Equal(t, int64(0), mp.CurrNB())
	require.Equal(t, int32(0), server.cancels.Load())

	execution, err = runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings)
	require.NoError(t, err)
	writerErr := errors.New("client disconnected")
	runErr := execution.Run(context.Background(), mp, nil, func(*batch.Batch, *perfcounter.CounterSet) error {
		return writerErr
	})
	require.ErrorIs(t, runErr, writerErr)
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, execution.CleanupAfterRun(cleanupCtx, runErr), writerErr)
	require.Equal(t, int32(1), server.cancels.Load())
	require.Equal(t, int64(0), mp.CurrNB())
	require.NoError(t, runtime.Close(cleanupCtx))
}

func TestPrepareCancelsByIdempotencyWhenTicketIsUnknown(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), hash: make([]byte, sha256.Size), badInfo: true,
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Minute, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()

	_, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings)
	require.ErrorContains(t, err, "malformed endpoint ticket")
	require.False(t, IsQuiescenceUnknown(err))
	require.Equal(t, int32(1), server.cancelByIdempotency.Load())

	server.cancelErr = status.Error(codes.Unavailable, "cleanup unavailable")
	_, err = runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings)
	require.True(t, IsQuiescenceUnknown(err))
	require.False(t, IsPreVisibilityFallback(err))
}

func TestRuntimeCloseCancelsAndJoinsInflightPreparation(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
		prepareStarted: make(chan struct{}), blockPrepare: make(chan struct{}),
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Minute, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	prepareDone := make(chan error, 1)
	go func() {
		_, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings)
		prepareDone <- err
	}()
	<-server.prepareStarted
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, runtime.Close(closeCtx))
	require.Error(t, <-prepareDone)
	require.Equal(t, int32(1), server.cancelByIdempotency.Load())
}

func TestConcurrentRuntimeCloseWaitsForQuiescence(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
		cancelStarted: make(chan struct{}), blockCancel: make(chan struct{}),
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Minute, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	_, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- runtime.Close(ctx) }()
	<-server.cancelStarted
	second := make(chan error, 1)
	go func() { second <- runtime.Close(ctx) }()
	select {
	case err = <-second:
		require.Failf(t, "close returned early", "error: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(server.blockCancel)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
}

func TestContextCancellationReachesSidecarWhileWriterIsBlocked(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), header: mustHex(t, fixtureHeaderHex), body: mustHex(t, fixtureBodyHex),
		ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Minute, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	execution, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings)
	require.NoError(t, err)

	runCtx, cancelRun := context.WithCancel(context.Background())
	writerEntered := make(chan struct{})
	releaseWriter := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		runDone <- execution.Run(runCtx, mpool.MustNewZero(), nil, func(*batch.Batch, *perfcounter.CounterSet) error {
			close(writerEntered)
			<-releaseWriter
			return nil
		})
	}()
	<-writerEntered
	cancelRun()
	require.Eventually(t, func() bool { return server.cancels.Load() == 1 }, time.Second, time.Millisecond)
	close(releaseWriter)
	require.Error(t, <-runDone)
}

func fixtureOutputShape() ([]planpb.Type, []string) {
	return []planpb.Type{
		{Id: int32(types.T_bool)}, {Id: int32(types.T_int8)}, {Id: int32(types.T_int16)},
		{Id: int32(types.T_int32)}, {Id: int32(types.T_int64)}, {Id: int32(types.T_float32)},
		{Id: int32(types.T_float64)}, {Id: int32(types.T_varchar)},
		{Id: int32(types.T_decimal64), Width: 18, Scale: 2},
		{Id: int32(types.T_decimal128), Width: 38, Scale: 4},
		{Id: int32(types.T_date)}, {Id: int32(types.T_uint32)},
	}, []string{"b", "i8", "i16", "i32", "i64", "f32", "f64", "s", "d64", "d128", "date", "u32_transport"}
}

func testFlightConnection(t *testing.T, implementation *testFlightServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "arrow.flight.protocol.FlightService",
		HandlerType: (*testFlightService)(nil),
		Methods:     []grpc.MethodDesc{{MethodName: "GetFlightInfo", Handler: testGetFlightInfo}},
		Streams: []grpc.StreamDesc{
			{StreamName: "DoGet", Handler: testDoGet, ServerStreams: true},
			{StreamName: "DoAction", Handler: testDoAction, ServerStreams: true},
		},
	}, implementation)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///sidecar-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func testGetFlightInfo(service any, ctx context.Context, decode func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	request := new(flightDescriptor)
	if err := decode(request); err != nil {
		return nil, err
	}
	server := service.(*testFlightServer)
	command := new(executeSubstraitRequest)
	if request.Type != commandDescriptor || len(request.Path) != 0 || proto.Unmarshal(request.Cmd, command) != nil ||
		command.ProtocolVersion != protocolVersion || command.SubstraitVersion != substraitVersion ||
		len(command.CapabilityHash) != sha256.Size || command.MaxBatchBytes == 0 || command.DeadlineUnixMS == 0 ||
		len(command.Plan) == 0 || len(command.QueryID) != 16 || len(command.IdempotencyKey) != sha256.Size || command.AccountID == 0 {
		return nil, status.Error(codes.InvalidArgument, "malformed ExecuteSubstrait command")
	}
	if server.prepareStarted != nil {
		server.prepareOnce.Do(func() { close(server.prepareStarted) })
	}
	if server.blockPrepare != nil {
		select {
		case <-server.blockPrepare:
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
	if server.badInfo {
		return &flightInfo{Schema: server.schema, AppMetadata: server.hash}, nil
	}
	return &flightInfo{Schema: server.schema, Endpoint: []*flightEndpoint{{Ticket: &flightTicket{Ticket: server.ticket}}}, AppMetadata: server.hash}, nil
}

func testDoGet(service any, stream grpc.ServerStream) error {
	server := service.(*testFlightServer)
	ticket := new(flightTicket)
	if err := stream.RecvMsg(ticket); err != nil {
		return err
	}
	if err := stream.SendMsg(&flightData{DataHeader: server.schema}); err != nil {
		return err
	}
	return stream.SendMsg(&flightData{DataHeader: server.header, DataBody: server.body})
}

func testDoAction(service any, stream grpc.ServerStream) error {
	server := service.(*testFlightServer)
	action := new(flightAction)
	if err := stream.RecvMsg(action); err != nil {
		return err
	}
	if server.cancelErr != nil {
		return server.cancelErr
	}
	request := new(cancelExecutionRequest)
	if action.Type != "CancelExecution" || proto.Unmarshal(action.Body, request) != nil ||
		(len(request.Ticket) != ticketBytes && len(request.IdempotencyKey) != sha256.Size) ||
		(len(request.Ticket) != 0 && len(request.IdempotencyKey) != 0) {
		return status.Error(codes.InvalidArgument, "malformed cancellation command")
	}
	if len(request.IdempotencyKey) != 0 {
		server.cancelByIdempotency.Add(1)
	}
	if server.cancelStarted != nil {
		server.cancelOnce.Do(func() { close(server.cancelStarted) })
	}
	if server.blockCancel != nil {
		select {
		case <-server.blockCancel:
		case <-stream.Context().Done():
			return context.Cause(stream.Context())
		}
	}
	server.cancels.Add(1)
	return stream.SendMsg(&flightResult{Body: []byte("quiesced")})
}
