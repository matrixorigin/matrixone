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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
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
	locations           []*flightLocation
	capabilities        []byte
	streamMessages      []*flightData
	cancelErr           error
	cancelByIdempotency atomic.Int32
	prepareStarted      chan struct{}
	blockPrepare        chan struct{}
	prepareOnce         sync.Once
	cancelStarted       chan struct{}
	blockCancel         chan struct{}
	cancelOnce          sync.Once
}

func TestInternalErrorfUsesMoerrAndPreservesCause(t *testing.T) {
	plain := internalErrorf("protocol failure")
	require.True(t, moerr.IsMoErrCode(plain, moerr.ErrInternal))

	cause := errors.New("transport failure")
	wrapped := internalErrorf("prepare: %w", cause)
	require.ErrorIs(t, wrapped, cause)
	require.ErrorContains(t, wrapped, "prepare: transport failure")
	unknown := &quiescenceUnknownError{err: cause}
	require.Equal(t, cause.Error(), unknown.Error())
	require.ErrorIs(t, unknown, cause)
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

func TestNewRuntimeNegotiatesCapabilitiesOverTLS(t *testing.T) {
	server := &testFlightServer{capabilities: []byte("contract")}
	address, clientTLS := testTLSFlightServer(t, server)
	clientTLS.MinVersion = tls.VersionTLS10
	runtime, err := NewRuntime(context.Background(), Config{
		Address: address, TLSConfig: clientTLS, MaxBatchBytes: 1 << 20,
		RequestTimeout: time.Second, CleanupTimeout: time.Second,
	}, "contract")
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS10), clientTLS.MinVersion)
	require.NoError(t, runtime.Close(context.Background()))
	require.NoError(t, runtime.Close(context.Background()))

	server.capabilities = []byte("different")
	_, err = NewRuntime(context.Background(), Config{
		Address: address, TLSConfig: clientTLS, MaxBatchBytes: 1 << 20,
		RequestTimeout: time.Second, CleanupTimeout: time.Second,
	}, "contract")
	require.ErrorContains(t, err, "capability document mismatch")
}

func TestRuntimeAndExecutionRejectInvalidStates(t *testing.T) {
	validTLS := &tls.Config{
		Certificates: []tls.Certificate{{}}, RootCAs: x509.NewCertPool(),
	}
	for _, tc := range []struct {
		name   string
		config Config
		doc    string
		want   string
	}{
		{name: "missing identity", config: Config{}, doc: "contract", want: "server CA"},
		{name: "invalid limits", config: Config{Address: "unused", TLSConfig: validTLS}, doc: "contract", want: "invalid transport limits"},
		{name: "missing capability", config: Config{Address: "unused", TLSConfig: validTLS, MaxBatchBytes: 1, RequestTimeout: time.Second, CleanupTimeout: time.Second}, want: "invalid transport limits"},
		{name: "message overflow", config: Config{Address: "unused", TLSConfig: validTLS, MaxBatchBytes: ^uint64(0), RequestTimeout: time.Second, CleanupTimeout: time.Second}, doc: "contract", want: "overflows"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewRuntime(context.Background(), tc.config, tc.doc)
			require.ErrorContains(t, err, tc.want)
		})
	}

	var nilRuntime *Runtime
	_, err := nilRuntime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), nil, nil)
	require.ErrorContains(t, err, "nil runtime")
	runtime := &Runtime{config: Config{RequestTimeout: time.Second}, stopped: true}
	_, err = runtime.Prepare(context.Background(), 0, nil, nil, nil, nil)
	require.ErrorContains(t, err, "query identity")
	_, err = runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), nil, nil)
	require.ErrorContains(t, err, "runtime is stopping")

	var nilExecution *Execution
	require.ErrorContains(t, nilExecution.Run(nil, nil, nil, nil), "invalid execution")
	require.NoError(t, nilExecution.CancelAndJoin(nil))
	require.NoError(t, nilExecution.CleanupAfterRun(nil, nil))
}

func TestPrepareRejectsUnsafeFlightInfo(t *testing.T) {
	typesOut, headings := fixtureOutputShape()
	for _, tc := range []struct {
		name      string
		configure func(*testFlightServer)
		want      string
	}{
		{name: "redirect", configure: func(s *testFlightServer) { s.locations = []*flightLocation{{URI: "grpc://other"}} }, want: "redirection"},
		{name: "capability hash", configure: func(s *testFlightServer) { s.hash[0] = 1 }, want: "capability hash mismatch"},
		{name: "schema", configure: func(s *testFlightServer) { s.schema = []byte("bad") }, want: "validate schema"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &testFlightServer{
				schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
			}
			tc.configure(server)
			runtime := &Runtime{
				config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Second, CleanupTimeout: time.Second},
				conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
			}
			_, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings)
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, int32(1), server.cancels.Load())
		})
	}
}

func TestFlightFallbackClassificationAndWireMessages(t *testing.T) {
	require.False(t, IsPreVisibilityFallback(nil))
	require.True(t, IsPreVisibilityFallback(status.Error(codes.Unavailable, "down")))
	require.True(t, IsPreVisibilityFallback(status.Error(codes.ResourceExhausted, "busy")))
	require.True(t, IsPreVisibilityFallback(errors.New("UNSUPPORTED_PLAN")))
	require.True(t, IsPreVisibilityFallback(errors.New("UNSUPPORTED_VERSION")))
	require.True(t, IsPreVisibilityFallback(errors.New("CAPABILITY_MISMATCH")))
	require.False(t, IsPreVisibilityFallback(errors.New("permission denied")))

	require.Contains(t, (&flightAction{Type: "x", Body: []byte{1}}).String(), "x")
	require.Contains(t, (&flightResult{Body: []byte{1}}).String(), "1 bytes")
	require.Contains(t, (&flightDescriptor{Type: 2, Cmd: []byte{1}}).String(), "1 bytes")
	require.Contains(t, (&flightTicket{Ticket: []byte{1}}).String(), "1 bytes")
	require.Contains(t, (&flightLocation{URI: "grpc://sidecar"}).String(), "sidecar")
	require.Equal(t, "FlightEndpoint", (&flightEndpoint{}).String())
	require.Contains(t, (&flightInfo{Endpoint: []*flightEndpoint{{}}}).String(), "1 endpoints")
	require.Contains(t, (&flightData{DataHeader: []byte{1}, DataBody: []byte{2}}).String(), "1,1 bytes")
	require.Contains(t, (&executeSubstraitRequest{Plan: []byte{1}}).String(), "1 bytes")
	require.Contains(t, (&cancelExecutionRequest{Ticket: []byte{1}}).String(), "1,0 bytes")
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

func TestExecutionRejectsMalformedStreamsAndDuplicateClaims(t *testing.T) {
	schemaWire := mustHex(t, fixtureSchemaHex)
	typesOut, headings := fixtureOutputShape()
	schema, err := ParseSchema(schemaWire, typesOut, headings)
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		messages []*flightData
		maximum  uint64
		want     string
	}{
		{name: "missing schema", messages: []*flightData{}, maximum: 1 << 20, want: "before its schema"},
		{name: "empty header", messages: []*flightData{{}}, maximum: 1 << 20, want: "malformed or oversized"},
		{name: "schema body", messages: []*flightData{{DataHeader: schemaWire, DataBody: []byte{1}}}, maximum: 1 << 20, want: "schema message contains a body"},
		{name: "schema mismatch", messages: []*flightData{{DataHeader: append([]byte(nil), schemaWire[:len(schemaWire)-1]...)}}, maximum: 1 << 20, want: "stream schema"},
		{name: "oversized body", messages: []*flightData{{DataHeader: schemaWire}, {DataHeader: []byte{1}, DataBody: []byte{1, 2}}}, maximum: 1, want: "malformed or oversized"},
		{name: "invalid batch", messages: []*flightData{{DataHeader: schemaWire}, {DataHeader: []byte{1}}}, maximum: 1 << 20, want: "decode record batch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &testFlightServer{
				ticket: make([]byte, ticketBytes), streamMessages: tc.messages,
			}
			runtime := &Runtime{
				config: Config{MaxBatchBytes: tc.maximum, CleanupTimeout: time.Second},
				conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
			}
			execution := &Execution{
				runtime: runtime, ticket: server.ticket, schema: schema, cleanupDone: make(chan struct{}),
			}
			runtime.executions[execution] = struct{}{}
			runErr := execution.Run(context.Background(), mpool.MustNewZero(), nil, func(*batch.Batch, *perfcounter.CounterSet) error { return nil })
			require.ErrorContains(t, runErr, tc.want)
			require.ErrorContains(t, execution.Run(context.Background(), mpool.MustNewZero(), nil, func(*batch.Batch, *perfcounter.CounterSet) error { return nil }), "already claimed")
			require.ErrorIs(t, execution.CleanupAfterRun(context.Background(), runErr), runErr)
			require.NoError(t, execution.CancelAndJoin(context.Background()))
		})
	}
}

func TestCancelAndJoinHonorsExistingCleanupAndContext(t *testing.T) {
	runtime := &Runtime{executions: make(map[*Execution]struct{})}
	terminal := &Execution{runtime: runtime, terminal: true, cleanupDone: make(chan struct{})}
	require.NoError(t, terminal.CancelAndJoin(context.Background()))

	done := make(chan struct{})
	waiting := &Execution{runtime: runtime, cleanupRunning: true, cleanupDone: done}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, waiting.CancelAndJoin(ctx), context.Canceled)
	close(done)
	require.NoError(t, waiting.CancelAndJoin(context.Background()))
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
	return &flightInfo{Schema: server.schema, Endpoint: []*flightEndpoint{{
		Ticket: &flightTicket{Ticket: server.ticket}, Locations: server.locations,
	}}, AppMetadata: server.hash}, nil
}

func testDoGet(service any, stream grpc.ServerStream) error {
	server := service.(*testFlightServer)
	ticket := new(flightTicket)
	if err := stream.RecvMsg(ticket); err != nil {
		return err
	}
	if server.streamMessages != nil {
		for _, message := range server.streamMessages {
			if err := stream.SendMsg(message); err != nil {
				return err
			}
		}
		return nil
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
	if action.Type == "GetCapabilities" {
		return stream.SendMsg(&flightResult{Body: server.capabilities})
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

func testTLSFlightServer(t *testing.T, implementation *testFlightServer) (string, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		IsCA:        true, BasicConstraintsValid: true, DNSNames: []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	privateDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}), 0o600))
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate},
	})))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "arrow.flight.protocol.FlightService", HandlerType: (*testFlightService)(nil),
		Methods: []grpc.MethodDesc{{MethodName: "GetFlightInfo", Handler: testGetFlightInfo}},
		Streams: []grpc.StreamDesc{
			{StreamName: "DoGet", Handler: testDoGet, ServerStreams: true},
			{StreamName: "DoAction", Handler: testDoAction, ServerStreams: true},
		},
	}, implementation)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String(), &tls.Config{
		MinVersion: tls.VersionTLS12, ServerName: "localhost",
		Certificates: []tls.Certificate{certificate}, RootCAs: roots,
	}
}
