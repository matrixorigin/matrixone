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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

// These one-byte sentinels keep older test setup concise. GetFlightInfo now
// replaces schema with the request's negotiated native schema, and DoGet uses
// body only as the signal to emit the native result fixture.
const (
	fixtureSchemaHex = "00"
	fixtureHeaderHex = "00"
	fixtureBodyHex   = "00"
)

func mustHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	require.NoError(t, err)
	return decoded
}

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
	doGetStarted        chan struct{}
	blockDoGet          chan struct{}
	doGetOnce           sync.Once
	doPutBatch          chan struct{}
	blockDoPutAck       chan struct{}
	doPutOnce           sync.Once
	doPutBatches        atomic.Int32
	notNeededInput      bool
	cancelFailures      atomic.Int32
	deadlineUnixMS      atomic.Int64
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
	key := executionIdempotencyKey(42, []byte("qqqqqqqqqqqqqqqq"))
	require.Equal(t, "77f6a676cc4bfdbc9265e1bbbcd8140f4a820ec41a2979f52706f41ff22fb33a", hex.EncodeToString(key[:]))
	system := executionIdempotencyKey(0, []byte("qqqqqqqqqqqqqqqq"))
	require.NotEqual(t, key, system)
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
	_, err := nilRuntime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), nil, nil, testFlightDeadline(), testFlightRelease)
	require.ErrorContains(t, err, "nil runtime")
	runtime := &Runtime{config: Config{RequestTimeout: time.Second}, stopped: true}
	_, err = runtime.Prepare(context.Background(), 0, nil, nil, nil, nil, testFlightDeadline(), testFlightRelease)
	require.ErrorContains(t, err, "query identity")
	_, err = runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), nil, nil, testFlightDeadline(), testFlightRelease)
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
			_, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings, testFlightDeadline(), testFlightRelease)
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, int32(1), server.cancels.Load())
		})
	}
}

func TestPrepareClampsDeadlineToLeaseSafetyCeiling(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Minute, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	ceiling := time.Now().Add(10 * time.Second).Truncate(time.Millisecond)
	execution, err := runtime.Prepare(
		context.Background(), 0, make([]byte, 16), []byte("plan"), typesOut, headings,
		ceiling, testFlightRelease,
	)
	require.NoError(t, err)
	require.Equal(t, ceiling.UnixMilli(), server.deadlineUnixMS.Load())
	require.NoError(t, execution.Cleanup(context.Background()))

	var releases atomic.Int32
	_, err = runtime.Prepare(
		context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings,
		time.Now().Add(-time.Second), func(context.Context) error { releases.Add(1); return nil },
	)
	require.ErrorContains(t, err, "lease-safe execution deadline has expired")
	require.Equal(t, int32(1), releases.Load())
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
	require.Contains(t, (&flightPutResult{AppMetadata: []byte{1}}).String(), "1 bytes")
	require.Contains(t, (&uploadInputRequest{Ticket: []byte{1}, StreamRef: []byte{2}}).String(), "1,1 bytes")
	require.Contains(t, (&uploadInputAck{AcknowledgedBatches: 1, Rows: 2, Bytes: 3}).String(), "1,false,false,false")
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

	execution, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings, testFlightDeadline(), testFlightRelease)
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

	execution, err = runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings, testFlightDeadline(), testFlightRelease)
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

func TestNativeInputStreamsOneConsumedBatchAtATime(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
	}
	runtime := &Runtime{
		config:     Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Second, CleanupTimeout: time.Second},
		conn:       testFlightConnection(t, server),
		executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	execution, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"),
		typesOut, headings, testFlightDeadline(), testFlightRelease)
	require.NoError(t, err)
	input, err := execution.NewNativeInput(bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	_, err = execution.NewNativeInput(bytes.Repeat([]byte{7}, 32))
	require.ErrorContains(t, err, "duplicate native input identity")
	require.NoError(t, input.Start(context.Background()))
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Attrs = []string{"value"}
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(42), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)
	require.NoError(t, input.Send(context.Background(), bat, mp))
	require.NoError(t, input.Finish(context.Background()))
	require.NoError(t, input.Err())
}

func TestNativeInputRejectsInvalidAndTerminalStates(t *testing.T) {
	ref := bytes.Repeat([]byte{7}, 32)
	_, err := (*Execution)(nil).NewNativeInput(ref)
	require.ErrorContains(t, err, "invalid native input identity")
	_, err = (&Execution{}).NewNativeInput(ref)
	require.ErrorContains(t, err, "invalid native input identity")
	_, err = (&Execution{runtime: &Runtime{}, ticket: make([]byte, ticketBytes)}).NewNativeInput(ref[:31])
	require.ErrorContains(t, err, "invalid native input identity")

	for _, tc := range []struct {
		name      string
		configure func(*Execution)
	}{
		{name: "started", configure: func(e *Execution) { e.started = true }},
		{name: "cleanup", configure: func(e *Execution) { e.cleanupRunning = true }},
		{name: "terminal", configure: func(e *Execution) { e.terminal = true }},
		{name: "quiesced", configure: func(e *Execution) { e.quiesced = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			execution := &Execution{runtime: &Runtime{}, ticket: make([]byte, ticketBytes)}
			tc.configure(execution)
			_, err := execution.NewNativeInput(ref)
			require.ErrorContains(t, err, "no longer accepts native inputs")
		})
	}

	full := &Execution{
		runtime: &Runtime{}, ticket: make([]byte, ticketBytes),
		inputs: make([]*NativeInput, maxNativeInputs),
	}
	_, err = full.NewNativeInput(ref)
	require.ErrorContains(t, err, "count exceeds")

	execution := &Execution{runtime: &Runtime{}, ticket: make([]byte, ticketBytes)}
	input, err := execution.NewNativeInput(ref)
	require.NoError(t, err)
	ref[0] = 0
	require.Equal(t, byte(7), input.streamRef[0])

	require.ErrorContains(t, (*NativeInput)(nil).Start(context.Background()), "nil native input")
	require.NoError(t, (*NativeInput)(nil).Send(context.Background(), nil, nil))
	require.NoError(t, (*NativeInput)(nil).Finish(context.Background()))
	require.NoError(t, (*NativeInput)(nil).Err())
	require.False(t, (*NativeInput)(nil).NotNeeded())
	(*NativeInput)(nil).Abort(nil)

	notNeeded := &NativeInput{notNeeded: true}
	require.NoError(t, notNeeded.Start(context.Background()))
	require.True(t, notNeeded.NotNeeded())

	terminalCause := errors.New("terminal input")
	terminal := &NativeInput{finished: true, terminalErr: terminalCause}
	require.ErrorIs(t, terminal.Start(context.Background()), terminalCause)
	require.ErrorIs(t, terminal.Finish(context.Background()), terminalCause)
	require.ErrorIs(t, terminal.Err(), terminalCause)

	aborted := new(NativeInput)
	aborted.Abort(nil)
	require.ErrorIs(t, aborted.Err(), context.Canceled)
	require.ErrorIs(t, aborted.Finish(context.Background()), context.Canceled)
}

func TestCloneNativeWindowHandlesConstAndRejectsImpossibleRows(t *testing.T) {
	mp := mpool.MustNewZero()

	withNil := batch.NewWithSize(1)
	withNil.SetRowCount(1)
	_, err := cloneNativeWindow(withNil, 0, 1, mp)
	require.ErrorContains(t, err, "nil vector")

	constVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(42), 4, mp)
	require.NoError(t, err)
	constant := batch.NewWithSize(1)
	constant.Vecs[0] = constVec
	constant.SetRowCount(4)
	window, err := cloneNativeWindow(constant, 1, 3, mp)
	require.NoError(t, err)
	require.Equal(t, 2, window.RowCount())
	require.True(t, window.Vecs[0].IsConst())
	require.Equal(t, 2, window.Vecs[0].Length())
	window.Clean(mp)
	constant.Clean(mp)

	largeVec, err := vector.NewConstBytes(types.T_varchar.ToType(), bytes.Repeat([]byte{'x'}, 1024), 1, mp)
	require.NoError(t, err)
	large := batch.NewWithSize(1)
	large.Vecs[0] = largeVec
	large.SetRowCount(1)
	require.ErrorContains(t, new(NativeInput).sendSplitLocked(large, 1, mp), "one native input row exceeds")
	large.Clean(mp)
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestNativeInputAbortCancelsBlockedAcknowledgement(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
		doPutBatch: make(chan struct{}), blockDoPutAck: make(chan struct{}),
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 128, RequestTimeout: time.Second, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	execution, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"),
		typesOut, headings, testFlightDeadline(), testFlightRelease)
	require.NoError(t, err)
	input, err := execution.NewNativeInput(bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	require.NoError(t, input.Start(context.Background()))

	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for row := int64(0); row < 20; row++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], row, false, mp))
	}
	bat.SetRowCount(20)
	defer bat.Clean(mp)
	sendDone := make(chan error, 1)
	go func() { sendDone <- input.Send(context.Background(), bat, mp) }()
	select {
	case <-server.doPutBatch:
	case <-time.After(time.Second):
		t.Fatal("native input batch did not reach the server")
	}
	input.Abort(errors.New("injected cancellation"))
	select {
	case sendErr := <-sendDone:
		require.Error(t, sendErr)
	case <-time.After(time.Second):
		t.Fatal("Abort did not release the blocked acknowledgement")
	}
}

func TestNativeInputRetireCancelsBlockedAcknowledgementWithoutError(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
		doPutBatch: make(chan struct{}), blockDoPutAck: make(chan struct{}),
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 128, RequestTimeout: time.Second, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	execution, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"),
		typesOut, headings, testFlightDeadline(), testFlightRelease)
	require.NoError(t, err)
	input, err := execution.NewNativeInput(bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	require.NoError(t, input.Start(context.Background()))

	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
	bat.SetRowCount(1)
	defer bat.Clean(mp)
	sendDone := make(chan error, 1)
	go func() { sendDone <- input.Send(context.Background(), bat, mp) }()
	select {
	case <-server.doPutBatch:
	case <-time.After(time.Second):
		t.Fatal("native input batch did not reach the server")
	}
	input.Retire()
	select {
	case sendErr := <-sendDone:
		require.NoError(t, sendErr)
	case <-time.After(time.Second):
		t.Fatal("successful retirement did not release the blocked acknowledgement")
	}
	require.True(t, input.NotNeeded())
	require.NoError(t, input.Err())
}

func TestNativeInputSplitsOversizedBatchesWithinNegotiatedLimit(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 128, RequestTimeout: time.Second, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	execution, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"),
		typesOut, headings, testFlightDeadline(), testFlightRelease)
	require.NoError(t, err)
	input, err := execution.NewNativeInput(bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	require.NoError(t, input.Start(context.Background()))

	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for row := int64(0); row < 20; row++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], row, false, mp))
	}
	bat.SetRowCount(20)
	defer bat.Clean(mp)
	require.NoError(t, input.Send(context.Background(), bat, mp))
	require.NoError(t, input.Finish(context.Background()))
	require.Greater(t, input.sequence, uint64(1))
	require.Equal(t, uint64(20), input.rows)
	require.LessOrEqual(t, input.bytes, input.sequence*runtime.config.MaxBatchBytes)
}

func TestNativeInputAcceptsEarlyNotNeeded(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
		notNeededInput: true,
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 128, RequestTimeout: time.Second, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	execution, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"),
		typesOut, headings, testFlightDeadline(), testFlightRelease)
	require.NoError(t, err)
	input, err := execution.NewNativeInput(bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	require.NoError(t, input.Start(context.Background()))

	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	for row := int64(0); row < 20; row++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], row, false, mp))
	}
	bat.SetRowCount(20)
	defer bat.Clean(mp)
	require.NoError(t, input.Send(context.Background(), bat, mp))
	require.True(t, input.notNeeded)
	require.Equal(t, int32(1), server.doPutBatches.Load())
	require.NoError(t, input.Send(context.Background(), bat, mp))
	require.Equal(t, int32(1), server.doPutBatches.Load())
	require.NoError(t, input.Finish(context.Background()))
}

func TestCleanupAfterResultEOFStillJoinsNativeInputs(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Second, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	execution, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"),
		typesOut, headings, testFlightDeadline(), testFlightRelease)
	require.NoError(t, err)
	input, err := execution.NewNativeInput(bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	require.NoError(t, input.Start(context.Background()))
	execution.mu.Lock()
	execution.quiesced = true
	execution.mu.Unlock()
	require.NoError(t, execution.Cleanup(context.Background()))
	require.ErrorIs(t, input.Err(), context.Canceled)
	require.Equal(t, int32(1), server.cancels.Load())
}

func TestExecutionRejectsCompressedNativeResultBeforeFill(t *testing.T) {
	mp := mpool.MustNewZero()
	payload := func() []byte {
		vec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
		require.NoError(t, err)
		vec.SetLength(1 << 30)
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vec
		bat.SetRowCount(1 << 30)
		defer bat.Clean(mp)
		payload, err := bat.MarshalBinary()
		require.NoError(t, err)
		require.Less(t, len(payload), 1024)
		return payload
	}()

	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
		streamMessages: []*flightData{
			{DataHeader: []byte{1}},
			{DataHeader: marshalNativeBatchFrame(1, payload)},
		},
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Second, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	execution, err := runtime.Prepare(
		context.Background(), 1, make([]byte, 16), []byte("plan"),
		[]planpb.Type{{Id: int32(types.T_int64)}}, []string{"v"}, testFlightDeadline(), testFlightRelease,
	)
	require.NoError(t, err)
	fillCalls := 0
	err = execution.Run(context.Background(), mp, nil, func(*batch.Batch, *perfcounter.CounterSet) error {
		fillCalls++
		return nil
	})
	require.ErrorContains(t, err, "is not flat")
	require.Zero(t, fillCalls)
	require.Equal(t, int64(0), mp.CurrNB())
	require.NoError(t, execution.Cleanup(context.Background()))
}

func TestResultEOFRetiresInputBeforeItsFirstBatch(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Second, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	execution, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"),
		typesOut, headings, testFlightDeadline(), testFlightRelease)
	require.NoError(t, err)
	input, err := execution.NewNativeInput(bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	require.NoError(t, input.Start(context.Background()))

	mp := mpool.MustNewZero()
	runDone := make(chan error, 1)
	go func() {
		runDone <- execution.Run(context.Background(), mp, nil,
			func(*batch.Batch, *perfcounter.CounterSet) error { return nil })
	}()
	select {
	case runErr := <-runDone:
		require.NoError(t, runErr)
	case <-time.After(time.Second):
		t.Fatal("result EOF waited for an input batch that the sidecar did not need")
	}
	require.True(t, input.NotNeeded())
	require.NoError(t, input.Finish(context.Background()))
	require.NoError(t, input.Err())
	require.Zero(t, server.doPutBatches.Load())

	require.NoError(t, execution.CleanupAfterRun(context.Background(), nil))
	require.Equal(t, int32(1), server.cancels.Load(),
		"streamed success must join the server-side DoPut handler before cleanup")
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

	_, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings, testFlightDeadline(), testFlightRelease)
	require.ErrorContains(t, err, "malformed endpoint ticket")
	require.False(t, IsQuiescenceUnknown(err))
	require.Equal(t, int32(1), server.cancelByIdempotency.Load())

	server.cancelErr = status.Error(codes.Unavailable, "cleanup unavailable")
	_, err = runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings, testFlightDeadline(), testFlightRelease)
	require.True(t, IsQuiescenceUnknown(err))
	require.False(t, IsPreVisibilityFallback(err))
}

func TestAmbiguousPrepareCleanupIsRetriedUntilLeaseRelease(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), hash: make([]byte, sha256.Size), badInfo: true,
	}
	server.cancelFailures.Store(1)
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Minute, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	var releaseAttempts atomic.Int32

	_, err := runtime.Prepare(
		context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings,
		testFlightDeadline(),
		func(context.Context) error {
			releaseAttempts.Add(1)
			return nil
		},
	)
	require.True(t, IsQuiescenceUnknown(err))
	require.Eventually(t, func() bool {
		return server.cancelByIdempotency.Load() == 2 && releaseAttempts.Load() == 1 && runtimeExecutionCount(runtime) == 0
	}, time.Second, time.Millisecond)
	require.NoError(t, runtime.Close(context.Background()))
}

func TestExecutionRetriesFailedCancellationAndPartialLeaseRelease(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), header: mustHex(t, fixtureHeaderHex), body: mustHex(t, fixtureBodyHex),
		ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
	}
	server.cancelFailures.Store(1)
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: time.Minute, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	var releaseAttempts atomic.Int32
	execution, err := runtime.Prepare(
		context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings,
		testFlightDeadline(),
		func(context.Context) error {
			if releaseAttempts.Add(1) == 1 {
				return errors.New("second lease unregister failed")
			}
			return nil
		},
	)
	require.NoError(t, err)
	writerErr := errors.New("writer stopped")
	runErr := execution.Run(context.Background(), mpool.MustNewZero(), nil, func(*batch.Batch, *perfcounter.CounterSet) error {
		return writerErr
	})
	require.ErrorIs(t, runErr, writerErr)
	require.Error(t, execution.CleanupAfterRun(context.Background(), runErr))
	require.Eventually(t, func() bool {
		return server.cancels.Load() == 1 && releaseAttempts.Load() == 2 && runtimeExecutionCount(runtime) == 0
	}, time.Second, time.Millisecond)
	require.NoError(t, runtime.Close(context.Background()))
}

func TestRequestDeadlineCancelsStalledDoGet(t *testing.T) {
	server := &testFlightServer{
		schema: mustHex(t, fixtureSchemaHex), ticket: make([]byte, ticketBytes), hash: make([]byte, sha256.Size),
		doGetStarted: make(chan struct{}), blockDoGet: make(chan struct{}),
	}
	runtime := &Runtime{
		config: Config{MaxBatchBytes: 1 << 20, RequestTimeout: 250 * time.Millisecond, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	copy(runtime.capabilityHash[:], server.hash)
	typesOut, headings := fixtureOutputShape()
	var released atomic.Int32
	execution, err := runtime.Prepare(
		context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings,
		testFlightDeadline(),
		func(context.Context) error { released.Add(1); return nil },
	)
	require.NoError(t, err)
	started := time.Now()
	runErr := execution.Run(context.Background(), mpool.MustNewZero(), nil, func(*batch.Batch, *perfcounter.CounterSet) error { return nil })
	require.Error(t, runErr)
	require.Less(t, time.Since(started), time.Second)
	require.Error(t, execution.CleanupAfterRun(context.Background(), runErr))
	require.Eventually(t, func() bool {
		return server.cancels.Load() == 1 && released.Load() == 1 && runtimeExecutionCount(runtime) == 0
	}, time.Second, time.Millisecond)
	require.NoError(t, runtime.Close(context.Background()))
}

func TestReplayReconciliationSurvivesRuntimeClose(t *testing.T) {
	server := &testFlightServer{cancelErr: status.Error(codes.Unavailable, "sidecar unavailable")}
	first := &Runtime{
		config: Config{RequestTimeout: time.Second, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	var released atomic.Int32
	release := func(context.Context) error { released.Add(1); return nil }
	require.NoError(t, first.Reconcile(0, make([]byte, 16), release))
	require.Eventually(t, func() bool { return server.cancelByIdempotency.Load() > 0 }, time.Second, time.Millisecond)
	require.Error(t, first.Close(context.Background()))
	require.Zero(t, released.Load())

	server.cancelErr = nil
	second := &Runtime{
		config: Config{RequestTimeout: time.Second, CleanupTimeout: time.Second},
		conn:   testFlightConnection(t, server), executions: make(map[*Execution]struct{}),
	}
	require.NoError(t, second.Reconcile(0, make([]byte, 16), release))
	require.Eventually(t, func() bool {
		return released.Load() == 1 && runtimeExecutionCount(second) == 0
	}, time.Second, time.Millisecond)
	require.NoError(t, second.Close(context.Background()))
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
		_, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings, testFlightDeadline(), testFlightRelease)
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
	_, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings, testFlightDeadline(), testFlightRelease)
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
	execution, err := runtime.Prepare(context.Background(), 1, make([]byte, 16), []byte("plan"), typesOut, headings, testFlightDeadline(), testFlightRelease)
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
	typesOut, headings := fixtureOutputShape()
	schema, schemaWire, err := newNativeResultSchema(typesOut, headings)
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		messages []*flightData
		maximum  uint64
		want     string
	}{
		{name: "missing schema", messages: []*flightData{}, maximum: 1 << 20, want: "before its Flight transport schema"},
		{name: "empty frame", messages: []*flightData{{}}, maximum: 1 << 20, want: "malformed Flight transport schema"},
		{name: "schema body", messages: []*flightData{{DataHeader: []byte{1}, DataBody: []byte{1}}}, maximum: 1 << 20, want: "malformed Flight transport schema"},
		{name: "schema metadata", messages: []*flightData{{AppMetadata: schemaWire}}, maximum: 1 << 20, want: "malformed Flight transport schema"},
		{name: "oversized frame", messages: []*flightData{{DataHeader: []byte{1}}, {DataHeader: make([]byte, nativeBatchFrameHeaderBytes+2)}}, maximum: 1, want: "malformed or oversized"},
		{name: "invalid batch", messages: []*flightData{{DataHeader: []byte{1}}, {DataHeader: marshalNativeBatchFrame(1, []byte{1})}}, maximum: 1 << 20, want: "decode MO native result batch"},
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

func fixtureNativeResultPayload() []byte {
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(12)
	typesOut, _ := fixtureOutputShape()
	for i := range typesOut {
		typ := types.T(typesOut[i].Id).ToType()
		typ.Width = typesOut[i].Width
		typ.Scale = typesOut[i].Scale
		typ.Charset = uint8(typesOut[i].Charset)
		typ.SetNotNull(typesOut[i].NotNullable)
		bat.Vecs[i] = vector.NewVec(typ)
	}
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	must(vector.AppendFixed(bat.Vecs[0], true, false, mp))
	must(vector.AppendFixed(bat.Vecs[0], false, true, mp))
	must(vector.AppendFixed(bat.Vecs[1], int8(-8), false, mp))
	must(vector.AppendFixed(bat.Vecs[1], int8(0), true, mp))
	must(vector.AppendFixed(bat.Vecs[2], int16(-16), false, mp))
	must(vector.AppendFixed(bat.Vecs[2], int16(0), true, mp))
	must(vector.AppendFixed(bat.Vecs[3], int32(-32), false, mp))
	must(vector.AppendFixed(bat.Vecs[3], int32(0), true, mp))
	must(vector.AppendFixed(bat.Vecs[4], int64(-64), false, mp))
	must(vector.AppendFixed(bat.Vecs[4], int64(0), true, mp))
	must(vector.AppendFixed(bat.Vecs[5], float32(1.25), false, mp))
	must(vector.AppendFixed(bat.Vecs[5], float32(0), true, mp))
	must(vector.AppendFixed(bat.Vecs[6], 2.5, false, mp))
	must(vector.AppendFixed(bat.Vecs[6], float64(0), true, mp))
	must(vector.AppendBytes(bat.Vecs[7], []byte("tpch"), false, mp))
	must(vector.AppendBytes(bat.Vecs[7], nil, true, mp))
	must(vector.AppendFixed(bat.Vecs[8], types.Decimal64(^uint64(12344)), false, mp))
	must(vector.AppendFixed(bat.Vecs[8], types.Decimal64(0), true, mp))
	must(vector.AppendFixed(bat.Vecs[9], types.Decimal128{B0_63: 7, B64_127: 1}, false, mp))
	must(vector.AppendFixed(bat.Vecs[9], types.Decimal128{}, true, mp))
	must(vector.AppendFixed(bat.Vecs[10], types.DaysFromUnixEpochToDate(1), false, mp))
	must(vector.AppendFixed(bat.Vecs[10], types.Date(0), true, mp))
	must(vector.AppendFixed(bat.Vecs[11], uint32(42), false, mp))
	must(vector.AppendFixed(bat.Vecs[11], uint32(0), true, mp))
	bat.SetRowCount(2)
	payload, err := bat.MarshalBinary()
	bat.Clean(mp)
	must(err)
	if mp.CurrNB() != 0 {
		panic("native result fixture leaked its memory pool")
	}
	return payload
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
			{StreamName: "DoPut", Handler: testDoPut, ServerStreams: true, ClientStreams: true},
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
		command.MaxInputBatchBytes == 0 || len(command.ResultSchema) == 0 ||
		len(command.Plan) == 0 || len(command.QueryID) != 16 || len(command.IdempotencyKey) != sha256.Size || command.AccountID == nil {
		return nil, status.Error(codes.InvalidArgument, "malformed ExecuteSubstrait command")
	}
	server.deadlineUnixMS.Store(int64(command.DeadlineUnixMS))
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
	if !bytes.Equal(server.schema, []byte("bad")) {
		server.schema = bytes.Clone(command.ResultSchema)
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
	if server.doGetStarted != nil {
		server.doGetOnce.Do(func() { close(server.doGetStarted) })
	}
	if server.blockDoGet != nil {
		select {
		case <-server.blockDoGet:
		case <-stream.Context().Done():
			return context.Cause(stream.Context())
		}
	}
	if server.streamMessages != nil {
		for _, message := range server.streamMessages {
			if err := stream.SendMsg(message); err != nil {
				return err
			}
		}
		return nil
	}
	if err := stream.SendMsg(&flightData{DataHeader: []byte{1}}); err != nil {
		return err
	}
	if len(server.body) == 0 {
		return nil
	}
	return stream.SendMsg(&flightData{DataHeader: marshalNativeBatchFrame(1, fixtureNativeResultPayload())})
}

func testDoPut(service any, stream grpc.ServerStream) error {
	server := service.(*testFlightServer)
	first := new(flightData)
	if err := stream.RecvMsg(first); err != nil {
		return err
	}
	request := new(uploadInputRequest)
	if first.Descriptor == nil || first.Descriptor.Type != commandDescriptor ||
		proto.Unmarshal(first.Descriptor.Cmd, request) != nil || len(request.Ticket) != ticketBytes ||
		len(request.StreamRef) != 32 {
		return status.Error(codes.InvalidArgument, "malformed native input descriptor")
	}
	attached, _ := proto.Marshal(&uploadInputAck{Ready: true})
	if err := stream.SendMsg(&flightPutResult{AppMetadata: attached}); err != nil {
		return err
	}
	var batches, rows, bytesSeen uint64
	for {
		message := new(flightData)
		err := stream.RecvMsg(message)
		if err == io.EOF {
			ack, _ := proto.Marshal(&uploadInputAck{AcknowledgedBatches: batches, Rows: rows, Bytes: bytesSeen, Complete: true})
			return stream.SendMsg(&flightPutResult{AppMetadata: ack})
		}
		if err != nil {
			return err
		}
		if len(message.AppMetadata) < nativeBatchFrameHeaderBytes || string(message.AppMetadata[:4]) != "MOB1" {
			return status.Error(codes.InvalidArgument, "malformed native input frame")
		}
		batches++
		server.doPutBatches.Add(1)
		if binary.LittleEndian.Uint64(message.AppMetadata[8:16]) != batches {
			return status.Error(codes.InvalidArgument, "non-contiguous native input sequence")
		}
		payload := message.AppMetadata[nativeBatchFrameHeaderBytes:]
		if server.notNeededInput {
			ack, _ := proto.Marshal(&uploadInputAck{Complete: true, NotNeeded: true})
			return stream.SendMsg(&flightPutResult{AppMetadata: ack})
		}
		rows += binary.LittleEndian.Uint64(payload[:8])
		bytesSeen += uint64(len(payload))
		if server.doPutBatch != nil {
			server.doPutOnce.Do(func() { close(server.doPutBatch) })
		}
		if server.blockDoPutAck != nil {
			select {
			case <-server.blockDoPutAck:
			case <-stream.Context().Done():
				return context.Cause(stream.Context())
			}
		}
		ack, _ := proto.Marshal(&uploadInputAck{AcknowledgedBatches: batches, Rows: rows, Bytes: bytesSeen})
		if err = stream.SendMsg(&flightPutResult{AppMetadata: ack}); err != nil {
			return err
		}
	}
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
	request := new(cancelExecutionRequest)
	if action.Type != "CancelExecution" || proto.Unmarshal(action.Body, request) != nil ||
		(len(request.Ticket) != ticketBytes && len(request.IdempotencyKey) != sha256.Size) ||
		(len(request.Ticket) != 0 && len(request.IdempotencyKey) != 0) {
		return status.Error(codes.InvalidArgument, "malformed cancellation command")
	}
	if len(request.IdempotencyKey) != 0 {
		server.cancelByIdempotency.Add(1)
	}
	if server.cancelFailures.Load() > 0 && server.cancelFailures.Add(-1) >= 0 {
		return status.Error(codes.Unavailable, "transient cleanup failure")
	}
	if server.cancelErr != nil {
		return server.cancelErr
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

func runtimeExecutionCount(runtime *Runtime) int {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return len(runtime.executions)
}

func testFlightRelease(context.Context) error { return nil }

func testFlightDeadline() time.Time { return time.Now().Add(time.Hour) }

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
			{StreamName: "DoPut", Handler: testDoPut, ServerStreams: true, ClientStreams: true},
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
