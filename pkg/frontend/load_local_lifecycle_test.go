// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util"
)

type interruptibleLoadLocalWriter struct {
	*testMysqlWriter
	reading      chan struct{}
	disconnects  atomic.Int32
	interrupts   atomic.Int32
	frees        atomic.Int32
	blockRequest bool
	// interruptGate deterministically parks the cancellation watcher between
	// CloseWithError and InterruptLoadLocalIO, with interruptEntered signaling
	// the park: the test alone decides when the socket interrupt is allowed to
	// land, exposing owner/watcher ordering windows without sleeps.
	interruptEntered chan struct{}
	interruptGate    chan struct{}
}

func (w *interruptibleLoadLocalWriter) WriteLocalInfileRequest(string) error {
	if !w.blockRequest {
		return nil
	}
	w.reading <- struct{}{}
	_, err := w.ioses.conn.Write([]byte{'r'})
	return err
}

func (w *interruptibleLoadLocalWriter) ReadLoadLocalPacket() ([]byte, error) {
	select {
	case w.reading <- struct{}{}:
	default:
	}
	return w.ioses.ReadLoadLocalPacket()
}

func (w *interruptibleLoadLocalWriter) ReadLoadLocalPacketWithTimeout(timeout time.Duration, deadline time.Time) ([]byte, error) {
	select {
	case w.reading <- struct{}{}:
	default:
	}
	return w.ioses.ReadLoadLocalPacketWithTimeout(timeout, deadline)
}

func (w *interruptibleLoadLocalWriter) InterruptLoadLocalIO() {
	w.interrupts.Add(1)
	if w.interruptGate != nil {
		// Deterministic hold: the watcher already retired the pipe (so the
		// owner's pipe write failed) but has not touched the socket yet. The
		// test releases the gate to land the interrupt in the exact window it
		// wants to pin.
		w.interruptEntered <- struct{}{}
		<-w.interruptGate
	}
	w.ioses.InterruptLoadLocalIO()
}

func (w *interruptibleLoadLocalWriter) ClearLoadLocalIOInterrupt() {
	w.ioses.ClearLoadLocalIOInterrupt()
}

func (w *interruptibleLoadLocalWriter) Disconnect() error {
	w.disconnects.Add(1)
	return w.ioses.Disconnect()
}

func (w *interruptibleLoadLocalWriter) FreeLoadLocal() {
	w.ioses.FreeLoadLocal()
	w.frees.Add(1)
}

// Use the actual packet reader and net.Conn cancellation, with no service,
// wall-clock phase trigger, global allocator, or client EOF on failure.
func newInterruptibleLoadLocal(t *testing.T) (*Session, *interruptibleLoadLocalWriter, net.Conn) {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
	sv := &config.FrontendParameters{}
	sv.SetDefaultValues()
	allocator := NewLeakCheckAllocator()
	conn, err := NewIOSessionWithOptions(server, config.NewParameterUnit(sv, nil, nil, nil), "",
		WithIOSessionAllocator(allocator))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		require.True(t, allocator.CheckBalance(), "connection/upload buffers must be released")
	})
	w := &interruptibleLoadLocalWriter{testMysqlWriter: &testMysqlWriter{ioses: conn}, reading: make(chan struct{}, 1)}
	return &Session{feSessionImpl: feSessionImpl{respr: NewMysqlResp(w)}}, w, client
}

func TestProcessLoadLocalProtocolTermination(t *testing.T) {
	for _, name := range []string{"already canceled", "invalid config", "request write cancel", "idle cancel", "partial header cancel", "partial payload cancel", "pipe cancel", "pipe failure", "client disconnect", "EOF and reuse"} {
		t.Run(name, func(t *testing.T) {
			ses, protocol, client := newInterruptibleLoadLocal(t)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			reader, writer := io.Pipe()
			t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
			ec := newTestExecCtx(ctx, gomock.NewController(t))
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "test.csv"}}
			if name == "already canceled" {
				cancel()
			}
			if name == "invalid config" {
				param.Filepath = ""
			}
			protocol.blockRequest = name == "request write cancel"
			done := make(chan error, 1)
			go func() {
				done <- processLoadLocal(ctx, ses, ec, param, writer, reader)
			}()
			if name == "already canceled" || name == "invalid config" {
				select {
				case err := <-done:
					require.Error(t, err)
					if name == "already canceled" {
						require.ErrorIs(t, err, context.Canceled)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("rejected upload did not terminate")
				}
				require.Zero(t, protocol.disconnects.Load())
				require.EqualValues(t, 1, protocol.frees.Load())
				return
			}
			select {
			case <-protocol.reading:
			case <-time.After(5 * time.Second):
				t.Fatal("upload did not enter network read")
			}
			// net.Pipe writes return only after those bytes have been consumed.
			switch name {
			case "partial header cancel":
				_, err := client.Write([]byte{1, 0})
				require.NoError(t, err)
			case "partial payload cancel":
				_, err := client.Write([]byte{2, 0, 0, 1, 'x'})
				require.NoError(t, err)
			case "pipe cancel", "pipe failure":
				_, err := client.Write([]byte{1, 0, 0, 1, 'x'})
				require.NoError(t, err)
			case "client disconnect":
				require.NoError(t, client.Close())
			case "EOF and reuse":
				_, err := client.Write([]byte{0, 0, 0, 1})
				require.NoError(t, err)
			}
			if name == "pipe failure" {
				require.NoError(t, reader.Close())
			} else if name != "client disconnect" && name != "EOF and reuse" {
				cancel()
			}
			var err error
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				_ = client.Close()
				t.Fatal("upload required client EOF to terminate")
			}
			if name == "EOF and reuse" {
				require.NoError(t, err)
				require.Zero(t, protocol.disconnects.Load())
				// A late cancellation must not let the previous watcher close the
				// next command's connection. The watcher is joined before return.
				cancel()
				written := make(chan error, 1)
				go func() { _, err := client.Write([]byte{1, 0, 0, 0, 'q'}); written <- err }()
				payload, err := protocol.ioses.ReadLoadLocalPacket()
				require.NoError(t, err)
				require.Equal(t, []byte{'q'}, payload)
				require.NoError(t, <-written)
			} else {
				require.Error(t, err)
				if name != "pipe failure" && name != "client disconnect" {
					require.ErrorIs(t, err, context.Canceled)
				}
				require.EqualValues(t, 1, protocol.disconnects.Load())
			}
			require.EqualValues(t, 1, protocol.frees.Load())
		})
	}
}

func TestExecuteStatusStmtAbortsStalledLocalUpload(t *testing.T) {
	for _, panicRunner := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panicRunner], func(t *testing.T) {
			ses, protocol, _ := newInterruptibleLoadLocal(t)
			ctrl := gomock.NewController(t)
			ec := newTestExecCtx(context.Background(), ctrl)
			ec.proc = testutil.NewProc(t)
			ec.stmt = &tree.Load{Local: true, Param: &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "test.csv"}}}
			failure := errors.New("runner failed")
			runner := mock_frontend.NewMockComputationRunner(ctrl)
			runner.EXPECT().Run(uint64(0)).DoAndReturn(func(uint64) (*util.RunResult, error) {
				<-protocol.reading
				if panicRunner {
					panic(failure)
				}
				return nil, failure
			})
			ec.runner = runner
			done := make(chan any, 1)
			go func() {
				defer func() {
					if v := recover(); v != nil {
						done <- v
					}
				}()
				done <- executeStatusStmt(ses, ec)
			}()
			select {
			case result := <-done:
				require.Equal(t, failure, result)
			case <-time.After(5 * time.Second):
				t.Fatal("runner cleanup blocked on client upload")
			}
			require.EqualValues(t, 1, protocol.disconnects.Load())
			require.EqualValues(t, 1, protocol.frees.Load())
			require.Nil(t, ec.proc.GetLoadLocalReader())
			require.Nil(t, ec.loadLocalWriter)
		})
	}
}

type failingLocalUploadFS struct {
	fileservice.FileService
	reading    <-chan struct{}
	panicWrite bool
	err        error
}

func (f *failingLocalUploadFS) Delete(context.Context, ...string) error { return nil }
func (f *failingLocalUploadFS) Write(context.Context, fileservice.IOVector) error {
	<-f.reading
	if f.panicWrite {
		panic(f.err)
	}
	return f.err
}

func TestUploadAbortsStalledClientOnStorageFailure(t *testing.T) {
	for _, panicWrite := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panicWrite], func(t *testing.T) {
			ses, protocol, _ := newInterruptibleLoadLocal(t)
			service := t.Name()
			InitServerLevelVars(service)
			t.Cleanup(func() { serverVarsMap.Delete(service) })
			ses.service = service
			failure := errors.New("storage write failed")
			fs := &failingLocalUploadFS{reading: protocol.reading, panicWrite: panicWrite, err: failure}
			pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
			pu.FileService = fs
			setPu(service, pu)
			ec := newTestExecCtx(context.Background(), gomock.NewController(t))
			done := make(chan any, 1)
			go func() {
				defer func() {
					if v := recover(); v != nil {
						done <- v
					}
				}()
				_, err := Upload(ses, ec, "test.py", "test")
				done <- err
			}()
			select {
			case result := <-done:
				require.ErrorIs(t, result.(error), failure)
			case <-time.After(5 * time.Second):
				t.Fatal("storage failure left upload waiting on client EOF")
			}
			require.EqualValues(t, 1, protocol.disconnects.Load())
			require.EqualValues(t, 1, protocol.frees.Load())
		})
	}
}

// A cancellation no longer disconnects mid-upload: the owner interrupts the
// in-flight read and drains a finite client stream to its protocol EOF, so the
// connection survives. A silent stream is retired by the drain's per-read
// budget instead. Both paths synchronize on the owner's network reads.
func TestProcessLoadLocalDrainAfterCancel(t *testing.T) {
	for _, name := range []string{"finite tail reaches EOF", "stalled stream forced retire"} {
		t.Run(name, func(t *testing.T) {
			ses, protocol, client := newInterruptibleLoadLocal(t)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			reader, writer := io.Pipe()
			t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
			ec := newTestExecCtx(ctx, gomock.NewController(t))
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "test.csv"}}
			done := make(chan error, 1)
			go func() {
				done <- processLoadLocal(ctx, ses, ec, param, writer, reader)
			}()
			select {
			case <-protocol.reading:
			case <-time.After(5 * time.Second):
				t.Fatal("upload did not enter network read")
			}
			cancel()
			if name == "finite tail reaches EOF" {
				// The drain parks in its own read before the client tail is
				// written; the packets below then reach the drained connection
				// in upload framing and end at the protocol EOF.
				select {
				case <-protocol.reading:
				case <-time.After(5 * time.Second):
					t.Fatal("cancellation did not enter the drain read")
				}
				// net.Pipe writes return only after those bytes have been consumed.
				_, err := client.Write([]byte{1, 0, 0, 1, 'x'})
				require.NoError(t, err)
				_, err = client.Write([]byte{0, 0, 0, 2})
				require.NoError(t, err)
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("cancelled upload did not terminate")
			}
			require.EqualValues(t, 1, protocol.interrupts.Load())
			if name == "finite tail reaches EOF" {
				require.Zero(t, protocol.disconnects.Load())
				// The drained connection serves the next command.
				written := make(chan error, 1)
				go func() { _, err := client.Write([]byte{1, 0, 0, 0, 'q'}); written <- err }()
				payload, err := protocol.ioses.ReadLoadLocalPacket()
				require.NoError(t, err)
				require.Equal(t, []byte{'q'}, payload)
				require.NoError(t, <-written)
			} else {
				require.EqualValues(t, 1, protocol.disconnects.Load())
			}
			require.EqualValues(t, 1, protocol.frees.Load())
		})
	}
}

// An ordinary statement-side error that is NOT a cancellation (here: lock
// table bind changed mid-upload, the #30640 2013 flavor) must also resync the
// finite client stream at its protocol EOF: the original SQL error, not 2013,
// answers the command and the drained connection serves the next command.
func TestProcessLoadLocalDrainsOrdinaryErrorToEOF(t *testing.T) {
	ses, protocol, client := newInterruptibleLoadLocal(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ec := newTestExecCtx(ctx, ctrl)
	ec.proc = testutil.NewProc(t)
	expected := moerr.NewLockTableBindChangedNoCtx()
	op := newTestTxnOp()
	op.checkLockTableBinds = func(context.Context) error {
		if op.checkLockTableChecks == 2 {
			return expected
		}
		return nil
	}
	ec.proc.Base.TxnOperator = op
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "test.csv"}}
	reader, writer := io.Pipe()
	// Consume anything the loop pipes through so writer.Write never blocks the
	// upload owner.
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		buf := make([]byte, 64)
		for {
			if _, err := reader.Read(buf); err != nil {
				return
			}
		}
	}()
	done := make(chan error, 1)
	go func() {
		done <- processLoadLocal(ctx, ses, ec, param, writer, reader)
	}()
	select {
	case <-protocol.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not enter network read")
	}
	// Finite upload: iteration 1 consumes the first packet, the second bind
	// check then fails, and the drain resyncs the remainder to protocol EOF.
	written := make(chan error, 1)
	go func() {
		if _, err := client.Write([]byte{1, 0, 0, 1, 'x'}); err != nil {
			written <- err
			return
		}
		if _, err := client.Write([]byte{1, 0, 0, 2, 'y'}); err != nil {
			written <- err
			return
		}
		_, err := client.Write([]byte{0, 0, 0, 3})
		written <- err
	}()
	select {
	case err := <-done:
		require.Equal(t, expected, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary error left the upload waiting on client EOF")
	}
	// No cancellation happened, so no deadline interrupt was needed; the
	// drained connection survived.
	require.Zero(t, protocol.interrupts.Load())
	require.Zero(t, protocol.disconnects.Load())
	require.EqualValues(t, 1, protocol.frees.Load())
	require.NoError(t, <-written)
	// The drained connection serves the next command.
	nextWritten := make(chan error, 1)
	go func() { _, err := client.Write([]byte{1, 0, 0, 0, 'q'}); nextWritten <- err }()
	payload, err := protocol.ioses.ReadLoadLocalPacket()
	require.NoError(t, err)
	require.Equal(t, []byte{'q'}, payload)
	require.NoError(t, <-nextWritten)
	<-consumed
}

// A parser-retired pipe mid-upload is an ordinary (non-cancel) error too: the
// over-budget record flavor of #30640 reached the mysql client as 2013
// because the upload owner returned without draining. The owner must resync
// the finite client stream at its protocol EOF, return the pipe's error, and
// serve the next command on the same connection.
func TestProcessLoadLocalDrainsOrdinaryPipeErrorToEOF(t *testing.T) {
	ses, protocol, client := newInterruptibleLoadLocal(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ec := newTestExecCtx(ctx, ctrl)
	ec.proc = testutil.NewProc(t)
	op := newTestTxnOp()
	op.checkLockTableBinds = func(context.Context) error { return nil }
	ec.proc.Base.TxnOperator = op
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "over.csv"}}
	reader, writer := io.Pipe()
	// Consume anything the loop pipes through so writer.Write never blocks the
	// upload owner.
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		buf := make([]byte, 64)
		for {
			if _, err := reader.Read(buf); err != nil {
				return
			}
		}
	}()
	done := make(chan error, 1)
	go func() {
		done <- processLoadLocal(ctx, ses, ec, param, writer, reader)
	}()
	select {
	case <-protocol.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not enter network read")
	}
	// The parser side retires the pipe mid-upload (e.g. an over-budget record
	// rejected after ~64MiB). This is an ordinary statement error — ctx is
	// never cancelled — so the owner must drain before answering.
	pipeErr := errors.New("csv record size exceeds max record size")
	_ = reader.CloseWithError(pipeErr)
	written := make(chan error, 1)
	go func() {
		if _, err := client.Write([]byte{1, 0, 0, 1, 'x'}); err != nil {
			written <- err
			return
		}
		if _, err := client.Write([]byte{1, 0, 0, 2, 'y'}); err != nil {
			written <- err
			return
		}
		_, err := client.Write([]byte{0, 0, 0, 3})
		written <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, pipeErr)
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary pipe error left the upload waiting on client EOF")
	}
	// No cancellation happened, so no deadline interrupt was needed; the
	// drained connection survived.
	require.Zero(t, protocol.interrupts.Load())
	require.Zero(t, protocol.disconnects.Load())
	require.EqualValues(t, 1, protocol.frees.Load())
	require.NoError(t, <-written)
	// The drained connection serves the next command.
	nextWritten := make(chan error, 1)
	go func() { _, err := client.Write([]byte{1, 0, 0, 0, 'q'}); nextWritten <- err }()
	payload, err := protocol.ioses.ReadLoadLocalPacket()
	require.NoError(t, err)
	require.Equal(t, []byte{'q'}, payload)
	require.NoError(t, <-nextWritten)
	<-consumed
}

// The #30640 contract: runner failure cancels the upload, the owner drains the
// finite client stream to its protocol EOF instead of disconnecting, and the
// statement still returns the runner's own error on a reusable connection.
func TestExecuteStatusStmtDrainsFiniteLocalUpload(t *testing.T) {
	ses, protocol, client := newInterruptibleLoadLocal(t)
	ctrl := gomock.NewController(t)
	ec := newTestExecCtx(context.Background(), ctrl)
	ec.proc = testutil.NewProc(t)
	ec.stmt = &tree.Load{Local: true, Param: &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "test.csv"}}}
	failure := errors.New("runner failed")
	runner := mock_frontend.NewMockComputationRunner(ctrl)
	uploading := make(chan struct{})
	serving := make(chan struct{})
	runner.EXPECT().Run(uint64(0)).DoAndReturn(func(uint64) (*util.RunResult, error) {
		<-protocol.reading // upload owner parked in its first network read
		close(uploading)
		<-serving // client tail in flight; then fail the load
		return nil, failure
	})
	ec.runner = runner
	done := make(chan any, 1)
	go func() {
		done <- executeStatusStmt(ses, ec)
	}()
	<-uploading
	close(serving)
	// Finite client: synchronize on the drain's own network read before
	// feeding the tail, so each net.Pipe write pairs with a parked drain read
	// instead of racing the per-read drain budget on a loaded race build.
	select {
	case <-protocol.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not enter the drain read")
	}
	_, err := client.Write([]byte{1, 0, 0, 1, 'x'})
	require.NoError(t, err)
	_, err = client.Write([]byte{0, 0, 0, 2})
	require.NoError(t, err)
	select {
	case result := <-done:
		require.Equal(t, failure, result)
	case <-time.After(5 * time.Second):
		t.Fatal("runner cleanup blocked on client upload")
	}
	require.EqualValues(t, 1, protocol.interrupts.Load())
	require.Zero(t, protocol.disconnects.Load())
	require.EqualValues(t, 1, protocol.frees.Load())
	require.Nil(t, ec.proc.GetLoadLocalReader())
	require.Nil(t, ec.loadLocalWriter)
	// The drained connection serves the next command.
	written := make(chan error, 1)
	go func() { _, err := client.Write([]byte{1, 0, 0, 0, 'q'}); written <- err }()
	payload, err := protocol.ioses.ReadLoadLocalPacket()
	require.NoError(t, err)
	require.Equal(t, []byte{'q'}, payload)
	require.NoError(t, <-written)
}

// The production chain of #30640: sessions are answered through
// MysqlResp -> *MysqlProtocolImpl -> *Conn, and the upload owner asserts the
// drain contract on that chain — not on a test stroke. The 2013 escaped the
// earlier suites because only the stroke implemented the interface while the
// real protocol did not. Wire the real chain end to end: assert the
// interface, drain a finite upload to its protocol EOF through the protocol,
// then reuse the same connection for the next command. The compile-time
// guard next to loadLocalDrainCapable pins the same contract at build time.
func TestProductionMysqlRrWrDrainsFiniteUpload(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
	sv := &config.FrontendParameters{}
	sv.SetDefaultValues()
	allocator := NewLeakCheckAllocator()
	conn, err := NewIOSessionWithOptions(server, config.NewParameterUnit(sv, nil, nil, nil), "",
		WithIOSessionAllocator(allocator))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		require.True(t, allocator.CheckBalance(), "connection/upload buffers must be released")
	})
	protocol := &MysqlProtocolImpl{tcpConn: conn}
	ses := &Session{feSessionImpl: feSessionImpl{respr: NewMysqlResp(protocol)}}
	drainer, ok := ses.GetResponser().MysqlRrWr().(loadLocalDrainCapable)
	require.True(t, ok, "production MysqlRrWr chain must expose the drain contract")

	ctx := context.Background()
	ctrl := gomock.NewController(t)
	ec := newTestExecCtx(ctx, ctrl)
	drained := make(chan bool, 1)
	go func() { drained <- drainLoadLocalUpload(ses, ec, drainer) }()
	// Finite client: one data packet, then the empty protocol EOF packet.
	_, err = client.Write([]byte{1, 0, 0, 1, 'x'})
	require.NoError(t, err)
	_, err = client.Write([]byte{0, 0, 0, 2})
	require.NoError(t, err)
	select {
	case eof := <-drained:
		require.True(t, eof)
	case <-time.After(5 * time.Second):
		t.Fatal("real protocol chain did not drain to upload EOF")
	}
	conn.FreeLoadLocal()
	// The drained connection serves the next command.
	written := make(chan error, 1)
	go func() { _, err := client.Write([]byte{1, 0, 0, 0, 'q'}); written <- err }()
	payload, err := protocol.ReadLoadLocalPacket()
	require.NoError(t, err)
	require.Equal(t, []byte{'q'}, payload)
	require.NoError(t, <-written)
}

// A single stalling packet must not outlive the drain's total budget: the
// client completes the header (announcing a large payload) and then trickles
// one byte inside every per-read timeout. The absolute deadline is now
// installed on every network read, so the drain retires the connection
// promptly; with the per-read-only check a 16MiB payload could hang for days.
func TestDrainAbsoluteDeadlineBoundsMidPacketTrickle(t *testing.T) {
	oldReadTimeout, oldMaxWait := loadLocalDrainReadTimeout, loadLocalDrainMaxWait
	loadLocalDrainReadTimeout = time.Second
	loadLocalDrainMaxWait = 300 * time.Millisecond
	defer func() { loadLocalDrainReadTimeout, loadLocalDrainMaxWait = oldReadTimeout, oldMaxWait }()

	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	require.NoError(t, client.SetDeadline(time.Now().Add(10*time.Second)))
	sv := &config.FrontendParameters{}
	sv.SetDefaultValues()
	allocator := NewLeakCheckAllocator()
	conn, err := NewIOSessionWithOptions(server, config.NewParameterUnit(sv, nil, nil, nil), "",
		WithIOSessionAllocator(allocator))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		require.True(t, allocator.CheckBalance(), "connection/upload buffers must be released")
	})
	ses := &Session{feSessionImpl: feSessionImpl{respr: NewMysqlResp(&MysqlProtocolImpl{tcpConn: conn})}}
	ctrl := gomock.NewController(t)
	ec := newTestExecCtx(context.Background(), ctrl)
	quit := make(chan struct{})
	go func() {
		// Header announces a 512-byte payload, then trickle a byte at a time
		// well inside the per-read timeout (30ms << 1s).
		if _, err := client.Write([]byte{0, 2, 0, 1}); err != nil {
			return
		}
		for i := 0; i < 512; i++ {
			select {
			case <-quit:
				return
			default:
			}
			if _, err := client.Write([]byte{'x'}); err != nil {
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
	}()
	start := time.Now()
	eof := drainLoadLocalUpload(ses, ec, conn)
	close(quit)
	require.False(t, eof, "a trickled mid-packet stream must retire the drain")
	require.Less(t, time.Since(start), 5*time.Second,
		"the total drain budget must bound a single stalling packet")
}

// A cancellation interrupt that lands BEFORE the next packet read installs
// its own deadline must not be covered by that fresh deadline: without the
// interrupt flag (#30640), a racing watcher could expire the socket, the
// owner would then install a long deadline over it, and a trickled partial
// read would stall the statement for the whole session timeout. The flag
// makes every later read fail fast until the owner clears it to drain or
// reuse the connection.
func TestLoadLocalInterruptSurvivesLateReadArm(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	require.NoError(t, client.SetDeadline(time.Now().Add(10*time.Second)))
	sv := &config.FrontendParameters{}
	sv.SetDefaultValues()
	allocator := NewLeakCheckAllocator()
	conn, err := NewIOSessionWithOptions(server, config.NewParameterUnit(sv, nil, nil, nil), "",
		WithIOSessionAllocator(allocator))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		require.True(t, allocator.CheckBalance(), "connection/upload buffers must be released")
	})
	// No read is in flight yet: the watcher wins the race to the socket.
	conn.InterruptLoadLocalIO()
	done := make(chan error, 1)
	go func() {
		_, err := conn.ReadLoadLocalPacketWithTimeout(24*time.Hour, time.Time{})
		done <- err
	}()
	select {
	case err := <-done:
		require.Error(t, err)
		var netErr net.Error
		require.ErrorAs(t, err, &netErr)
		require.True(t, netErr.Timeout(), "interrupted read must fail fast, got %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("read armed after an interrupt ignored the cancellation")
	}
	// The owner lifts the interrupt and the connection still serves reads:
	// a leftover interrupt would poison the next command on a reused session.
	conn.ClearLoadLocalIOInterrupt()
	written := make(chan error, 1)
	go func() { _, err := client.Write([]byte{1, 0, 0, 0, 'q'}); written <- err }()
	payload, err := conn.ReadLoadLocalPacket()
	require.NoError(t, err)
	require.Equal(t, []byte{'q'}, payload)
	require.NoError(t, <-written)
}

// A late watcher InterruptLoadLocalIO landing after the owner had lifted the
// interrupt used to expire the drain's deadline the moment it armed: the 65MiB
// over-budget upload still became 2013 instead of the resource error (the
// drain was retired 251µs after the statement error). This reproduces the
// exact ordering deterministically — the stroke gate parks the watcher between
// CloseWithError (which unblocks the owner's pipe write) and the socket
// interrupt — and pins the fixed owner behaviour: it joins the watcher BEFORE
// lifting the interrupt, so the interrupt lands first, is cleared once, and
// the finite client tail drains to the protocol EOF.
func TestProcessLoadLocalJoinsWatcherBeforeDrainInterrupt(t *testing.T) {
	ses, protocol, client := newInterruptibleLoadLocal(t)
	protocol.interruptEntered = make(chan struct{}, 1)
	protocol.interruptGate = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ec := newTestExecCtx(ctx, gomock.NewController(t))
	param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "test.csv"}}
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	done := make(chan error, 1)
	go func() { done <- processLoadLocal(ctx, ses, ec, param, writer, reader) }()
	// Nobody consumes the pipe: once the owner has read the first uploaded
	// packet it blocks in writer.Write until the pipe is retired by the
	// watcher.
	_, err := client.Write([]byte{1, 0, 0, 1, 'x'})
	require.NoError(t, err)
	select {
	case <-protocol.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not enter its first network read")
	}
	cancel()
	// The watcher is now parked between CloseWithError — which already failed
	// the owner's blocked pipe write — and its socket interrupt. The owner's
	// very next step is either the (broken) lift-and-arm of the drain or the
	// (fixed) join of this watcher.
	select {
	case <-protocol.interruptEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher never reached its socket interrupt")
	}
	// Allow the interrupt. Lift-then-drain would now be retired by it; the
	// fixed owner waits for it first, then lifts once and drains the finite
	// tail to the protocol EOF.
	close(protocol.interruptGate)
	_, err = client.Write([]byte{1, 0, 0, 2, 'y'})
	require.NoError(t, err)
	_, err = client.Write([]byte{0, 0, 0, 3})
	require.NoError(t, err)
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("late watcher interrupt still retired the finite drain")
	}
	require.EqualValues(t, 1, protocol.interrupts.Load())
	require.Zero(t, protocol.disconnects.Load())
	require.EqualValues(t, 1, protocol.frees.Load())
	// The drained connection serves the next command.
	written := make(chan error, 1)
	go func() { _, err := client.Write([]byte{1, 0, 0, 0, 'q'}); written <- err }()
	payload, err := protocol.ioses.ReadLoadLocalPacket()
	require.NoError(t, err)
	require.Equal(t, []byte{'q'}, payload)
	require.NoError(t, <-written)
}

// gatedReadDeadlineConn forces the old check-then-arm window at the real
// SetReadDeadline boundary without sleeps: the blockAt-th long future deadline
// install is held (signalled via entered) until the test releases it, so a
// cancellation interrupt can be fired strictly between the reader's flag check
// and the actual deadline install.
type gatedReadDeadlineConn struct {
	net.Conn
	arms    atomic.Int32
	blockAt int32
	entered chan struct{}
	release chan struct{}
}

// Only a long future read deadline is gated: the watcher's expiry (≈now) and
// the owner's clear (zero) pass straight through.
func (g *gatedReadDeadlineConn) SetReadDeadline(t time.Time) error {
	if !t.IsZero() && time.Until(t) > time.Minute && g.arms.Add(1) == g.blockAt {
		g.entered <- struct{}{}
		<-g.release
	}
	return g.Conn.SetReadDeadline(t)
}

// The old deadline-arm window: the reader loaded the interrupt flag, then
// continued into SetReadDeadline; a cancellation interrupt landing inside that
// window was overwritten by the install, and the silent read hung for the
// whole (configurable) session timeout. The gate forces the exact window for
// a header read, a partial header read, and a partial payload read: the read
// now unblocks deterministically on the interrupt in every variant, and a
// cleared connection serves the next read.
func TestLoadLocalInterruptCoversDeadlineArmWindow(t *testing.T) {
	for _, name := range []string{"header read", "partial header read", "partial payload read"} {
		t.Run(name, func(t *testing.T) {
			var blockAt int32
			switch name {
			case "header read":
				blockAt = 1
			case "partial header read":
				blockAt = 2
			case "partial payload read":
				blockAt = 3
			}
			server, client := net.Pipe()
			gated := &gatedReadDeadlineConn{Conn: server, blockAt: blockAt, entered: make(chan struct{}, 1), release: make(chan struct{})}
			t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
			require.NoError(t, client.SetDeadline(time.Now().Add(10*time.Second)))
			sv := &config.FrontendParameters{}
			sv.SetDefaultValues()
			allocator := NewLeakCheckAllocator()
			conn, err := NewIOSessionWithOptions(gated, config.NewParameterUnit(sv, nil, nil, nil), "",
				WithIOSessionAllocator(allocator))
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, conn.Close())
				require.True(t, allocator.CheckBalance(), "connection/upload buffers must be released")
			})
			// The consuming reads pair these net.Pipe writes in order: a plain
			// header read consumes nothing upfront, a partial header read one
			// short header, and a partial payload read a header plus half the
			// payload — leaving the gated arm (blockAt-th) silent in all three.
			switch name {
			case "partial header read":
				go func() { _, _ = client.Write([]byte{2, 0}) }()
			case "partial payload read":
				go func() {
					if _, err := client.Write([]byte{8, 0, 0, 1}); err != nil {
						return
					}
					_, _ = client.Write([]byte{'a', 'b', 'c'})
				}()
			}
			done := make(chan error, 1)
			go func() {
				_, err := conn.ReadLoadLocalPacketWithTimeout(24*time.Hour, time.Time{})
				done <- err
			}()
			select {
			case <-gated.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("deadline arm never reached the network boundary")
			}
			// Fire the watcher strictly inside the arm: without the mutex the
			// install following the release would overwrite the interrupt's
			// expiry and pin the silent read for 24h (test timeout).
			go conn.InterruptLoadLocalIO()
			close(gated.release)
			select {
			case err := <-done:
				require.Error(t, err)
				var netErr net.Error
				require.ErrorAs(t, err, &netErr)
				require.True(t, netErr.Timeout(), "interrupt lost in the arm window, got: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("interrupt lost in the arm window: 24h read did not die")
			}
			conn.ClearLoadLocalIOInterrupt()
			// A leftover interrupt would poison the next command on a reused
			// session; the cleared connection reads normally again.
			written := make(chan error, 1)
			go func() { _, err := client.Write([]byte{1, 0, 0, 0, 'q'}); written <- err }()
			payload, err := conn.ReadLoadLocalPacket()
			require.NoError(t, err)
			require.Equal(t, []byte{'q'}, payload)
			require.NoError(t, <-written)
		})
	}
}
