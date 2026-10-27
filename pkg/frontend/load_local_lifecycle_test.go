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
	"sync"
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
	// CloseWithError and InterruptLoadLocalIO. interruptEntered signals the
	// park, interruptApplied is CLOSED after the real interrupt executed, and
	// gateReleased guards the single release. An owner read that arrives while
	// the gate is still held (i.e. it raced ahead of the watcher) releases the
	// interrupt itself and waits for it — the live-failure order — so RED vs
	// GREEN outcomes are scheduling-independent.
	interruptEntered chan struct{}
	interruptGate    chan struct{}
	interruptApplied chan struct{}
	gateReleased     atomic.Bool
	// watcherParked marks that InterruptLoadLocalIO is actually held in the
	// gate; only then may a racing owner read release and await it (plain
	// upload traffic far earlier must pass through untouched).
	watcherParked atomic.Bool
	appliedOnce   sync.Once
	// windowHit fires the first time an owner action (clear or network read)
	// arrives while the watcher still holds the interrupt: the observable
	// marker that the owner raced ahead of the cancellation watcher.
	windowHit chan struct{}
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
	w.awaitPendingInterruptBeforeRead()
	return w.ioses.ReadLoadLocalPacket()
}

func (w *interruptibleLoadLocalWriter) ReadLoadLocalPacketWithTimeout(timeout time.Duration, deadline time.Time) ([]byte, error) {
	select {
	case w.reading <- struct{}{}:
	default:
	}
	w.awaitPendingInterruptBeforeRead()
	return w.ioses.ReadLoadLocalPacketWithTimeout(timeout, deadline)
}

func (w *interruptibleLoadLocalWriter) InterruptLoadLocalIO() {
	w.interrupts.Add(1)
	if w.interruptGate != nil {
		// Deterministic hold: the watcher already retired the pipe (so the
		// owner's pipe write failed) but has not touched the socket yet. The
		// release decides which window the interrupt lands in.
		w.interruptEntered <- struct{}{}
		w.watcherParked.Store(true)
		<-w.interruptGate
		w.ioses.InterruptLoadLocalIO()
		w.appliedOnce.Do(func() { close(w.interruptApplied) })
		return
	}
	w.ioses.InterruptLoadLocalIO()
}

// releaseInterruptGate closes the watcher's gate exactly once; it is the only
// way the parked InterruptLoadLocalIO may proceed to touch the socket.
func (w *interruptibleLoadLocalWriter) releaseInterruptGate() {
	if w.interruptGate != nil && w.gateReleased.CompareAndSwap(false, true) {
		close(w.interruptGate)
	}
}

// noteOwnerActionWhileWatcherPending observes an owner action (interrupt
// clear or network read) that arrived while the cancellation watcher still
// holds the socket interrupt. That very order — owner ahead of the watcher —
// is the #30640 ordering race: signal windowHit, release the interrupt itself,
// and block the action until the interrupt has executed, so the caller resumes
// in exactly the live-failure order, scheduling-free.
func (w *interruptibleLoadLocalWriter) noteOwnerActionWhileWatcherPending() {
	if w.interruptGate == nil || w.gateReleased.Load() || !w.watcherParked.Load() {
		return
	}
	if w.windowHit != nil {
		select {
		case w.windowHit <- struct{}{}:
		default:
		}
	}
	w.releaseInterruptGate()
	<-w.interruptApplied
}

// awaitPendingInterruptBeforeRead catches a broken owner whose next action is
// a network read; the clear path below catches a clear-first order.
func (w *interruptibleLoadLocalWriter) awaitPendingInterruptBeforeRead() {
	w.noteOwnerActionWhileWatcherPending()
}

func (w *interruptibleLoadLocalWriter) ClearLoadLocalIOInterrupt() {
	// An owner that clears a still-pending watcher interrupt is exactly the
	// missing join: forced into the live-failure order here.
	w.noteOwnerActionWhileWatcherPending()
	w.ioses.ClearLoadLocalIOInterrupt()
}

// LoadLocalFramingBroken surfaces the real connection's lost-packet-boundary
// state so the drain tests assert the retire decision taken on it.
func (w *interruptibleLoadLocalWriter) LoadLocalFramingBroken() bool {
	return w.ioses.LoadLocalFramingBroken()
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
	// Cleanup-based restore: it must also run when an assertion Fatals.
	t.Cleanup(func() { loadLocalDrainReadTimeout, loadLocalDrainMaxWait = oldReadTimeout, oldMaxWait })
	loadLocalDrainReadTimeout = time.Second
	loadLocalDrainMaxWait = 300 * time.Millisecond

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
	protocol.interruptApplied = make(chan struct{}, 1)
	protocol.windowHit = make(chan struct{}, 1)
	// One-shot release, registered immediately so a Fatal-path also unblocks
	// the parked watcher. Registered after (i.e. running before, LIFO) the
	// connection-closing cleanups of newInterruptibleLoadLocal, the resumed
	// watcher can finish its interrupt while the socket is still alive and the
	// owner can finish joining it; every goroutine this test starts exits.
	releaseInterruptGate := protocol.releaseInterruptGate
	t.Cleanup(releaseInterruptGate)
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
	// Owner/watcher order oracle: if ANY owner action (clear or network read)
	// arrives while the watcher still holds the interrupt, the stroke fires
	// windowHit, releases the interrupt itself, and runs that action only
	// after the interrupt executed — the exact live-failure order. Silence
	// here is the fixed behaviour: the owner is parked in its watcher join, so
	// the test releases the gate (a bounded negative proof, not a race).
	select {
	case <-protocol.windowHit:
	case <-time.After(3 * time.Second):
		releaseInterruptGate()
	}
	select {
	case <-protocol.interruptApplied:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher interrupt did not execute")
	}
	// A broken owner has already returned here: the interrupt that landed on
	// its premature action killed the drain and the upload was reported over
	// a widowed connection. Detect that immediately, else feed the tail.
	select {
	case err := <-done:
		t.Fatalf("upload returned before the finite tail passed (interrupt hit the owner action): %v", err)
	default:
	}
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
	// Ordered-closure oracle: no owner action ever raced a held watcher.
	select {
	case <-protocol.windowHit:
		t.Fatal("owner touched the interrupt plan before the watcher was joined")
	default:
	}
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
			// One-shot arm-gate release: a Fatal before the happy-path release
			// must not leave the gated deadline-arm goroutine parked. Registered
			// after (so running before, LIFO) the conn/client closing cleanups,
			// the resumed arm still sees a live socket.
			var releaseArmOnce sync.Once
			releaseArmGate := func() { releaseArmOnce.Do(func() { close(gated.release) }) }
			t.Cleanup(releaseArmGate)
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
			interruptDone := make(chan struct{}, 1)
			go func() { conn.InterruptLoadLocalIO(); interruptDone <- struct{}{} }()
			releaseArmGate()
			// The interrupt must actually EXECUTE: its expiry is the causal
			// reason the armed read below may die. An interrupt that fires
			// "eventually, sometime" cannot explain this read's outcome, so the
			// barrier binds the claim instead of assuming goroutine order.
			select {
			case <-interruptDone:
			case <-time.After(5 * time.Second):
				t.Fatal("cancellation interrupt never executed")
			}
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

// A cancellation that lands while the upload packet reader is mid-frame —
// part of the header or part of the payload already consumed — leaves the
// connection's packet boundary unknown. The owner must retire that
// connection: every byte after it still belongs to the interrupted frame and
// must never be re-framed as an upload EOF (which would authorize reuse) nor
// decoded as the next command. The watcher gate fixes the order deterministically:
// the socket interrupt executes strictly before the masquerade tail is offered,
// so the offered bytes can only be consumed by a drain that forgot the partial
// frame — never by the interrupted read itself.
func TestProcessLoadLocalBrokenFrameCancelRetiresConn(t *testing.T) {
	for _, name := range []string{"partial header", "partial payload"} {
		t.Run(name, func(t *testing.T) {
			ses, protocol, client := newInterruptibleLoadLocal(t)
			protocol.interruptEntered = make(chan struct{}, 1)
			protocol.interruptGate = make(chan struct{})
			protocol.interruptApplied = make(chan struct{}, 1)
			releaseInterruptGate := protocol.releaseInterruptGate
			t.Cleanup(releaseInterruptGate)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			ec := newTestExecCtx(ctx, gomock.NewController(t))
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "test.csv"}}
			reader, writer := io.Pipe()
			t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
			done := make(chan error, 1)
			go func() { done <- processLoadLocal(ctx, ses, ec, param, writer, reader) }()
			select {
			case <-protocol.reading:
			case <-time.After(5 * time.Second):
				t.Fatal("upload did not enter network read")
			}
			// net.Pipe writes return only after those bytes have been consumed:
			// the owner is now parked INSIDE one upload frame.
			if name == "partial header" {
				_, err := client.Write([]byte{1, 0}) // 2 of 4 header bytes
				require.NoError(t, err)
			} else {
				// Full header (8-byte payload announced) plus 2 payload bytes:
				// 6 more payload bytes were still owed when the interrupt hit.
				_, err := client.Write([]byte{8, 0, 0, 1, 's', 'e'})
				require.NoError(t, err)
			}
			cancel()
			select {
			case <-protocol.interruptEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("watcher never reached its socket interrupt")
			}
			releaseInterruptGate()
			select {
			case <-protocol.interruptApplied:
			case <-time.After(5 * time.Second):
				t.Fatal("watcher interrupt did not execute")
			}
			// The socket interrupt has executed: the interrupted read failed
			// mid-frame and NO server read is in flight. Offer the masquerade
			// tail: an EMPTY upload packet (the protocol EOF marker) that a
			// boundary-forgetting drain accepts as EOF to justify reuse. Its
			// write result is deliberately not asserted: a retired connection
			// fails it, a desynced drain consumes it.
			_, _ = client.Write([]byte{0, 0, 0, 5})
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("cancelled upload did not terminate")
			}
			require.EqualValues(t, 1, protocol.disconnects.Load(),
				"an upload interrupted mid-frame must retire the connection: its packet boundary is unknown")
			require.EqualValues(t, 1, protocol.frees.Load())
			// File bytes must never surface as the payload of a later packet:
			// offer what a reused connection would decode as the next command.
			written := make(chan error, 1)
			go func() {
				_, err := client.Write([]byte{0x0b, 0, 0, 0, 0x03, 's', 'e', 'l', 'e', 'c', 't', ' ', '1', '2', '3'})
				written <- err
			}()
			payload, err := protocol.ioses.ReadLoadLocalPacketWithTimeout(300*time.Millisecond, time.Time{})
			require.Error(t, err, "a retired connection must not decode further client bytes")
			require.Empty(t, payload, "upload file bytes must never be decoded as a command packet")
			select {
			case err := <-written:
				require.Error(t, err, "client writes must fail once the connection is retired")
			case <-time.After(5 * time.Second):
				t.Fatal("client write did not settle")
			}
		})
	}
}

// The lost-boundary contract must hold through the production chain, not just
// the test stroke: MysqlResp -> *MysqlProtocolImpl -> *Conn. Drive a partial
// frame through the real proxy chain — a header or payload prefix consumed by
// a read that then dies — and pin that the protocol reports the lost boundary
// and the drain refuses to resync WITHOUT consuming even an offered fake EOF;
// the offered tail settles only when the connection is retired. The
// safe-boundary reuse happy path through the same chain stays pinned by
// TestProductionMysqlRrWrDrainsFiniteUpload.
func TestDrainRefusesBrokenFrameThroughProductionChain(t *testing.T) {
	for _, name := range []string{"partial header", "partial payload"} {
		t.Run(name, func(t *testing.T) {
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
			ec := newTestExecCtx(context.Background(), gomock.NewController(t))

			// Offer only a frame prefix; the read consumes it and dies on the
			// per-read timeout with the frame unfinished, like a cancellation
			// interrupt landing mid-frame.
			written := make(chan error, 1)
			if name == "partial header" {
				go func() { _, err := client.Write([]byte{8, 0}); written <- err }()
			} else {
				go func() { _, err := client.Write([]byte{8, 0, 0, 1, 's', 'e'}); written <- err }()
			}
			_, err = protocol.ReadLoadLocalPacketWithTimeout(200*time.Millisecond, time.Time{})
			require.Error(t, err, "the unfinished frame must fail, not linger")
			require.NoError(t, <-written)
			require.True(t, protocol.LoadLocalFramingBroken(),
				"a read failed mid-frame must mark the packet boundary as lost")

			// Offer the fake EOF tail BEFORE the drain runs: if any drain byte
			// were re-framed as the protocol EOF marker, this write would
			// complete and the drain would report a resync.
			tail := make(chan error, 1)
			go func() { _, err := client.Write([]byte{0, 0, 0, 5}); tail <- err }()
			drained := make(chan bool, 1)
			go func() { drained <- drainLoadLocalUpload(ses, ec, protocol) }()
			select {
			case eof := <-drained:
				require.False(t, eof, "a lost packet boundary must retire the connection, not drain it")
			case <-time.After(5 * time.Second):
				t.Fatal("drain parked on a connection whose boundary is unknown")
			}
			select {
			case <-tail:
				t.Fatal("drain consumed file bytes after the frame boundary was lost")
			default:
			}
			require.NoError(t, protocol.Disconnect())
			require.Error(t, <-tail, "the offered tail must die with the retired connection")
		})
	}
}

// scriptedTimeoutError is the net.Error read failure a deadline/interrupt
// produces; it deliberately satisfies net.Error so production timeout
// classification keeps treating it like a real expiry.
type scriptedTimeoutError struct{}

func (e scriptedTimeoutError) Error() string   { return "scripted partial read expiry" }
func (e scriptedTimeoutError) Timeout() bool   { return true }
func (e scriptedTimeoutError) Temporary() bool { return true }

// dataErrConn scripts the first len(script) underlying Conn.Read calls and
// passes everything else through to the wrapped connection. A scripted read
// returns its staged bytes AND its error in the SAME call — an explicitly
// legal io.Reader/net.Conn shape (n > 0, err != nil), e.g. bytes copied out
// before a deadline fires — which a byte counter that only adds the count on
// success silently drops. The staging is at the transport edge only; the
// framing reader, interrupt/drain admission and retire decision above it are
// the real production chain.
type dataErrConn struct {
	net.Conn
	script []scriptedRead
	next   atomic.Int32
}

type scriptedRead struct {
	prefix  []byte
	err     error
	release chan struct{} // non-nil: the scripted read parks until closed
}

func (c *dataErrConn) Read(p []byte) (int, error) {
	i := int(c.next.Add(1)) - 1
	if i < len(c.script) {
		s := c.script[i]
		if s.release != nil {
			<-s.release
		}
		return copy(p, s.prefix), s.err
	}
	return c.Conn.Read(p)
}

// A cancellation interrupt can fire while a single underlying Read delivers
// bytes AND its error in one call (n > 0, err != nil): those bytes were still
// consumed off the wire, so the frame boundary is already lost. Byte
// accounting that only credits fully successful reads undercounts to zero on
// the first header read and lets a drain re-frame file bytes as the upload
// EOF. Both stages of one frame are pinned — partial header (the
// discriminating case: no earlier stage can rescue the count) and a clean
// header followed by a data+error payload read (payload-stage accounting
// control) — through the real processLoadLocal owner and its watcher.
func TestProcessLoadLocalDataPlusErrorCancelRetiresConn(t *testing.T) {
	for _, name := range []string{"partial header", "clean header, partial payload"} {
		t.Run(name, func(t *testing.T) {
			ses, protocol, client := newInterruptibleLoadLocal(t)
			protocol.interruptEntered = make(chan struct{}, 1)
			protocol.interruptGate = make(chan struct{})
			protocol.interruptApplied = make(chan struct{}, 1)
			releaseInterruptGate := protocol.releaseInterruptGate
			t.Cleanup(releaseInterruptGate)
			scriptRelease := make(chan struct{})
			// Idempotent, early-registered release: a Fatal/require failure
			// anywhere below must not strand the reader parked inside
			// dataErrConn's scripted wait — closing the underlying net.Conn
			// does NOT unblock that channel wait. Registered after (i.e.
			// running before, LIFO) the connection-closing cleanups of
			// newInterruptibleLoadLocal, the released reader settles while the
			// socket and pipe still exist. The normal path uses the same
			// action, so success and failure share one release order.
			var scriptReleaseOnce sync.Once
			releaseScriptRead := func() {
				scriptReleaseOnce.Do(func() { close(scriptRelease) })
			}
			t.Cleanup(releaseScriptRead)
			var script []scriptedRead
			if name == "partial header" {
				// Single underlying read answers the 4-byte header request
				// with 2 bytes AND the expiry error in one call.
				script = []scriptedRead{
					{prefix: []byte{1, 0}, err: scriptedTimeoutError{}, release: scriptRelease},
				}
			} else {
				// Clean header, then ONE payload read delivers 2 of 8 payload
				// bytes together with the expiry error.
				script = []scriptedRead{
					{prefix: []byte{8, 0, 0, 1}},
					{prefix: []byte{'s', 'e'}, err: scriptedTimeoutError{}, release: scriptRelease},
				}
			}
			scripted := &dataErrConn{Conn: protocol.ioses.conn, script: script}
			protocol.ioses.UseConn(scripted)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			ec := newTestExecCtx(ctx, gomock.NewController(t))
			param := &tree.ExternParam{ExParamConst: tree.ExParamConst{Filepath: "test.csv"}}
			reader, writer := io.Pipe()
			t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
			done := make(chan error, 1)
			// ownerExited is the cleanup-owned shutdown witness: it closes when
			// the owner goroutine returns and is never drained by the normal
			// case, so a body that received done — or died before it — can
			// never mask a still-running owner (or its internal watcher).
			ownerExited := make(chan struct{})
			go func() {
				defer close(ownerExited)
				done <- processLoadLocal(ctx, ses, ec, param, writer, reader)
			}()
			// Failure-safe owner join, registered immediately at launch so it
			// is armed before every wait/assert below (running FIRST, LIFO,
			// before the pipe/cancel/gate cleanups above and the connection/
			// allocator cleanups inside newInterruptibleLoadLocal). Ordered
			// teardown: cancel the ctx, idempotently release both gates, fail
			// the real pipe/socket I/O from the test side only (reader/writer/
			// client ends — no Conn buffer is touched before the join), THEN
			// boundedly wait for the owner. The owner joins its internal
			// watcher before returning (its joinWatcher defer), so owner exit
			// also proves watcher exit; only afterwards may the helper's
			// Conn.Close/CheckBalance run. A join timeout fails the test
			// loudly — never inferred away from a closed channel or a
			// buffered done.
			t.Cleanup(func() {
				cancel()
				releaseInterruptGate()
				releaseScriptRead()
				_ = reader.Close()
				_ = writer.Close()
				_ = client.Close()
				select {
				case <-ownerExited:
				case <-time.After(5 * time.Second):
					t.Errorf("processLoadLocal owner did not exit within 5s during cleanup")
				}
			})
			select {
			case <-protocol.reading:
			case <-time.After(5 * time.Second):
				t.Fatal("upload did not enter network read")
			}
			cancel()
			select {
			case <-protocol.interruptEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("watcher never reached its socket interrupt")
			}
			releaseInterruptGate()
			select {
			case <-protocol.interruptApplied:
			case <-time.After(5 * time.Second):
				t.Fatal("watcher interrupt did not execute")
			}
			// The interrupt has executed; only now does the stalled read
			// deliver its data+error pair, so no other read can absorb the
			// staged bytes: consumed-upon-error is the only path that can
			// observe them. The normal path takes the same idempotent release
			// the failure-safe cleanup holds.
			releaseScriptRead()
			// Offer the masquerade tail (its result is deliberately not
			// asserted: a retired connection fails it, a desynced drain
			// consumes it).
			_, _ = client.Write([]byte{0, 0, 0, 5})
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("cancelled upload did not terminate")
			}
			require.EqualValues(t, 1, protocol.disconnects.Load(),
				"bytes delivered with the read error were consumed: the boundary is unknown and the connection must retire")
			require.True(t, protocol.LoadLocalFramingBroken(),
				"a data+error read must count its delivered bytes toward the broken frame")
			// The state is sticky: neither interrupt clearing nor connection
			// teardown revives a lost boundary; only a fresh connection is clean.
			protocol.ClearLoadLocalIOInterrupt()
			require.True(t, protocol.LoadLocalFramingBroken(),
				"ClearLoadLocalIOInterrupt must not revive a lost packet boundary")
			require.EqualValues(t, 1, protocol.frees.Load())
			// File bytes must never surface as the payload of a later packet.
			written := make(chan error, 1)
			go func() {
				_, err := client.Write([]byte{0x0b, 0, 0, 0, 0x03, 's', 'e', 'l', 'e', 'c', 't', ' ', '1', '2', '3'})
				written <- err
			}()
			payload, err := protocol.ioses.ReadLoadLocalPacketWithTimeout(300*time.Millisecond, time.Time{})
			require.Error(t, err, "a retired connection must not decode further client bytes")
			require.Empty(t, payload, "upload file bytes must never be decoded as a command packet")
			select {
			case err := <-written:
				require.Error(t, err, "client writes must fail once the connection is retired")
			case <-time.After(5 * time.Second):
				t.Fatal("client write did not settle")
			}
		})
	}
}
