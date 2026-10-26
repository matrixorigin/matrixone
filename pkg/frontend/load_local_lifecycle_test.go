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

func (w *interruptibleLoadLocalWriter) ReadLoadLocalPacketWithTimeout(timeout time.Duration) ([]byte, error) {
	select {
	case w.reading <- struct{}{}:
	default:
	}
	return w.ioses.ReadLoadLocalPacketWithTimeout(timeout)
}

func (w *interruptibleLoadLocalWriter) InterruptLoadLocalIO() {
	w.interrupts.Add(1)
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
