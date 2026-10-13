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
