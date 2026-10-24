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
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Metadata and source data use the real fileservice. The destination models a
// context-aware remote request made before the sink starts reading its pipe.
type storedDumpLifecycleFS struct {
	fileservice.FileService
	waitForCancel bool
	sourceErr     error
	entered       chan struct{}
	canceled      chan struct{}
	release       chan struct{}
	exited        chan struct{}
	output        []byte
}

func (f *storedDumpLifecycleFS) Write(ctx context.Context, v fileservice.IOVector) error {
	if v.FilePath != "SHARED:/dump-lifecycle.csv" {
		return f.FileService.Write(ctx, v)
	}
	close(f.entered)
	defer close(f.exited)
	if f.waitForCancel {
		<-ctx.Done()
		close(f.canceled)
		<-f.release
		return ctx.Err()
	}
	var err error
	f.output, err = io.ReadAll(v.Entries[0].ReaderForWrite)
	return err
}

func (f *storedDumpLifecycleFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	if f.sourceErr != nil && strings.Contains(v.FilePath, "/query_result/") {
		<-f.entered
		return f.sourceErr
	}
	return f.FileService.Read(ctx, v)
}

func TestStoredDumpCancellationOwnership(t *testing.T) {
	for _, mode := range []string{"header-error", "source-error", "cancel", "success"} {
		t.Run(mode, func(t *testing.T) {
			ioutil.RunPipelineTest(func() {
				ses := newTestSession(t, gomock.NewController(t))
				defer ses.Close()
				setupCtx := context.Background()
				require.NoError(t, ses.SetSessionSysVar(setupCtx, "save_query_result", int8(1)))
				proc := testutil.NewProcess(t)
				proc.Base.FileService = getPu("").FileService
				proc.Base.SessionInfo = process.SessionInfo{Account: sysAccountName}
				ses.GetTxnCompileCtx().execCtx = &ExecCtx{reqCtx: setupCtx, proc: proc}
				id := uuid.New()
				ses.SetStmtId(id)
				colTypes := make([]types.Type, 20)
				ses.rs = &plan.ResultColDef{ResultCols: make([]*plan.ColDef, len(colTypes))}
				for i := range colTypes {
					colTypes[i] = types.T_int8.ToType()
					ses.rs.ResultCols[i] = &plan.ColDef{Name: fmt.Sprintf("c%062d", i), Typ: plan.Type{Id: int32(types.T_int8)}}
				}
				asts, err := parsers.Parse(setupCtx, dialect.MYSQL, "select * from t", 1)
				require.NoError(t, err)
				ses.ast, ses.p = asts[0], &plan.Plan{}
				require.True(t, canSaveQueryResult(setupCtx, ses))
				bat := newBatch(colTypes, 1, proc)
				defer bat.Clean(proc.Mp())
				require.NoError(t, saveBatch(setupCtx, ses, bat))
				require.NoError(t, saveMeta(setupCtx, ses))

				option := "header 'false'"
				if mode == "header-error" {
					option = "header 'true' max_file_size 1"
				}
				dumpAST, err := parsers.Parse(setupCtx, dialect.MYSQL, fmt.Sprintf("modump query_result '%s' into 'SHARED:/dump-lifecycle.csv' %s", id.String(), option), 1)
				require.NoError(t, err)
				dump := dumpAST[0].(*tree.MoDump)
				sourceErr := moerr.NewInternalErrorNoCtx("source read failed")
				pu := getPu("")
				original := pu.FileService
				defer func() { pu.FileService = original }()

				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					sink := &storedDumpLifecycleFS{FileService: original, waitForCancel: mode != "success", entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
					if mode == "source-error" {
						sink.sourceErr = sourceErr
					}
					pu.FileService = sink
					release := sync.OnceFunc(func() { close(sink.release) })
					done, finished := make(chan error, 1), make(chan struct{})
					go func() { defer close(finished); done <- handleDump(ses, &ExecCtx{reqCtx: ctx}, dump) }()
					defer func() { cancel(); release(); <-finished }()
					select {
					case <-sink.entered:
					case <-time.After(5 * time.Second):
						t.Fatal("dump did not reach the destination")
					}
					if mode == "cancel" {
						cancel()
					}
					if sink.waitForCancel {
						select {
						case <-sink.canceled:
						case <-time.After(5 * time.Second):
							t.Fatal("export error did not cancel the destination")
						}
						synctest.Wait()
						select {
						case <-done:
							t.Fatal("dump returned before the destination exited")
						default:
						}
						release()
					}
					select {
					case err = <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("dump did not finish")
					}
					<-sink.exited
					switch mode {
					case "header-error":
						require.ErrorContains(t, err, "header line size")
					case "source-error":
						require.Same(t, sourceErr, err)
					case "cancel":
						require.ErrorIs(t, err, context.Canceled)
					case "success":
						require.NoError(t, err)
						require.Equal(t, strings.Repeat("0,", 19)+"0\n", string(sink.output))
					}
					if mode != "cancel" {
						require.NoError(t, ctx.Err(), "cleanup must leave the parent request usable")
					}
				})
			})
		})
	}
}
