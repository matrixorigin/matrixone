// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func exportLifecycleFixture(t *testing.T, ctx context.Context, format string) (*ExportConfig, *backSession, *batch.Batch, *mpool.MPool) {
	t.Helper()
	mp := mpool.MustNewZero()
	bat := batch.NewOffHeapWithSize(1)
	bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(42), false, mp))
	bat.SetRowCount(1)
	t.Cleanup(func() { bat.Clean(mp); require.Zero(t, mp.CurrNB()) })
	require.Positive(t, mp.CurrNB())
	col := &MysqlColumn{}
	col.SetName("value")
	mrs := &MysqlResultSet{}
	mrs.AddColumn(col)
	ep := &ExportConfig{ctx: ctx, userConfig: newDiffCSVUserConfig(), mrs: mrs, proc: sysFileAccessTestProcess()}
	ep.userConfig.ExportFormat = format
	initExportFileParam(ep, mrs)
	ep.init()
	return ep, &backSession{feSessionImpl: feSessionImpl{pool: mp}}, bat, mp
}

func TestExportFinalizationCancellation(t *testing.T) {
	for _, format := range []string{"csv", "jsonline"} {
		for _, when := range []string{"before-worker", "while-waiting"} {
			t.Run(format+"/"+when, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					ep, ses, bat, mp := exportLifecycleFixture(t, ctx, format)
					defer ep.Close()
					ep.Index.Store(1)
					convert := constructByte
					if format == "jsonline" {
						convert = constructJSONLine
					}
					if when == "before-worker" {
						cancel()
						convert(ep.ctx, ses, bat, 1, ep.ByteChan, ep)
					}
					done := make(chan error, 1)
					go func() { done <- exportAllDataFromBatches(ep) }()
					if when == "while-waiting" {
						synctest.Wait() // finalizer must block on a missing result, rather than spin.
						cancel()
						convert(ep.ctx, ses, bat, 1, ep.ByteChan, ep)
					}
					err := <-done
					require.ErrorIs(t, err, context.Canceled)
					require.ErrorIs(t, ep.finish(err), context.Canceled)
					require.Zero(t, mp.CurrNB())
					require.Nil(t, ep.ByteChan)
					require.Nil(t, ep.BatchMap)
				})
			})
		}
	}
}

func TestExportAbortJoinsBlockedConversions(t *testing.T) {
	for _, format := range []string{"csv", "jsonline"} {
		t.Run(format, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ep, ses, bat, mp := exportLifecycleFixture(t, context.Background(), format)
				defer ep.Close()
				ep.ByteChan = make(chan *BatchByte) // worker must wait for the abandoned consumer.
				convert := constructByte
				if format == "jsonline" {
					convert = constructJSONLine
				}
				ep.workers.Add(1)
				go func() { defer ep.workers.Done(); convert(ep.ctx, ses, bat, 1, ep.ByteChan, ep) }()
				synctest.Wait()
				injected := errors.New("sink failed while request remains active")
				require.ErrorIs(t, ep.finish(injected), injected)
				require.Zero(t, mp.CurrNB())
				require.Nil(t, ep.ByteChan)
				require.Nil(t, ep.mrs)
			})
		})
	}
}

func TestExportOrderedFinalization(t *testing.T) {
	ep, ses, bat, mp := exportLifecycleFixture(t, context.Background(), "csv")
	defer ep.Close()
	var output bytes.Buffer
	old := Write
	Write = func(_ *ExportConfig, b []byte) (int, error) { return output.Write(b) }
	defer func() { Write = old }()
	ep.Index.Store(3)
	// Ready map data must be consumed before waiting on an empty result channel.
	ep.BatchMap[1] = &BatchByte{index: 1}
	ep.ByteChan <- &BatchByte{index: 3, writeByte: []byte("tail\n"), rowCount: 1, rowEnds: []int{5}}
	constructByte(ep.ctx, ses, bat, 2, ep.ByteChan, ep)
	require.NoError(t, exportAllDataFromBatches(ep))
	require.Equal(t, "42\ntail\n", output.String())
	require.EqualValues(t, 3, ep.WriteIndex.Load())
	require.Empty(t, ep.BatchMap)
	require.Zero(t, mp.CurrNB())
	require.NoError(t, ep.finish(nil))
	require.Nil(t, ep.ByteChan)
}

func TestExportFinalizerClosedChannel(t *testing.T) {
	ep := &ExportConfig{ctx: context.Background(), userConfig: newDiffCSVUserConfig()}
	ep.init()
	defer ep.Close()
	ep.Index.Store(1)
	close(ep.ByteChan)
	require.ErrorContains(t, exportAllDataFromBatches(ep), "missing batches")
}

type exportLifecycleFS struct {
	fileservice.FileService
	write  func(context.Context, fileservice.IOVector) error
	read   func(context.Context, *fileservice.IOVector) error
	closed atomic.Bool
}

func (f *exportLifecycleFS) Write(ctx context.Context, v fileservice.IOVector) error {
	return f.write(ctx, v)
}
func (f *exportLifecycleFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	if f.read != nil {
		return f.read(ctx, v)
	}
	return f.FileService.Read(ctx, v)
}
func (f *exportLifecycleFS) Close(context.Context) { f.closed.Store(true) }

func TestExportPipeCancellation(t *testing.T) {
	for _, phase := range []string{"reader", "writer", "header", "late-close"} {
		t.Run(phase, func(t *testing.T) {
			ses := newValidateSession(t)
			pu := getPu(ses.GetService())
			original := pu.FileService
			t.Cleanup(func() { pu.FileService = original })
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				ep, _, _, _ := exportLifecycleFixture(t, ctx, "csv")
				ep.auditEnabled = true
				defer ep.Close()
				ep.service = ses.GetService()
				ep.userConfig.FilePath = "SHARED:/cancel.csv"
				ep.userConfig.Header = phase == "header"
				entered := make(chan struct{})
				finished := make(chan struct{})
				injected := errors.New("sink failed after EOF")
				fs := &exportLifecycleFS{write: func(ctx context.Context, v fileservice.IOVector) error {
					defer close(finished)
					close(entered)
					if phase == "writer" || phase == "header" {
						<-ctx.Done()
						return ctx.Err()
					}
					_, err := io.Copy(io.Discard, v.Entries[0].ReaderForWrite)
					if phase == "late-close" {
						return injected
					}
					return err
				}}
				pu.FileService = fs
				opened := make(chan error, 1)
				go func() { opened <- openNewFile(ep.ctx, ep, ep.mrs) }()
				<-entered
				if phase == "header" {
					synctest.Wait()
					cancel()
					require.ErrorIs(t, <-opened, context.Canceled)
				} else {
					require.NoError(t, <-opened)
					if phase == "writer" {
						wrote := make(chan error, 1)
						go func() { wrote <- writeToCSVFile(ep, []byte("42\n")) }()
						synctest.Wait()
						cancel()
						require.ErrorIs(t, <-wrote, context.Canceled)
					} else if phase == "reader" {
						synctest.Wait()
						cancel()
					} else if phase == "late-close" {
						// Model progress accepted by the pipe before the sink reports a
						// close error. It must remain provisional, not exact output.
						ep.CurFileSize, ep.Rows = 17, 1
					}
				}
				err := ep.finish(nil)
				if phase == "late-close" {
					require.ErrorIs(t, err, injected)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
				<-finished
				require.False(t, fs.closed.Load(), "shared file service is borrowed")
				require.Nil(t, ep.AsyncReader)
				require.Nil(t, ep.AsyncWriter)
				require.Nil(t, ep.AsyncGroup)
				require.Nil(t, ep.stopFileCancel)
				require.Zero(t, ep.auditBytesWritten)
				require.Zero(t, ep.auditRowsWritten)
			})
		})
	}
}

func TestExportSplitAndEmptyFileCleanup(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "split", true: "empty"}[empty], func(t *testing.T) {
			dir := t.TempDir()
			ep := &ExportConfig{ctx: context.Background(), proc: sysFileAccessTestProcess(), userConfig: newDiffCSVUserConfig(), mrs: &MysqlResultSet{}, auditEnabled: true}
			ep.userConfig.FilePath = filepath.Join(dir, "out.csv")
			ep.userConfig.MaxFileSize = 3
			ep.init()
			defer ep.Close()
			require.NoError(t, openNewFile(ep.ctx, ep, ep.mrs))
			if !empty {
				require.NoError(t, writeExportBatchToFile(ep, &BatchByte{writeByte: []byte("a\n"), rowEnds: []int{2}, rowCount: 1}))
				require.NoError(t, writeExportBatchToFile(ep, &BatchByte{writeByte: []byte("b\n"), rowEnds: []int{2}, rowCount: 1}))
			}
			require.NoError(t, exportAllDataFromBatches(ep))
			require.NoError(t, ep.finish(nil))
			if empty {
				require.Zero(t, ep.auditBytesWritten)
				require.Zero(t, ep.auditRowsWritten)
			} else {
				require.EqualValues(t, 4, ep.auditBytesWritten)
				require.EqualValues(t, 2, ep.auditRowsWritten)
			}
			require.Nil(t, ep.FileService)
			require.Nil(t, ep.AsyncGroup)
			content, err := os.ReadFile(filepath.Join(dir, "out.csv"))
			require.NoError(t, err)
			if empty {
				require.Empty(t, content)
			} else {
				require.Equal(t, "a\n", string(content))
				content, err = os.ReadFile(filepath.Join(dir, "out.csv.1"))
				require.NoError(t, err)
				require.Equal(t, "b\n", string(content))
			}
		})
	}
}

func TestExportAuditPartialSplitFailureKeepsConfirmedLowerBoundAndAllowsReuse(t *testing.T) {
	ses := newValidateSession(t)
	pu := getPu(ses.GetService())
	original := pu.FileService
	t.Cleanup(func() { pu.FileService = original })

	injected := errors.New("second split sink failure")
	completed := make(map[string][]byte)
	fs := &exportLifecycleFS{write: func(_ context.Context, v fileservice.IOVector) error {
		data, err := io.ReadAll(v.Entries[0].ReaderForWrite)
		if err != nil {
			return err
		}
		if v.FilePath == "SHARED:/audit/partial.csv.1" {
			return injected
		}
		completed[v.FilePath] = data
		return nil
	}}
	pu.FileService = fs

	failed, _, _, _ := exportLifecycleFixture(t, context.Background(), "csv")
	failed.auditEnabled = true
	failed.service = ses.GetService()
	failed.userConfig.FilePath = "SHARED:/audit/partial.csv"
	failed.userConfig.SplitSize = 2
	defer failed.Close()
	require.NoError(t, openNewFile(failed.ctx, failed, failed.mrs))
	require.NoError(t, writeExportBatchToFile(failed, &BatchByte{
		writeByte: []byte("a\nb\n"), rowEnds: []int{2, 4}, rowCount: 2,
	}))
	require.ErrorIs(t, failed.finish(nil), injected)
	require.Equal(t, []byte("a\n"), completed["SHARED:/audit/partial.csv"])
	require.NotContains(t, completed, "SHARED:/audit/partial.csv.1", "the failed split is not a confirmed file")
	require.EqualValues(t, 2, failed.auditBytesWritten)
	require.EqualValues(t, 1, failed.auditRowsWritten)
	require.Nil(t, failed.AsyncGroup)
	require.Nil(t, failed.AsyncWriter)

	failedStatement := motrace.NewStatementInfo()
	failedStatement.SetOutfileReceipt(motrace.StatementOutfileReceipt{
		Version: 1, Target: "SHARED:/audit/partial.csv", Format: "csv",
	})
	failedStatement.UpdateOutfileReceipt("csv", failed.auditBytesWritten, failed.auditRowsWritten, false)
	require.JSONEq(t,
		`{"outfile":{"version":1,"target":"SHARED:/audit/partial.csv","format":"csv","bytes_written":2,"rows_written":1,"complete":false}}`,
		string(failedStatement.ExecPlan2Json(context.Background())),
	)

	recovered, _, _, _ := exportLifecycleFixture(t, context.Background(), "csv")
	recovered.auditEnabled = true
	recovered.service = ses.GetService()
	recovered.userConfig.FilePath = "SHARED:/audit/recovered.csv"
	defer recovered.Close()
	require.NoError(t, openNewFile(recovered.ctx, recovered, recovered.mrs))
	require.NoError(t, writeExportBatchToFile(recovered, &BatchByte{
		writeByte: []byte("c\n"), rowEnds: []int{2}, rowCount: 1,
	}))
	require.NoError(t, recovered.finish(nil))
	require.Equal(t, []byte("c\n"), completed["SHARED:/audit/recovered.csv"])
	require.EqualValues(t, 2, recovered.auditBytesWritten)
	require.EqualValues(t, 1, recovered.auditRowsWritten)
	ordinaryStatement := motrace.NewStatementInfo()
	require.False(t, ordinaryStatement.HasOutfileReceipt(), "the next ordinary statement has no receipt")
	require.False(t, fs.closed.Load(), "the shared file service is borrowed")
}

func TestExportAuditCountsCSVHeaderAndJSONLines(t *testing.T) {
	for _, tc := range []struct {
		name, format, payload, want string
		rows                        int
		header                      bool
		rowEnds                     []int
	}{
		{name: "csv header", format: "csv", payload: "42\n", want: "value\n42\n", rows: 1, header: true, rowEnds: []int{3}},
		{name: "jsonline", format: "jsonline", payload: "{\"value\":1}\n{\"value\":2}\n", want: "{\"value\":1}\n{\"value\":2}\n", rows: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ep, _, _, _ := exportLifecycleFixture(t, context.Background(), tc.format)
			ep.auditEnabled = true
			ep.userConfig.FilePath = filepath.Join(t.TempDir(), "audit.out")
			ep.userConfig.Header = tc.header
			defer ep.Close()
			require.NoError(t, openNewFile(ep.ctx, ep, ep.mrs))
			require.NoError(t, writeExportBatchToFile(ep, &BatchByte{
				writeByte: []byte(tc.payload), rowCount: tc.rows, rowEnds: tc.rowEnds,
			}))
			require.NoError(t, ep.finish(nil))
			actual, err := os.ReadFile(ep.userConfig.FilePath)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(actual))
			require.Equal(t, uint64(len(actual)), ep.auditBytesWritten)
			require.Equal(t, uint64(tc.rows), ep.auditRowsWritten)
		})
	}
}

func TestExportConversionFailure(t *testing.T) {
	for _, format := range []string{"csv", "jsonline"} {
		t.Run(format, func(t *testing.T) {
			ep, ses, bat, mp := exportLifecycleFixture(t, context.Background(), format)
			defer ep.Close()
			bat.Clean(mp)
			bat.Vecs = []*vector.Vector{vector.NewOffHeapVecWithType(types.T_TS.ToType())}
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.BuildTS(1, 1), false, mp))
			bat.SetRowCount(1)
			ep.Index.Store(1)
			if format == "csv" {
				constructByte(ep.ctx, ses, bat, 1, ep.ByteChan, ep)
			} else {
				constructJSONLine(ep.ctx, ses, bat, 1, ep.ByteChan, ep)
			}
			err := exportAllDataFromBatches(ep)
			require.ErrorContains(t, err, "unsupported type")
			require.ErrorIs(t, ep.finish(err), err)
			require.Zero(t, mp.CurrNB())
			require.Nil(t, ep.BatchMap)
		})
	}
}

type exportStatusRunner struct{ run func() error }

func (r exportStatusRunner) Run(uint64) (*util.RunResult, error) { return &util.RunResult{}, r.run() }

type exportStatusColumns struct {
	ComputationWrapper
	columns []interface{}
}

func (w exportStatusColumns) GetColumns(context.Context) ([]interface{}, error) {
	return w.columns, nil
}

func TestExportStatementLifecycle(t *testing.T) {
	for _, format := range []string{"csv", "jsonline"} {
		modes := []string{"success", "runner-error", "cancel", "header-error"}
		if format == "jsonline" {
			modes = append(modes, "duplicate-columns")
		}
		for _, mode := range modes {
			t.Run(format+"/"+mode, func(t *testing.T) {
				ses := newValidateSession(t)
				ses.pool = ses.proc.Mp()
				ses.SetMysqlResultSet(&MysqlResultSet{})
				cfg := newDiffCSVUserConfig()
				cfg.ExportFormat = format
				cfg.FilePath = filepath.Join(t.TempDir(), "out.csv")
				if mode == "header-error" {
					cfg.Header = true
					cfg.MaxFileSize = 1
					cfg.ExportFormat = "csv"
				}
				ses.tStmt = motrace.NewStatementInfo()
				ses.tStmt.SetOutfileReceipt(motrace.StatementOutfileReceipt{Target: cfg.FilePath, Format: format})
				ses.InitExportConfig(cfg)
				ep := ses.GetExportConfig()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				col := &MysqlColumn{}
				col.SetName("value")
				col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
				source := batch.NewOffHeapWithSize(1)
				defer source.Clean(ses.pool)
				source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(42), false, ses.pool))
				source.SetRowCount(1)
				injected := errors.New("runner failed")
				columns := []interface{}{col}
				if mode == "duplicate-columns" {
					columns = append(columns, col)
				}
				execCtx := &ExecCtx{reqCtx: ctx, ses: ses, stmt: &tree.Select{}, cw: exportStatusColumns{columns: columns}}
				execCtx.runner = exportStatusRunner{run: func() error {
					if err := ep.Write(execCtx, nil, source); err != nil {
						return err
					}
					if mode == "cancel" {
						cancel()
					}
					if mode == "runner-error" {
						return injected
					}
					return nil
				}}
				err := executeStatusStmt(ses, execCtx)
				switch mode {
				case "success":
					require.NoError(t, err)
					b, e := os.ReadFile(cfg.FilePath)
					require.NoError(t, e)
					want := "42\n"
					if format == "jsonline" {
						want = "{\"value\":42}\n"
					}
					require.Equal(t, want, string(b))
				case "runner-error":
					require.ErrorIs(t, err, injected)
				case "cancel":
					require.ErrorIs(t, err, context.Canceled)
				case "header-error":
					require.ErrorContains(t, err, "header line size")
				case "duplicate-columns":
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
					require.ErrorContains(t, err, "duplicate column names")
				}
				var fields struct {
					Outfile motrace.StatementOutfileReceipt `json:"outfile"`
				}
				require.NoError(t, json.Unmarshal(ses.tStmt.ExecPlan2Json(ctx), &fields))
				require.Equal(t, mode == "success", fields.Outfile.Complete)
				if mode == "success" {
					actual, readErr := os.ReadFile(cfg.FilePath)
					require.NoError(t, readErr)
					require.EqualValues(t, len(actual), fields.Outfile.BytesWritten)
					require.EqualValues(t, 1, fields.Outfile.RowsWritten)
				}
				source.Clean(ses.pool)
				require.Zero(t, ses.pool.CurrNB())
				require.Nil(t, ep.AsyncGroup)
				require.Nil(t, ep.AsyncWriter)
				require.Nil(t, ep.ByteChan)
				require.Nil(t, ep.mrs)
			})
		}
	}
}

// A panic must abort publication before joining converters, preserve completed
// splits, and reach the outer statement recovery with its original identity.
func TestExportStatementPanicAbortsAndKeepsReceiptIncomplete(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-worker", true: "blocked-conversion"}[blocked], func(t *testing.T) {
			ses := newValidateSession(t)
			ses.pool = ses.proc.Mp()
			pu := getPu(ses.GetService())
			original := pu.FileService
			t.Cleanup(func() { pu.FileService = original })
			synctest.Test(t, func(t *testing.T) {
				completed := make(map[string][]byte)
				fs := &exportLifecycleFS{write: func(_ context.Context, v fileservice.IOVector) error {
					data, err := io.ReadAll(v.Entries[0].ReaderForWrite)
					if err == nil {
						completed[v.FilePath] = data
					}
					return err
				}}
				pu.FileService = fs
				cfg := newDiffCSVUserConfig()
				cfg.FilePath = "SHARED:/panic.csv"
				cfg.SplitSize = 2
				ses.SetMysqlResultSet(&MysqlResultSet{})
				ses.tStmt = motrace.NewStatementInfo()
				ses.tStmt.SetOutfileReceipt(motrace.StatementOutfileReceipt{Target: cfg.FilePath, Format: "csv"})
				ses.InitExportConfig(cfg)
				ep := ses.GetExportConfig()
				defer ep.Close()
				column := &MysqlColumn{}
				column.SetName("value")
				column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
				source := batch.NewOffHeapWithSize(1)
				defer source.Clean(ses.pool)
				source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(source.Vecs[0], int64(42), false, ses.pool))
				source.SetRowCount(1)
				injected := moerr.NewInvalidInputNoCtx("runner panic")
				execCtx := &ExecCtx{reqCtx: t.Context(), ses: ses, stmt: &tree.Select{}, cw: exportStatusColumns{columns: []interface{}{column}}}
				execCtx.runner = exportStatusRunner{run: func() error {
					require.NoError(t, writeExportBatchToFile(ep, &BatchByte{
						writeByte: []byte("a\nb\n"), rowEnds: []int{2, 4}, rowCount: 2,
					}))
					if blocked {
						ep.ByteChan = make(chan *BatchByte)
						copied, err := cloneExportWorkerBatch(source, ses.pool)
						require.NoError(t, err)
						ep.workers.Add(1)
						go func() {
							defer ep.workers.Done()
							constructByte(ep.ctx, ses, copied, 1, ep.ByteChan, ep)
						}()
						synctest.Wait() // converter is blocked publishing to the abandoned consumer.
					}
					panic(injected)
				}}
				require.PanicsWithValue(t, injected, func() { _ = executeStatusStmt(ses, execCtx) })
				var fields struct {
					Outfile motrace.StatementOutfileReceipt `json:"outfile"`
				}
				require.NoError(t, json.Unmarshal(ses.tStmt.ExecPlan2Json(t.Context()), &fields))
				require.False(t, fields.Outfile.Complete)
				require.EqualValues(t, 2, fields.Outfile.BytesWritten)
				require.EqualValues(t, 1, fields.Outfile.RowsWritten)
				require.Equal(t, []byte("a\n"), completed[cfg.FilePath])
				require.NotContains(t, completed, cfg.FilePath+".1")
				require.Nil(t, ep.AsyncGroup)
				require.Nil(t, ep.AsyncWriter)
				require.Nil(t, ep.ByteChan)
				require.Nil(t, ep.ctx)
				source.Clean(ses.pool)
				require.Zero(t, ses.pool.CurrNB())
				require.False(t, fs.closed.Load(), "shared file service is borrowed")
			})
		})
	}
}

func TestExportJSONLineConversionFailureCompletesPipeWrite(t *testing.T) {
	ses := newValidateSession(t)
	ses.pool = ses.proc.Mp()
	pu := getPu(ses.GetService())
	original := pu.FileService
	t.Cleanup(func() { pu.FileService = original })

	cfg := newDiffCSVUserConfig()
	cfg.ExportFormat = "jsonline"
	cfg.FilePath = "SHARED:/jsonline-conversion-error.jsonl"
	ses.SetMysqlResultSet(&MysqlResultSet{})
	ses.InitExportConfig(cfg)
	ep := ses.GetExportConfig()

	writeDone := make(chan error, 1)
	fs := &exportLifecycleFS{write: func(_ context.Context, v fileservice.IOVector) error {
		_, err := io.Copy(io.Discard, v.Entries[0].ReaderForWrite)
		writeDone <- err
		return err
	}}
	pu.FileService = fs

	column := &MysqlColumn{}
	column.SetName("value")
	column.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	source := batch.NewOffHeapWithSize(1)
	defer source.Clean(ses.pool)
	source.Vecs[0] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(source.Vecs[0], []byte{'x', 0xff, 'y'}, false, ses.pool))
	source.SetRowCount(1)

	var execCtx *ExecCtx
	execCtx = &ExecCtx{
		reqCtx: context.Background(),
		ses:    ses,
		stmt:   &tree.Select{},
		cw:     exportStatusColumns{columns: []interface{}{column}},
		runner: exportStatusRunner{run: func() error {
			err := ep.Write(execCtx, nil, source)
			source.Clean(ses.pool)
			return err
		}},
	}

	done := make(chan error, 1)
	go func() { done <- executeStatusStmt(ses, execCtx) }()
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("invalid UTF-8 export did not finish its statement")
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid UTF-8")
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("failed conversion did not complete the async pipe Write")
	}
	require.Nil(t, ep.AsyncReader)
	require.Nil(t, ep.AsyncWriter)
	require.Nil(t, ep.AsyncGroup)
	require.Nil(t, ep.ByteChan)
	require.Zero(t, ses.pool.CurrNB())
	require.False(t, fs.closed.Load(), "shared file service is borrowed")
}

type exportHeldProducer struct {
	*pickStreamingExecutor
	entered, release chan struct{}
}

func (e *exportHeldProducer) Exec(ctx context.Context, _ string, _ executor.Options) (executor.Result, error) {
	close(e.entered)
	<-e.release
	return executor.Result{}, ctx.Err()
}

func TestDataBranchCSVCancellationWhileProducerStops(t *testing.T) {
	ses := newValidateSession(t)
	ses.pool = ses.proc.Mp()
	ses.SetMysqlResultSet(&MysqlResultSet{})
	previous := moruntime.ServiceRuntime(ses.GetService())
	if previous == nil {
		previous = moruntime.DefaultRuntime()
	}
	t.Cleanup(func() { moruntime.SetupServiceBasedRuntime(ses.GetService(), previous) })
	dir := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		producer := &exportHeldProducer{pickStreamingExecutor: &pickStreamingExecutor{}, entered: make(chan struct{}), release: make(chan struct{})}
		bh := newPickStreamingBackExecForTest(t, ses, producer)
		pool, err := ants.NewPool(1)
		require.NoError(t, err)
		defer pool.Release()
		tbl := newTestBranchTableStuff(gomock.NewController(t))
		tbl.worker = pool
		done := make(chan error, 1)
		go func() {
			done <- writeCSV(ctx, ses, tbl, bh, &tree.DataBranchDiff{OutputOpt: &tree.DiffOutputOpt{DirPath: dir}})
		}()
		<-producer.entered
		cancel()
		synctest.Wait() // Cancellation must not keep the coordinator runnable or append errors repeatedly.
		close(producer.release)
		require.ErrorIs(t, <-done, context.Canceled)
		require.Zero(t, ses.pool.CurrNB())
	})
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestExportCleanupPreservesErrorIdentity(t *testing.T) {
	fatal := moerr.NewDeadLockDetectedNoCtx()
	sink := moerr.NewFileAlreadyExistsNoCtx("out.csv")
	require.Same(t, fatal, exportCleanupError(nil, fatal))
	require.Same(t, fatal, exportCleanupError(fatal, nil))
	require.Same(t, fatal, exportCleanupError(fatal, fatal))
	require.Same(t, fatal, exportCleanupError(fatal, sink))
	require.True(t, moerr.IsMoErrCode(exportCleanupError(nil, sink), moerr.ErrFileAlreadyExists))
	unknown := moerr.NewTxnUnknown(context.Background(), "commit")
	require.True(t, isTxnCommitResultUnknown(exportCleanupError(unknown, sink)), "cleanup must not change ambiguous-commit policy")
}

func TestExportCleanupKeepsWholeTransactionRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	ses := newMockErrSession3(t, ctx, ctrl)
	defer ses.Close()
	require.NoError(t, ses.SetSessionSysVar(ctx, "mo_rollback_txn_on_error", int64(0)))
	ec := newTestExecCtx(ctx, ctrl)
	ec.ses = ses
	require.NoError(t, ses.GetTxnHandler().SetAutocommit(ec, true, false))
	ec.txnOpt = FeTxnOption{byBegin: true}
	require.NoError(t, ses.GetTxnHandler().Create(ec))
	ec.stmt = &tree.Select{}
	require.True(t, ses.GetTxnHandler().InActiveTxn())
	ep := &ExportConfig{ctx: ctx, userConfig: newDiffCSVUserConfig()}
	ep.init()
	ep.AsyncReader, ep.AsyncWriter = io.Pipe()
	ep.AsyncGroup = &errgroup.Group{}
	sink := moerr.NewFileAlreadyExistsNoCtx("out.csv")
	ep.AsyncGroup.Go(func() error { return sink })
	fatal := moerr.NewDeadLockDetectedNoCtx()
	err := ep.finish(fatal)
	require.Same(t, fatal, err)
	require.ErrorIs(t, rollbackTxnFunc(ses, err, ec), fatal)
	require.True(t, ec.txnOpt.byRollback)
	require.Nil(t, ses.GetTxnHandler().GetTxn(), "fatal export error must discard the entire explicit transaction")
}

// Exercise the stored-result entry point, including sinks waiting on a
// context-aware backend operation before they begin consuming the pipe.
func TestStoredResultDumpCancellationOwnership(t *testing.T) {
	for _, phase := range []string{"header-error", "source-error", "parent-cancel"} {
		t.Run(phase, func(t *testing.T) {
			ioutil.RunPipelineTest(func() {
				ctrl := gomock.NewController(t)
				ses := newTestSession(t, ctrl)
				defer ses.Close()
				ses.proc.Base.SessionInfo.Account = sysAccountName
				ses.proc.Base.SessionInfo.Role = moAdminRoleName
				key := &struct{}{}
				ctx := context.WithValue(context.Background(), key, "stored-result")
				require.NoError(t, ses.SetSessionSysVar(ctx, "save_query_result", int8(1)))
				ses.GetTxnCompileCtx().execCtx = &ExecCtx{reqCtx: ctx, proc: ses.proc}
				id := uuid.New()
				ses.SetStmtId(id)
				ses.tStmt = &motrace.StatementInfo{StatementID: id}
				ses.rs = &plan.ResultColDef{ResultCols: []*plan.ColDef{{Name: "value", Typ: plan.Type{Id: int32(types.T_int8)}}}}
				statements, err := parsers.Parse(ctx, dialect.MYSQL, "select value from t", 1)
				require.NoError(t, err)
				ses.ast, ses.p = statements[0], &plan.Plan{}
				require.True(t, canSaveQueryResult(ctx, ses))
				data := newBatch([]types.Type{types.T_int8.ToType()}, 1, ses.proc)
				defer data.Clean(ses.proc.Mp())
				require.NoError(t, saveBatch(ctx, ses, data))
				require.NoError(t, saveMeta(ctx, ses))

				cfg := newDiffCSVUserConfig()
				cfg.QueryId = id.String()
				cfg.FilePath = "SHARED:/stored-dump-" + phase + ".csv"
				cfg.Header = phase != "source-error"
				if phase == "header-error" {
					cfg.MaxFileSize = 1
				}
				pu := getPu(ses.GetService())
				original, originalProc := pu.FileService, ses.proc
				defer func() { pu.FileService = original }()
				injected := moerr.NewInternalErrorNoCtx("injected stored-result read failure")

				func() {
					parent, cancel := context.WithCancel(ctx)
					defer cancel()
					entered, exited := make(chan struct{}), make(chan struct{})
					sinkContext := make(chan context.Context, 1)
					fs := &exportLifecycleFS{FileService: original, write: func(ctx context.Context, _ fileservice.IOVector) error {
						sinkContext <- ctx
						close(entered)
						defer close(exited)
						<-ctx.Done()
						return ctx.Err()
					}}
					if phase == "source-error" {
						sourcePath := catalog.BuildQueryResultPath(sysAccountName, id.String(), 1)
						fs.read = func(ctx context.Context, vector *fileservice.IOVector) error {
							if vector.FilePath == sourcePath {
								<-entered
								return injected
							}
							return original.Read(ctx, vector)
						}
					}
					pu.FileService = fs
					done := make(chan error, 1)
					go func() { done <- doDumpQueryResult(parent, ses, cfg) }()
					<-entered
					if phase == "parent-cancel" {
						cancel()
					}
					var dumpErr error
					select {
					case dumpErr = <-done:
					case <-time.After(5 * time.Second):
						// A regressing test must still cancel and join its cooperative sink.
						cancel()
						dumpErr = <-done
						t.Error("stored-result cleanup waited for independent parent cancellation")
					}
					switch phase {
					case "header-error":
						require.ErrorContains(t, dumpErr, "header line size")
						require.NoError(t, parent.Err())
					case "source-error":
						require.Same(t, injected, dumpErr)
						require.NoError(t, parent.Err())
					case "parent-cancel":
						require.ErrorIs(t, dumpErr, context.Canceled)
					}
					select {
					case <-exited:
					default:
						t.Fatal("dump returned before its sink exited")
					}
					child := <-sinkContext
					require.Equal(t, "stored-result", child.Value(key))
					require.ErrorIs(t, child.Err(), context.Canceled)
					require.False(t, fs.closed.Load(), "dump must not close a borrowed shared service")
					require.Same(t, originalProc, ses.proc)
					cols, err := openResultMeta(ctx, ses, id.String())
					require.NoError(t, err, "saved metadata and shared service must remain usable")
					require.Equal(t, "value", cols.ResultCols[0].Name)
				}()
			})
		})
	}
}
