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

package siriusbridge

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/stretchr/testify/require"
)

type retiringReader struct {
	readutil.EmptyReader
	entered chan struct{}
	readErr func(context.Context) error
	closes  atomic.Int32
}

func (r *retiringReader) Read(ctx context.Context, _ []string, _ *planpb.Expr, _ *mpool.MPool, _ *batch.Batch) (bool, error) {
	close(r.entered)
	return false, r.readErr(ctx)
}

func (r *retiringReader) Close() error { r.closes.Add(1); return nil }

// Use a real MO table-scan pipeline, not a callback returning a canned error:
// EOF must stop the reader, join pipeline cleanup, and arbitrate its actual
// cancellation result without reporting input failure after native success.
func TestRunArbitratesReaderRetirementAtEOF(t *testing.T) {
	readerFailure := errors.New("reader storage failure")
	callerCause := errors.New("caller stopped the statement")
	for _, tc := range []struct {
		name              string
		readerErr         func(context.Context) error
		cancelAtEOF       bool
		cancelAfterRetire bool
		failBeforeEOF     bool
		wantErr           error
		wantFailure       int32
	}{
		{name: "EOF_retires_context_error", readerErr: func(ctx context.Context) error { return ctx.Err() }},
		{name: "EOF_retires_wrapped_context_error", readerErr: func(ctx context.Context) error { return fmt.Errorf("reader: %w", ctx.Err()) }},
		{name: "EOF_retires_context_cause", readerErr: context.Cause},
		{name: "EOF_retires_query_interrupted", readerErr: func(ctx context.Context) error { return moerr.NewQueryInterrupted(ctx) }},
		{name: "EOF_retires_joined_stop_errors", readerErr: func(ctx context.Context) error { return errors.Join(ctx.Err(), context.Cause(ctx)) }},
		{name: "EOF_preserves_reader_error", readerErr: func(context.Context) error { return readerFailure }, wantErr: readerFailure, wantFailure: 1},
		{name: "EOF_preserves_joined_reader_error", readerErr: func(ctx context.Context) error { return errors.Join(ctx.Err(), readerFailure) }, wantErr: readerFailure, wantFailure: 1},
		{name: "EOF_preserves_reader_error_joined_with_not_needed", readerErr: func(ctx context.Context) error { return errors.Join(context.Cause(ctx), readerFailure) }, wantErr: readerFailure, wantFailure: 1},
		{name: "EOF_preserves_independent_deadline", readerErr: func(context.Context) error { return context.DeadlineExceeded }, wantErr: context.DeadlineExceeded, wantFailure: 1},
		{name: "caller_cancellation_before_EOF", readerErr: func(ctx context.Context) error { return ctx.Err() }, cancelAtEOF: true, wantErr: context.Canceled, wantFailure: 1},
		{name: "caller_cancellation_during_retirement", readerErr: func(ctx context.Context) error { return ctx.Err() }, cancelAfterRetire: true, wantErr: context.Canceled},
		{name: "reader_error_before_EOF", readerErr: func(context.Context) error { return readerFailure }, failBeforeEOF: true, wantErr: readerFailure, wantFailure: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			guard, stopGuard := context.WithTimeout(t.Context(), 30*time.Second)
			t.Cleanup(stopGuard)
			ctx, cancelCaller := context.WithCancelCause(guard)
			t.Cleanup(func() { cancelCaller(nil) })
			proc := testutil.NewProcess(t)
			r, d := testRuntime()
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				require.NoError(t, r.Close(cleanup))
			})
			source := &panicTestInput{}
			d.q.source = source
			reader := &retiringReader{entered: make(chan struct{}), readErr: func(ctx context.Context) error {
				if !tc.failBeforeEOF {
					<-ctx.Done()
				}
				if tc.cancelAfterRetire {
					cancelCaller(callerCause)
				}
				return tc.readerErr(ctx)
			}}
			d.q.nextFn = func(func(Result) error) error {
				select {
				case <-reader.entered:
				case <-ctx.Done():
					return ctx.Err()
				}
				if tc.failBeforeEOF {
					<-d.q.cancelled
					return context.Canceled
				}
				if tc.cancelAtEOF {
					cancelCaller(callerCause)
				}
				return errEOF
			}
			req := testRequest()
			req.Reads = []Read{{BindingID: 1, Columns: []ReadColumn{{Column: Column{Name: "n"}}}, Producer: func(ctx context.Context, _ *Input) (err error) {
				child := proc.NewViewBindingProcess(ctx)
				defer child.Free()
				child.BuildPipelineContext(ctx)
				scan := table_scan.NewArgument()
				scan.Types = []planpb.Type{{Id: int32(types.T_int64)}}
				scan.SetAnalyzeControl(0, true)
				p := pipeline.New(7, []string{"n"}, scan)
				defer func() { p.Cleanup(child, err != nil, false, err) }()
				_, err = p.RunWithReader(reader, 0, child)
				return err
			}}}
			var releases atomic.Int32
			req.Release = func(context.Context) error {
				if reader.closes.Load() != 1 {
					return errors.New("query release preceded reader cleanup")
				}
				releases.Add(1)
				return nil
			}
			q, err := r.Prepare(ctx, req)
			require.NoError(t, err)
			err = q.Run(ctx, func(Result) error { return nil })
			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
			}
			if tc.cancelAtEOF || tc.cancelAfterRetire {
				require.ErrorIs(t, err, callerCause, "native EOF must not erase caller cancellation")
			}
			require.Equal(t, tc.wantFailure, source.failed.Load())
			require.Zero(t, source.finished.Load())
			require.Equal(t, int32(1), reader.closes.Load(), "Run must join reader cleanup")
			require.Zero(t, proc.Mp().CurrNB())
			require.NoError(t, q.Close(guard))
			require.Equal(t, int32(1), releases.Load())
			require.Equal(t, int32(1), d.q.closes.Load())
		})
	}
}
