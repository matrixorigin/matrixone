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

package cnservice

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/testutil"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/siriusbridge"
	"github.com/stretchr/testify/require"
)

type embeddedRuntimeTestRecorder struct {
	request siriusbridge.Request
	closed  bool
	err     error
}

func (*embeddedRuntimeTestRecorder) Accepting() bool { return true }

func (r *embeddedRuntimeTestRecorder) Close(context.Context) error {
	r.closed = true
	return nil
}

func (r *embeddedRuntimeTestRecorder) Prepare(
	_ context.Context,
	request siriusbridge.Request,
) (*siriusbridge.Query, error) {
	r.request = request
	return nil, r.err
}

func TestEmbeddedSiriusConfigurationDoesNotRequireFlight(t *testing.T) {
	config := SiriusConfig{Backend: "embedded", NativeConfigPath: "sirius.conf"}
	if err := validateSiriusEmbeddedConfig(&config); err != nil {
		t.Fatal(err)
	}
	if config.InputMode != "mo" || config.GPUStreams != 2 || config.MaxWaitingQueries != 16 {
		t.Fatalf("defaults: %+v", config)
	}
	for _, modify := range []func(*SiriusConfig){
		func(c *SiriusConfig) { c.InputMode = "tae" },
		func(c *SiriusConfig) { c.NativeConfigPath = "" },
		func(c *SiriusConfig) { c.GPUStreams = 129 },
		func(c *SiriusConfig) { c.MaxWaitingQueries = 17 },
		func(c *SiriusConfig) { c.BenchmarkNoGC = true },
	} {
		invalid := config
		modify(&invalid)
		if validateSiriusEmbeddedConfig(&invalid) == nil {
			t.Fatalf("accepted %+v", invalid)
		}
	}
}

func TestEmbeddedSiriusBackendMapsRequestAndDelegates(t *testing.T) {
	wantErr := errors.New("prepare failed")
	recorder := &embeddedRuntimeTestRecorder{err: wantErr}
	backend := &embeddedBackend{native: recorder}
	require.True(t, backend.Accepting())
	require.False(t, backend.CanFallbackBeforeVisibility(wantErr))
	require.ErrorContains(t, backend.Reconcile(1, nil, nil), "no external execution")
	require.NoError(t, backend.Close(t.Context()))
	require.True(t, recorder.closed)

	producerCalls := 0
	producer := func(_ context.Context, input compile.SiriusInput) error {
		producerCalls++
		require.Equal(t, 1, input.(embeddedInput).columns)
		return nil
	}
	request := compile.SiriusPrepareRequest{
		AccountID:   7,
		QueryID:     []byte("query"),
		Plan:        []byte("plan"),
		OutputTypes: []planpb.Type{{Id: int32(types.T_int64), Width: 8, NotNullable: true}},
		Headings:    []string{"answer"},
		Reads: []compile.SiriusReadDescriptor{{
			BindingID: 9,
			Database:  "db",
			Table:     "table",
			Schema:    "schema",
			Columns: []compile.SiriusReadColumn{{
				Name: "value", Type: planpb.Type{Id: int32(types.T_int64), Width: 8},
				PhysicalID: 11, Sequence: 12,
			}},
			Producer: producer,
		}},
	}
	execution, err := backend.Prepare(t.Context(), request)
	require.Nil(t, execution)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, request.AccountID, recorder.request.AccountID)
	require.Equal(t, request.QueryID, recorder.request.QueryID)
	require.Equal(t, request.Plan, recorder.request.Plan)
	require.Equal(t, []siriusbridge.Column{{
		OID: uint32(types.T_int64), Width: 8, Nullable: false, Name: "answer",
	}}, recorder.request.Columns)
	require.Len(t, recorder.request.Reads, 1)
	mapped := recorder.request.Reads[0]
	require.Equal(t, uint64(9), mapped.BindingID)
	require.Equal(t, "db", mapped.Database)
	require.Equal(t, "table", mapped.Table)
	require.Equal(t, "schema", mapped.Schema)
	require.Equal(t, []siriusbridge.ReadColumn{{
		Column:     siriusbridge.Column{OID: uint32(types.T_int64), Width: 8, Nullable: true, Name: "value"},
		PhysicalID: 11, Sequence: 12,
	}}, mapped.Columns)
	require.NotNil(t, mapped.Producer)
	require.NoError(t, mapped.Producer(t.Context(), nil))
	require.Equal(t, 1, producerCalls)
}

func TestEmbeddedSiriusStubFailsClosed(t *testing.T) {
	if siriusbridge.Available() {
		t.Skip("native Sirius build exercises the tagged launcher")
	}
	s := &service{}
	require.ErrorContains(t, s.startEmbeddedSiriusRuntime(t.Context()), "not available")
}

type embeddedLeaseRecorder struct {
	rows                uint32
	vectors             []siriusbridge.Vector
	capacity            uint64
	publishes, releases int
	err                 error
}

func (l *embeddedLeaseRecorder) Capacity() uint64 { return l.capacity }
func (l *embeddedLeaseRecorder) Publish(_ context.Context, rows uint32, vectors []siriusbridge.Vector) error {
	l.publishes++
	l.rows, l.vectors = rows, vectors
	return l.err
}
func (l *embeddedLeaseRecorder) Release() error { l.releases++; return l.err }

func TestEmbeddedSiriusInputLeaseAdapter(t *testing.T) {
	var input compile.SiriusInput = embeddedInput{}
	lease, err := input.Acquire(t.Context(), 1)
	require.Error(t, err)
	require.Nil(t, lease)

	failure := errors.New("native lease failure")
	recorder := &embeddedLeaseRecorder{capacity: 6, err: failure}
	var adapter compile.SiriusInputLease = embeddedInputLease{lease: recorder, columns: 1}
	require.Equal(t, uint64(6), adapter.Capacity())
	vector := compile.SiriusInputVector{Class: 1, Data: []byte{1}, Area: []byte{2, 3}, Nulls: []byte{4, 5, 6}}
	require.ErrorIs(t, adapter.Publish(t.Context(), 3, []compile.SiriusInputVector{vector}), failure)
	require.Equal(t, uint32(3), recorder.rows)
	require.Equal(t, []siriusbridge.Vector{{Class: 1, Data: vector.Data, Area: vector.Area, Nulls: vector.Nulls}}, recorder.vectors)
	require.ErrorIs(t, adapter.Release(), failure)
	require.Equal(t, 1, recorder.releases)
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(failure)
	require.ErrorIs(t, adapter.Publish(ctx, 3, []compile.SiriusInputVector{vector}), failure)
	require.Equal(t, 1, recorder.publishes)
	recorder.err = nil
	require.ErrorContains(t, adapter.Publish(t.Context(), 3, []compile.SiriusInputVector{vector, vector}), "registered schema")
	require.ErrorContains(t, adapter.Publish(t.Context(), 3, nil), "registered schema")
	require.Equal(t, 1, recorder.publishes)
	require.NoError(t, adapter.Publish(t.Context(), 3, []compile.SiriusInputVector{vector}))
	counts := &embeddedExecutionCounters{}
	adapter = embeddedInputLease{lease: recorder, columns: 1, counters: counts}
	require.NoError(t, adapter.Publish(t.Context(), 3, []compile.SiriusInputVector{vector}))
	require.Equal(t, uint64(3), counts.inputRows.Load())
	require.Equal(t, uint64(6), counts.inputBytes.Load())
}

type embeddedPreparedRecorder struct {
	results  []siriusbridge.Result
	stats    siriusbridge.ExecutionStats
	ready    bool
	closeErr error
	closes   int
}

func (q *embeddedPreparedRecorder) Run(_ context.Context, fill func(siriusbridge.Result) error) error {
	for _, r := range q.results {
		if err := fill(r); err != nil {
			return err
		}
	}
	return nil
}
func (q *embeddedPreparedRecorder) Close(context.Context) error { q.closes++; return q.closeErr }
func (q *embeddedPreparedRecorder) Statistics() (siriusbridge.ExecutionStats, bool) {
	return q.stats, q.ready
}

func TestEmbeddedSiriusExecutionOutputAndTerminalCleanup(t *testing.T) {
	for _, outcome := range []string{"success", "output error", "invalid output", "cleanup error", "fatal", "not terminal"} {
		t.Run(outcome, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, 42)
			q := &embeddedPreparedRecorder{results: []siriusbridge.Result{{Rows: 1, Backing: data, Vectors: []siriusbridge.Vector{{Data: data}}}}, ready: outcome != "not terminal", stats: siriusbridge.ExecutionStats{Terminal: true, SourceMask: 1, Fatal: outcome == "fatal"}}
			if outcome == "invalid output" {
				q.results[0].Vectors[0].Data = data[:7]
			}
			failure := errors.New("output/cleanup failure")
			if outcome == "cleanup error" {
				q.closeErr = failure
			}
			e := &embeddedExecution{query: q, streams: 2, counters: &embeddedExecutionCounters{}, request: compile.SiriusPrepareRequest{Headings: []string{"n"}, OutputTypes: []planpb.Type{{Id: int32(types.T_int64)}}}}
			fills := 0
			err := e.Run(t.Context(), proc.Mp(), nil, func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
				fills++
				require.Equal(t, 1, bat.RowCount())
				if outcome == "output error" {
					return failure
				}
				return nil
			})
			if outcome == "invalid output" {
				require.Error(t, err)
				require.Zero(t, fills)
			} else if outcome == "output error" {
				require.ErrorIs(t, err, failure)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, fills)
			}
			if outcome == "cleanup error" {
				require.ErrorIs(t, e.Cleanup(t.Context()), failure)
			} else {
				require.NoError(t, e.Cleanup(t.Context()))
			}
			require.Positive(t, e.counters.firstRow)
			q.closeErr = nil
			require.NoError(t, e.CleanupAfterRun(t.Context(), err), "terminal reporting cannot repeat side effects")
			require.Equal(t, 2, q.closes)
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}
