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
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/siriusbridge"
	"go.uber.org/zap"
)

func validateSiriusEmbeddedBuild() error {
	if !siriusbridge.Available() {
		return moerr.NewBadConfigNoCtx("Sirius embedded backend is not available in this build; rebuild with MO_SIRIUS=1")
	}
	return nil
}

func validateSiriusEmbeddedConfig(c *SiriusConfig) error {
	if c.InputMode == "" {
		c.InputMode = "mo"
	}
	if c.InputMode != "mo" {
		return moerr.NewBadConfigNoCtx("embedded Sirius TAE admission is not yet available")
	}
	if c.BenchmarkNoGC {
		return moerr.NewBadConfigNoCtx("embedded MO input does not use benchmark-no-gc")
	}
	if c.GPUStreams == 0 {
		c.GPUStreams = 2
	}
	if c.MaxWaitingQueries == 0 {
		c.MaxWaitingQueries = 16
	}
	return (siriusbridge.Config{ConfigPath: c.NativeConfigPath, GPUStreams: c.GPUStreams, MaxWaiting: c.MaxWaitingQueries, CleanupTimeout: c.CleanupTimeout.Duration}).Validate()
}

type embeddedRuntime interface {
	Accepting() bool
	Close(context.Context) error
	Prepare(context.Context, siriusbridge.Request) (*siriusbridge.Query, error)
}

type embeddedBackend struct {
	native  embeddedRuntime
	streams uint32
}

func (b *embeddedBackend) Accepting() bool { return b.native.Accepting() }

func (b *embeddedBackend) Close(ctx context.Context) error      { return b.native.Close(ctx) }
func (*embeddedBackend) CanFallbackBeforeVisibility(error) bool { return false }
func (*embeddedBackend) Reconcile(uint64, []byte, func(context.Context) error) error {
	return moerr.NewNotSupportedNoCtx("embedded Sirius has no external execution to reconcile")
}

func (b *embeddedBackend) Prepare(ctx context.Context, req compile.SiriusPrepareRequest) (compile.SiriusExecution, error) {
	counters := &embeddedExecutionCounters{}
	nativeReq := siriusbridge.Request{AccountID: req.AccountID, QueryID: req.QueryID, Snapshot: req.Snapshot, Plan: req.Plan, Deadline: req.Deadline, Release: req.Release}
	if len(req.OutputTypes) == len(req.Headings) {
		for i, t := range req.OutputTypes {
			nativeReq.Columns = append(nativeReq.Columns, siriusbridge.Column{OID: uint32(t.Id), Width: t.Width, Scale: t.Scale, Nullable: !t.NotNullable, Name: req.Headings[i]})
		}
	}
	for _, read := range req.Reads {
		r := siriusbridge.Read{BindingID: read.BindingID, Database: read.Database, Table: read.Table, Schema: read.Schema}
		for _, c := range read.Columns {
			r.Columns = append(r.Columns, siriusbridge.ReadColumn{Column: siriusbridge.Column{OID: uint32(c.Type.Id), Width: c.Type.Width, Scale: c.Type.Scale, Nullable: !c.Type.NotNullable, Name: c.Name}, PhysicalID: c.PhysicalID, Sequence: c.Sequence})
		}
		if read.Producer != nil {
			producer := read.Producer
			columns := len(read.Columns)
			r.Producer = func(ctx context.Context, input *siriusbridge.Input) error {
				return producer(ctx, embeddedInput{input: input, columns: columns, counters: counters})
			}
		}
		nativeReq.Reads = append(nativeReq.Reads, r)
	}
	q, err := b.native.Prepare(ctx, nativeReq)
	if err != nil {
		return nil, err
	}
	return &embeddedExecution{query: q, request: req, counters: counters, streams: b.streams}, nil
}

type embeddedInput struct {
	input    *siriusbridge.Input
	columns  int
	counters *embeddedExecutionCounters
}

func (i embeddedInput) Acquire(ctx context.Context, bytes uint64) (compile.SiriusInputLease, error) {
	lease, err := i.input.Acquire(ctx, bytes)
	if err != nil {
		return nil, err
	}
	return embeddedInputLease{lease: lease, columns: i.columns, counters: i.counters}, nil
}

type embeddedNativeInputLease interface {
	Capacity() uint64
	Publish(context.Context, uint32, []siriusbridge.Vector) error
	Release() error
}

type embeddedInputLease struct {
	lease    embeddedNativeInputLease
	columns  int
	counters *embeddedExecutionCounters
}

func (l embeddedInputLease) Capacity() uint64 { return l.lease.Capacity() }
func (l embeddedInputLease) Release() error   { return l.lease.Release() }

func (l embeddedInputLease) Publish(ctx context.Context, rows uint32, vs []compile.SiriusInputVector) error {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	// Prepare validates and bounds the registered schema. Reject malformed
	// producer descriptors before allocating their converted representation.
	if len(vs) == 0 || len(vs) != l.columns {
		return moerr.NewInvalidInputNoCtx("Sirius input vector count does not match registered schema")
	}
	vectors := make([]siriusbridge.Vector, len(vs))
	for n, v := range vs {
		vectors[n] = siriusbridge.Vector{Class: v.Class, Data: v.Data, Area: v.Area, Nulls: v.Nulls}
	}
	if err := l.lease.Publish(ctx, rows, vectors); err != nil {
		return err
	}
	if l.counters != nil {
		l.counters.inputRows.Add(uint64(rows))
		var bytes uint64
		for _, v := range vs {
			bytes += uint64(len(v.Data) + len(v.Area) + len(v.Nulls))
		}
		l.counters.inputBytes.Add(bytes)
	}
	return nil
}

var _ compile.SiriusInput = embeddedInput{}
var _ compile.SiriusInputLease = embeddedInputLease{}

type embeddedExecution struct {
	query    *siriusbridge.Query
	request  compile.SiriusPrepareRequest
	counters *embeddedExecutionCounters
	streams  uint32
	recorded sync.Once
}

type embeddedExecutionCounters struct {
	inputRows, inputBytes atomic.Uint64
	mu                    sync.Mutex
	started               time.Time
	firstRow              time.Duration
}

func (e *embeddedExecution) Run(ctx context.Context, mp *mpool.MPool, counters *perfcounter.CounterSet, fill func(*batch.Batch, *perfcounter.CounterSet) error) error {
	e.counters.mu.Lock()
	e.counters.started = time.Now()
	e.counters.mu.Unlock()
	return e.query.Run(ctx, func(result siriusbridge.Result) error {
		if result.Rows > 0 {
			e.counters.mu.Lock()
			if e.counters.firstRow == 0 {
				e.counters.firstRow = time.Since(e.counters.started)
			}
			e.counters.mu.Unlock()
		}
		bat, err := decodeEmbeddedSiriusResult(result, e.request, mp)
		if err != nil {
			return err
		}
		defer bat.Clean(mp)
		return fill(bat, counters)
	})
}
func (e *embeddedExecution) Cleanup(ctx context.Context) error {
	err := e.query.Close(ctx)
	if stats, ready := e.query.Statistics(); ready {
		e.recorded.Do(func() {
			e.counters.mu.Lock()
			firstRow, started := e.counters.firstRow, e.counters.started
			e.counters.mu.Unlock()
			var wall time.Duration
			if !started.IsZero() {
				wall = time.Since(started)
			}
			health := "healthy"
			if err != nil {
				health = "draining"
			}
			if stats.Fatal {
				health = "unavailable"
			}
			logutil.Info("Sirius embedded execution",
				zap.String("query_id", hex.EncodeToString(e.request.QueryID)), zap.Uint64("account_id", e.request.AccountID),
				zap.String("backend", "embedded"), zap.String("scan_mode", "mo"), zap.Bool("fallback", false),
				zap.Uint32("gpu_streams", e.streams), zap.Int("read_bindings", len(e.request.Reads)),
				zap.Uint64("input_rows", e.counters.inputRows.Load()), zap.Uint64("input_payload_bytes", e.counters.inputBytes.Load()),
				zap.Float64("first_row_seconds", firstRow.Seconds()), zap.Float64("wall_seconds", wall.Seconds()),
				zap.String("terminal_health", health), zap.Any("execution_stats", stats))
		})
	}
	return err
}
func (e *embeddedExecution) CleanupAfterRun(ctx context.Context, _ error) error {
	return e.Cleanup(ctx)
}

var _ compile.SiriusBackend = (*embeddedBackend)(nil)
var _ compile.SiriusExecution = (*embeddedExecution)(nil)
