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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/siriusbridge"
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

type embeddedBackend struct{ native embeddedRuntime }

func (b *embeddedBackend) Accepting() bool { return b.native.Accepting() }

func (b *embeddedBackend) Close(ctx context.Context) error      { return b.native.Close(ctx) }
func (*embeddedBackend) CanFallbackBeforeVisibility(error) bool { return false }
func (*embeddedBackend) Reconcile(uint64, []byte, func(context.Context) error) error {
	return moerr.NewNotSupportedNoCtx("embedded Sirius has no external execution to reconcile")
}

func (b *embeddedBackend) Prepare(ctx context.Context, req compile.SiriusPrepareRequest) (compile.SiriusExecution, error) {
	nativeReq := siriusbridge.Request{AccountID: req.AccountID, QueryID: req.QueryID, Snapshot: req.Snapshot, Plan: req.Plan, Deadline: req.Deadline, Release: req.Release}
	if len(req.OutputTypes) == len(req.Headings) {
		for i, t := range req.OutputTypes {
			nativeReq.Columns = append(nativeReq.Columns, siriusbridge.Column{OID: uint32(t.Id), Width: t.Width, Scale: t.Scale, Nullable: !t.NotNullable, Name: req.Headings[i]})
		}
	}
	for _, read := range req.Reads {
		r := siriusbridge.Read{BindingID: read.BindingID, Database: read.Database, Table: read.Table, Schema: read.Schema, TAEManifest: read.TAEManifest, DataRoot: read.DataRoot}
		for _, c := range read.Columns {
			r.Columns = append(r.Columns, siriusbridge.ReadColumn{Column: siriusbridge.Column{OID: uint32(c.Type.Id), Width: c.Type.Width, Scale: c.Type.Scale, Nullable: !c.Type.NotNullable, Name: c.Name}, PhysicalID: c.PhysicalID, Sequence: c.Sequence})
		}
		if read.Producer != nil {
			producer := read.Producer
			r.Producer = func(ctx context.Context, input *siriusbridge.Input) error { return producer(ctx, embeddedInput{input}) }
		}
		nativeReq.Reads = append(nativeReq.Reads, r)
	}
	q, err := b.native.Prepare(ctx, nativeReq)
	if err != nil {
		return nil, err
	}
	return &embeddedExecution{query: q, request: req}, nil
}

type embeddedInput struct{ input *siriusbridge.Input }

func (i embeddedInput) Push(ctx context.Context, rows uint32, vs []compile.SiriusInputVector) error {
	vectors := make([]siriusbridge.Vector, len(vs))
	for n, v := range vs {
		vectors[n] = siriusbridge.Vector{Class: v.Class, Data: v.Data, Area: v.Area, Nulls: v.Nulls}
	}
	return i.input.Push(ctx, rows, vectors)
}

type embeddedExecution struct {
	query   *siriusbridge.Query
	request compile.SiriusPrepareRequest
}

func (e *embeddedExecution) Run(ctx context.Context, mp *mpool.MPool, counters *perfcounter.CounterSet, fill func(*batch.Batch, *perfcounter.CounterSet) error) error {
	return e.query.Run(ctx, func(result siriusbridge.Result) error {
		if len(result.Vectors) != len(e.request.OutputTypes) {
			return moerr.NewInvalidInputNoCtx("Sirius output schema mismatch")
		}
		bat := batch.NewWithSize(len(result.Vectors))
		defer bat.Clean(mp)
		bat.Attrs = e.request.Headings
		bat.SetRowCount(int(result.Rows))
		for i, v := range result.Vectors {
			t := e.request.OutputTypes[i]
			typ := types.New(types.T(t.Id), t.Width, t.Scale)
			if v.Class != 0 || uint64(len(v.Data)) != uint64(result.Rows)*uint64(typ.TypeSize()) || len(v.Nulls)%8 != 0 {
				return moerr.NewInvalidInputNoCtx("invalid Sirius native vector layout")
			}
			vec, err := vector.NewVecWithDataCopy(typ, int(result.Rows), v.Data, v.Area, mp)
			if err != nil {
				return err
			}
			bat.Vecs[i] = vec
			for row := uint32(0); row < result.Rows; row++ {
				word := int(row/64) * 8
				if word < len(v.Nulls) && binary.LittleEndian.Uint64(v.Nulls[word:word+8])&(uint64(1)<<(row%64)) != 0 {
					vec.SetNull(uint64(row))
				}
			}
		}
		return fill(bat, counters)
	})
}
func (e *embeddedExecution) Cleanup(ctx context.Context) error {
	return e.query.Close(ctx)
}
func (e *embeddedExecution) CleanupAfterRun(ctx context.Context, _ error) error {
	return e.Cleanup(ctx)
}

var _ compile.SiriusBackend = (*embeddedBackend)(nil)
var _ compile.SiriusExecution = (*embeddedExecution)(nil)
