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

package runtimefilter

import (
	"context"
	"math/rand/v2"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const LocalShuffleFilterQueryCap uint64 = 64 << 20
const LocalShuffleProbeRows = 8192

// LocalShuffleFilter belongs to one compiled AP execution. Compile closes it
// after scope execution/Reset has joined the producers, before closing accounts.
// No vector, Process, or filter is transported to another CN.
type LocalShuffleFilter struct {
	mu           sync.Mutex
	tag          int32
	typ          types.T
	nbits        uint64
	hashes       uint32
	seed         uint64
	bytes        uint64
	maxProducers int
	expected     int
	producers    []*LocalShuffleProducer
	generation   *process.ExecutionResourceGeneration
	reservation  *process.ExecutionTransientMemoryReservation
	board        *message.MessageBoard
	accumulator  *bloomfilter.CBloomFilter
	finished     int
	nonempty     bool
	pass         bool
	published    bool
	closed       bool
	done         chan struct{}
}

type LocalShuffleProducer struct {
	owner    *LocalShuffleFilter
	bloom    *bloomfilter.CBloomFilter
	complete bool
	pass     bool
	nonempty bool
}

func NewLocalShuffleFilter(tag int32, typ types.T, rows int64, producers int) (*LocalShuffleFilter, uint64) {
	switch typ {
	case types.T_int16, types.T_int32, types.T_int64, types.T_uint16, types.T_uint32, types.T_uint64:
	default:
		return nil, 0
	}
	if rows <= 0 || rows >= 1_000_000_001 || producers <= 0 {
		return nil, 0
	}
	bytes, ok := bloomfilter.EstimateCBloomFilterMemoryBytes(rows, .01)
	if !ok || uint64(bytes) > LocalShuffleFilterQueryCap/uint64(producers) {
		return nil, 0
	}
	peak := (uint64(bytes) + LocalShuffleProbeRows + 256) * uint64(producers)
	if peak > LocalShuffleFilterQueryCap {
		return nil, 0
	}
	bits, hashes := bloomfilter.ComputeMemAndHashCountC(rows, .01)
	return &LocalShuffleFilter{tag: tag, typ: typ, nbits: bits, hashes: hashes, seed: rand.Uint64(), bytes: uint64(bytes), maxProducers: producers, done: make(chan struct{})}, peak
}

// Begin uses the actual duplicated operator's ParallelID/MaxParallel contract.
// A topology mismatch is a query error, never a partial membership domain.
func (f *LocalShuffleFilter) Begin(proc *process.Process, id, count int) (*LocalShuffleProducer, error) {
	g, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || count <= 0 || count > f.maxProducers || id < 0 || id >= count {
		return nil, moerr.NewInvalidStateNoCtx("invalid local shuffle filter producer")
	}
	if err = f.prepareLocked(proc, g, count); err != nil {
		return nil, err
	}
	if f.expected == 0 {
		f.expected = count
		f.producers = make([]*LocalShuffleProducer, count)
	} else if f.expected != count {
		return nil, moerr.NewInvalidStateNoCtx("inconsistent local shuffle build producer count")
	}
	if f.producers[id] != nil {
		return nil, moerr.NewInvalidStateNoCtx("duplicate local shuffle filter producer")
	}
	p := &LocalShuffleProducer{owner: f}
	if !f.pass {
		var ok bool
		p.bloom, ok = bloomfilter.TryNewCBloomFilterWithSeed(f.nbits, f.hashes, f.seed)
		if !ok {
			f.pass = true
		}
	}
	f.producers[id] = p
	return p, nil
}

func (f *LocalShuffleFilter) prepareLocked(proc *process.Process, g *process.ExecutionResourceGeneration, count int) (err error) {
	if f.closed || count <= 0 || count > f.maxProducers {
		return moerr.NewInvalidStateNoCtx("invalid local shuffle filter fanout")
	}
	if proc.GetMessageBoard() == nil {
		return moerr.NewInvalidStateNoCtx("local shuffle filter has no message board")
	}
	if f.generation == nil {
		f.generation = g
		f.board = proc.GetMessageBoard()
		// Build and probe reader counts can shrink independently. Reserve the
		// compile-proven upper bound, and register only actual build producers.
		f.reservation, err = g.ReserveTransientMemory((f.bytes + LocalShuffleProbeRows + 256) * uint64(f.maxProducers))
		if err != nil {
			if ClassifyOptionalFallback(err) != OptionalFallbackBudgetAdmission {
				return err
			}
			f.pass = true
		}
	} else if f.generation != g || f.board != proc.GetMessageBoard() {
		return moerr.NewInvalidStateNoCtx("local shuffle filter generation or board mismatch")
	}
	return nil
}

func (f *LocalShuffleFilter) PrepareProbe(proc *process.Process, count int) (bool, error) {
	g, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err = f.prepareLocked(proc, g, count); err != nil {
		return false, err
	}
	return !f.pass, nil
}

func (p *LocalShuffleProducer) AddVector(vec *vector.Vector) {
	if p == nil || p.bloom == nil || p.pass || p.complete {
		return
	}
	if vec == nil || vec.GetType().Oid != p.owner.typ || vec.HasGrouping() {
		p.pass = true
		return
	}
	if vec.Length() == 0 || vec.IsConstNull() {
		return
	}
	p.bloom.AddVector(vec)
	if vec.IsConst() || vec.GetNulls().GetBitmap().CountRange(0, uint64(vec.Length())) < vec.Length() {
		p.nonempty = true
	}
}

// Complete transfers the producer's private filter to the coordinator. Reset
// also uses this path for spill, error and incomplete-build fallback.
func (p *LocalShuffleProducer) Complete(pass bool) error {
	if p == nil {
		return nil
	}
	f := p.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if p.complete {
		return nil
	}
	p.complete = true
	f.finished++
	f.pass = f.pass || pass || p.pass
	f.nonempty = f.nonempty || p.nonempty
	if p.bloom != nil {
		if f.pass {
			p.bloom.Free()
			p.bloom = nil
		} else if f.accumulator == nil {
			f.accumulator = p.bloom
		} else {
			err := f.accumulator.Merge(p.bloom)
			p.bloom.Free()
			p.bloom = nil
			if err != nil {
				f.pass = true
				return err
			}
		}
		p.bloom = nil
	}
	if f.finished == f.expected && !f.published {
		f.published = true
		typ := int32(message.RuntimeFilter_PASS)
		if !f.pass && !f.nonempty {
			typ = message.RuntimeFilter_DROP
		}
		message.SendMessage(message.RuntimeFilterMessage{Tag: f.tag, Typ: typ}, f.board)
		close(f.done)
	}
	return nil
}

// Ready allows an optional consumer to keep streaming until the complete
// membership domain exists. No partial producer filter may be consulted.
func (f *LocalShuffleFilter) Ready() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

func (f *LocalShuffleFilter) Wait(ctx context.Context, stopped <-chan struct{}) (*bloomfilter.CBloomFilter, error) {
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	select {
	case <-stopped:
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-f.done:
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if f.closed || f.pass {
		return nil, nil
	}
	return f.accumulator, nil
}

// Close is a compile-owned safety net, called only after all operator work has
// stopped. It also covers partial Prepare and unstarted producers.
func (f *LocalShuffleFilter) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	if !f.published {
		f.published = true
		close(f.done)
	}
	for _, p := range f.producers {
		if p != nil && p.bloom != nil {
			p.bloom.Free()
			p.bloom = nil
		}
	}
	if f.accumulator != nil {
		f.accumulator.Free()
		f.accumulator = nil
	}
	if f.reservation != nil {
		f.reservation.Release()
		f.reservation = nil
	}
}
