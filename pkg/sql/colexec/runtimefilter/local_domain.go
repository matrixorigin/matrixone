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
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const LocalDomainQueryCap uint64 = 64 << 20
const LocalDomainBatchRows = 8192

type LocalDomainPassReason uint8

const (
	LocalDomainActive LocalDomainPassReason = iota
	LocalDomainBudgetRefused
	LocalDomainAllocationRefused
	LocalDomainUnsupported
	LocalDomainPartial
	LocalDomainUnprofitable
	LocalDomainClosed
)

type LocalDomainSnapshot struct {
	EstimatedRows int64
	ObservedRows  int64
	FilterBytes   uint64
	PeakBytes     uint64
	Expected      int
	Finished      int
	PassReason    LocalDomainPassReason
}

// LocalDomain owns one complete integer domain for one execution generation.
// Compile closes it only after every source and probe scope has stopped.
type LocalDomain struct {
	mu            sync.Mutex
	typ           types.T
	bits          uint64
	hashes        uint32
	seed          uint64
	peak          uint64
	filterBytes   uint64
	estimatedRows int64
	maxProbeNDV   float64
	maxSources    int
	expected      int
	finished      int
	sources       []*LocalDomainSource
	generation    *process.ExecutionResourceGeneration
	board         *message.MessageBoard
	reservation   *process.ExecutionTransientMemoryReservation
	bloom         *bloomfilter.CBloomFilter
	pass          bool
	passReason    LocalDomainPassReason
	sealed        bool
	closed        bool
	err           error
	nonempty      bool
	buildRows     int64
	buildTime     time.Duration
	done          chan struct{}
}

// LocalDomainBinding is immutable compile-owned routing metadata. It contains no
// borrowed vector or payload and must never be serialized to another CN.
type LocalDomainBinding struct {
	Domain   *LocalDomain
	Column   string
	ProbeNDV float64
}

type LocalDomainSource struct {
	owner    *LocalDomain
	complete bool
}

func NewLocalDomain(typ types.T, rows int64, sources, probes int, maxProbeNDV float64) (*LocalDomain, uint64) {
	switch typ {
	case types.T_int16, types.T_int32, types.T_int64, types.T_uint16, types.T_uint32, types.T_uint64:
	default:
		return nil, 0
	}
	if rows <= 0 || rows >= 1_000_000_001 || sources <= 0 || probes <= 0 || sources > 1<<16 || probes > 1<<16 ||
		maxProbeNDV <= 1 || math.IsNaN(maxProbeNDV) || math.IsInf(maxProbeNDV, 0) {
		return nil, 0
	}
	bytes, ok := bloomfilter.EstimateCBloomFilterMemoryBytes(rows, .01)
	if !ok || bytes < 0 {
		return nil, 0
	}
	peak := uint64(bytes) + uint64(probes)*(LocalDomainBatchRows*9+256) + uint64(sources)*128
	if peak > LocalDomainQueryCap {
		return nil, 0
	}
	bits, hashes := bloomfilter.ComputeMemAndHashCountC(rows, .01)
	return &LocalDomain{typ: typ, bits: bits, hashes: hashes, seed: rand.Uint64(), peak: peak, filterBytes: uint64(bytes),
		estimatedRows: rows, maxProbeNDV: maxProbeNDV, maxSources: sources, done: make(chan struct{})}, peak
}

func (d *LocalDomain) prepare(proc *process.Process) error {
	g, err := proc.GetExecutionResourceBudget()
	if err != nil {
		d.mu.Lock()
		d.err = err
		d.disableLocked(LocalDomainPartial)
		d.mu.Unlock()
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || proc.GetMessageBoard() == nil {
		return moerr.NewInvalidStateNoCtx("invalid local domain generation")
	}
	if d.generation != nil {
		if d.generation != g || d.board != proc.GetMessageBoard() {
			return moerr.NewInvalidStateNoCtx("local domain generation mismatch")
		}
		return d.err
	}
	d.generation, d.board = g, proc.GetMessageBoard()
	d.reservation, err = g.ReserveTransientMemory(d.peak)
	if err != nil {
		if ClassifyOptionalFallback(err) != OptionalFallbackBudgetAdmission {
			d.err = err
			d.disableLocked(LocalDomainPartial)
			return err
		}
		d.disableLocked(LocalDomainBudgetRefused)
		return nil
	}
	var ok bool
	d.bloom, ok = bloomfilter.TryNewCBloomFilterWithSeed(d.bits, d.hashes, d.seed)
	if !ok {
		d.disableLocked(LocalDomainAllocationRefused)
	}
	return nil
}

func (d *LocalDomain) PrepareProbe(proc *process.Process) error { return d.prepare(proc) }

func (d *LocalDomain) KeyType() types.T { return d.typ }

func (d *LocalDomain) Enabled() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.pass && !d.closed && d.err == nil
}

func (d *LocalDomain) Begin(proc *process.Process, id, count int) (*LocalDomainSource, error) {
	if err := d.prepare(proc); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || count <= 0 || count > d.maxSources || id < 0 || id >= count {
		return nil, moerr.NewInvalidStateNoCtx("invalid local domain source identity")
	}
	if d.expected == 0 {
		d.expected = count
		d.sources = make([]*LocalDomainSource, count)
	} else if d.expected != count {
		return nil, moerr.NewInvalidStateNoCtx("inconsistent local domain source count")
	}
	if d.sources[id] != nil {
		return nil, moerr.NewInvalidStateNoCtx("duplicate local domain source")
	}
	p := &LocalDomainSource{owner: d}
	d.sources[id] = p
	return p, nil
}

func (d *LocalDomain) disableLocked(reason LocalDomainPassReason) {
	if !d.pass {
		d.passReason = reason
	}
	d.pass = true
	if !d.sealed {
		d.sealed = true
		close(d.done)
	}
}

func (d *LocalDomain) Disable() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.disableLocked(LocalDomainUnsupported)
}

func (p *LocalDomainSource) AddVector(ctx context.Context, vec *vector.Vector, rows int) error {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	d := p.owner
	d.mu.Lock()
	defer d.mu.Unlock()
	if p.complete || d.closed {
		return moerr.NewInvalidStateNoCtx("local domain source is complete")
	}
	if d.pass {
		return nil
	}
	if vec == nil || vec.GetType().Oid != d.typ || vec.HasGrouping() || rows < 0 || rows > LocalDomainBatchRows ||
		(!vec.IsConst() && vec.Length() != rows) || (vec.IsConst() && rows > 0 && vec.Length() == 0) {
		d.disableLocked(LocalDomainUnsupported)
		return nil
	}
	if rows == 0 || vec.IsConstNull() {
		return nil
	}
	keys := rows
	if !vec.IsConst() {
		keys -= vec.GetNulls().CountRange(0, uint64(rows))
	}
	d.buildRows += int64(keys)
	if float64(d.buildRows)/d.maxProbeNDV >= .1 {
		// PASS needs no complete-domain proof. Stop optional work as soon
		// as every target is already unprofitable; normal query work continues.
		d.disableLocked(LocalDomainUnprofitable)
		return nil
	}
	start := time.Now()
	d.bloom.AddVector(vec)
	d.buildTime += time.Since(start)
	if keys > 0 {
		d.nonempty = true
	}
	return nil
}

func (p *LocalDomainSource) Complete(pass bool) {
	if p == nil {
		return
	}
	d := p.owner
	d.mu.Lock()
	defer d.mu.Unlock()
	if p.complete {
		return
	}
	p.complete = true
	d.finished++
	if pass {
		d.disableLocked(LocalDomainPartial)
	}
	if d.finished == d.expected && !d.sealed {
		d.sealed = true
		close(d.done)
	}
}

func (d *LocalDomain) Wait(ctx context.Context) (*bloomfilter.CBloomFilter, bool, error) {
	select {
	case <-ctx.Done():
		return nil, false, context.Cause(ctx)
	case <-d.done:
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if ctx.Err() != nil {
		return nil, false, context.Cause(ctx)
	}
	if d.closed || d.pass {
		return nil, false, d.err
	}
	return d.bloom, !d.nonempty, nil
}

func (d *LocalDomain) Stats() (rows int64, buildTime time.Duration, bytes uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.buildRows, d.buildTime, d.peak
}

// ProbeEligible must only authorize use of completed immutable membership.
// Unknown cost hints and partial/PASS domains never authorize rejection.
func (d *LocalDomain) ProbeEligible(ndv float64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return ndv > 1 && !math.IsNaN(ndv) && !math.IsInf(ndv, 0) && !d.closed && !d.pass &&
		d.expected > 0 && d.finished == d.expected && float64(d.buildRows)/ndv < .1
}

func (d *LocalDomain) Snapshot() LocalDomainSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	bytes := uint64(0)
	if d.bloom != nil {
		bytes = d.filterBytes
	}
	return LocalDomainSnapshot{EstimatedRows: d.estimatedRows, ObservedRows: d.buildRows,
		FilterBytes: bytes, PeakBytes: d.peak, Expected: d.expected, Finished: d.finished, PassReason: d.passReason}
}

func (d *LocalDomain) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	d.closed = true
	d.disableLocked(LocalDomainClosed)
	if d.bloom != nil {
		d.bloom.Free()
		d.bloom = nil
	}
	if d.reservation != nil {
		d.reservation.Release()
		d.reservation = nil
	}
}
