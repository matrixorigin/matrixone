// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package message

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// JoinSpillSource owns immutable anonymous files. Readers use independent offsets;
// dup(2) would share the open-file description and is not a safe replacement.
// Closing the source retires its reference; readers keep the files alive.
type JoinSpillSource struct {
	mu      sync.Mutex
	files   []*os.File
	refs    int
	retired bool
}

func NewJoinSpillSource(files []*os.File) *JoinSpillSource {
	return &JoinSpillSource{files: files, refs: 1}
}

type JoinSpillReader struct {
	*io.SectionReader
	source *JoinSpillSource
	once   sync.Once
}

func (s *JoinSpillSource) Reader(bucket int) (*JoinSpillReader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return nil, errors.New("join spill source is retired")
	}
	if bucket < 0 || bucket >= len(s.files) {
		return nil, errors.New("invalid join spill bucket")
	}
	f := s.files[bucket]
	if f == nil {
		return &JoinSpillReader{SectionReader: io.NewSectionReader(emptySpillReader{}, 0, 0)}, nil
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	s.refs++
	return &JoinSpillReader{SectionReader: io.NewSectionReader(f, 0, info.Size()), source: s}, nil
}

type emptySpillReader struct{}

func (emptySpillReader) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }

func (r *JoinSpillReader) Close() error {
	r.once.Do(func() {
		if r.source != nil {
			r.source.release()
		}
	})
	return nil
}

func (s *JoinSpillSource) release() {
	s.mu.Lock()
	s.refs--
	var files []*os.File
	if s.refs == 0 {
		files, s.files = s.files, nil
	}
	s.mu.Unlock()
	closeJoinSpillFiles(files)
}

func (s *JoinSpillSource) Close() {
	s.mu.Lock()
	if s.retired {
		s.mu.Unlock()
		return
	}
	s.retired = true
	s.mu.Unlock()
	s.release()
}

// TakeFiles transfers exclusive ownership to the one logical spill finalizer.
// It cannot race a reader: the source must have no borrowed references.
func (s *JoinSpillSource) TakeFiles() ([]*os.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired || s.refs != 1 {
		return nil, errors.New("join spill source is not exclusively owned")
	}
	files := s.files
	s.files, s.refs, s.retired = nil, 0, true
	return files, nil
}

func closeJoinSpillFiles(files []*os.File) {
	for _, f := range files {
		if f != nil {
			_ = f.Close()
		}
	}
}

type joinSpillPhase uint8

const (
	joinSpillCollecting joinSpillPhase = iota
	joinSpillClaimed
	joinSpillFinished
	joinSpillAborted
)

// JoinSpillGroup is shared by actual local copies of a broadcast probe. Seal is
// called after duplication, before Prepare. A fresh epoch retires all old state.
// No file close, IO or blocking wait occurs while mu is held.
type JoinSpillGroup struct {
	mu             sync.Mutex
	epoch          uint64
	phase          joinSpillPhase
	expected       map[uint64]struct{}
	expectedScopes map[uint32]struct{}
	sealedScopes   map[uint32]struct{}
	registered     map[uint64][]*os.File
	order          []uint64
	source         *JoinSpillSource
	memoryMap      *JoinMap
	spilled        bool
	stop           func() bool
	cancel         context.CancelFunc
	ctx            context.Context
	onTerminal     func()
	normalDone     map[uint64]struct{}
}

func NewJoinSpillGroup() *JoinSpillGroup { return &JoinSpillGroup{} }

// Begin runs once at query entry, before any build/probe scopes start. The
// build may publish before actual probe duplication seals the participant IDs.
func (g *JoinSpillGroup) Begin(ctx context.Context) uint64 {
	g.Abort(0)
	child, cancel := context.WithCancel(ctx)
	g.mu.Lock()
	g.epoch++
	epoch := g.epoch
	g.phase = joinSpillCollecting
	g.expected, g.order, g.registered = nil, nil, nil
	g.expectedScopes, g.sealedScopes = nil, nil
	g.normalDone = nil
	g.source, g.spilled, g.ctx, g.cancel = nil, false, child, cancel
	g.memoryMap = nil
	g.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { g.Abort(epoch) })
	g.mu.Lock()
	if g.epoch == epoch && g.phase == joinSpillCollecting {
		g.stop = stop
		stop = nil
	}
	g.mu.Unlock()
	if stop != nil {
		stop()
	}
	return epoch
}

func (g *JoinSpillGroup) SealActual(epoch uint64, ids []uint64) error {
	expected := make(map[uint64]struct{}, len(ids))
	order := append([]uint64(nil), ids...)
	for _, id := range ids {
		if _, exists := expected[id]; exists {
			return errors.New("duplicate broadcast spill participant")
		}
		expected[id] = struct{}{}
	}
	if len(ids) == 0 {
		return errors.New("empty broadcast spill topology")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if epoch != g.epoch || g.phase != joinSpillCollecting {
		return errors.New("broadcast spill generation is unavailable")
	}
	if g.expected != nil {
		if len(g.order) == len(ids) {
			same := true
			for i := range ids {
				same = same && ids[i] == g.order[i]
			}
			if same {
				return nil
			}
		}
		return errors.New("broadcast spill topology already sealed")
	}
	g.expected, g.order = expected, order
	g.registered = make(map[uint64][]*os.File, len(ids))
	return nil
}

// Seal is a convenience for isolated operator/tests with an already known
// topology. Production begins the epoch at query entry and seals actual copies.
func (g *JoinSpillGroup) Seal(ctx context.Context, ids []uint64) uint64 {
	epoch := g.Begin(ctx)
	if err := g.SealActual(epoch, ids); err != nil {
		panic(err)
	}
	return epoch
}

func (g *JoinSpillGroup) Epoch() uint64   { g.mu.Lock(); defer g.mu.Unlock(); return g.epoch }
func (g *JoinSpillGroup) IsSpilled() bool { g.mu.Lock(); defer g.mu.Unlock(); return g.spilled }

// PublishBuild always consumes files, including a cancelled publication.
func (g *JoinSpillGroup) PublishBuild(files []*os.File) error {
	source := NewJoinSpillSource(files)
	g.mu.Lock()
	if g.phase != joinSpillCollecting || g.source != nil {
		g.mu.Unlock()
		source.Close()
		return errors.New("broadcast spill generation is unavailable")
	}
	g.source, g.spilled = source, true
	g.mu.Unlock()
	return nil
}

// JoinSpillWork is the sole owner after claim. Abort cancels Ctx but does not
// close its files concurrently with the consumer. Close is the terminal owner.
type JoinSpillWork struct {
	Ctx    context.Context
	Build  *JoinSpillSource
	Probes [][]*os.File
	group  *JoinSpillGroup
	epoch  uint64
	once   sync.Once
}

func (w *JoinSpillWork) Close() {
	w.once.Do(func() {
		if w.Build != nil {
			w.Build.Close()
		}
		for _, files := range w.Probes {
			closeJoinSpillFiles(files)
		}
		w.Probes = nil
		w.group.finish(w.epoch)
	})
}

// Register consumes files only when accepted. Empty participants must register.
// The final registrant receives all sources; earlier participants never wait.
func (g *JoinSpillGroup) Register(epoch uint64, id uint64, files []*os.File) (*JoinSpillWork, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if epoch != g.epoch || g.phase != joinSpillCollecting || g.source == nil {
		return nil, errors.New("broadcast spill generation is unavailable")
	}
	if _, ok := g.expected[id]; !ok {
		return nil, fmt.Errorf("unexpected broadcast spill participant %d", id)
	}
	if _, ok := g.registered[id]; ok {
		return nil, fmt.Errorf("duplicate broadcast spill participant %d", id)
	}
	g.registered[id] = files
	if len(g.registered) != len(g.expected) || len(g.sealedScopes) != len(g.expectedScopes) {
		return nil, nil
	}
	probes := make([][]*os.File, 0, len(g.order))
	for _, participant := range g.order {
		probes = append(probes, g.registered[participant])
	}
	work := &JoinSpillWork{Ctx: g.ctx, Build: g.source, Probes: probes, group: g, epoch: epoch}
	g.source, g.registered, g.phase = nil, nil, joinSpillClaimed
	return work, nil
}

func (g *JoinSpillGroup) finish(epoch uint64) {
	g.mu.Lock()
	if g.epoch != epoch {
		g.mu.Unlock()
		return
	}
	g.phase = joinSpillFinished
	memoryMap := g.memoryMap
	g.memoryMap = nil
	stop, cancel := g.stop, g.cancel
	g.stop, g.cancel = nil, nil
	g.mu.Unlock()
	if memoryMap != nil {
		memoryMap.Free()
	}
	if stop != nil {
		stop()
	}
	if cancel != nil {
		cancel()
	}
	if g.onTerminal != nil {
		g.onTerminal()
	}
}

// Abort(0) targets the current epoch. A claimed owner keeps its files and is
// cancelled; collecting files are detached and closed by this call.
func (g *JoinSpillGroup) Abort(epoch uint64) {
	if g == nil {
		return
	}
	g.mu.Lock()
	if epoch != 0 && epoch != g.epoch {
		g.mu.Unlock()
		return
	}
	if g.phase == joinSpillFinished || g.phase == joinSpillAborted {
		g.mu.Unlock()
		return
	}
	source, registered := g.source, g.registered
	memoryMap := g.memoryMap
	g.memoryMap = nil
	stop, cancel := g.stop, g.cancel
	g.source, g.registered, g.stop, g.cancel, g.phase = nil, nil, nil, nil, joinSpillAborted
	g.mu.Unlock()
	if stop != nil {
		stop()
	}
	if cancel != nil {
		cancel()
	}
	if memoryMap != nil {
		memoryMap.Free()
	}
	if source != nil {
		source.Close()
	}
	for _, files := range registered {
		closeJoinSpillFiles(files)
	}
	if g.onTerminal != nil {
		g.onTerminal()
	}
}

// BeginScopes freezes CN-local scope identities received from the pipeline
// codec. Each scope seals its actual parallel copies before its Prepare.
func (g *JoinSpillGroup) BeginScopes(ctx context.Context, scopes []uint32) uint64 {
	epoch := g.Begin(ctx)
	expected := make(map[uint32]struct{}, len(scopes))
	for _, id := range scopes {
		expected[id] = struct{}{}
	}
	g.mu.Lock()
	g.expectedScopes = expected
	g.sealedScopes = make(map[uint32]struct{}, len(scopes))
	g.expected = make(map[uint64]struct{})
	g.registered = make(map[uint64][]*os.File)
	g.mu.Unlock()
	return epoch
}

func (g *JoinSpillGroup) SealScope(epoch uint64, scope uint32, parallelIDs []int32) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if epoch != g.epoch || g.phase != joinSpillCollecting {
		return errors.New("broadcast spill generation is unavailable")
	}
	if _, ok := g.expectedScopes[scope]; !ok {
		return errors.New("unexpected broadcast spill scope")
	}
	if _, ok := g.sealedScopes[scope]; ok {
		return nil
	}
	if len(parallelIDs) == 0 {
		return errors.New("empty actual broadcast spill scope")
	}
	for _, id := range parallelIDs {
		if id < 0 {
			return errors.New("negative broadcast spill parallel ID")
		}
		participant := uint64(scope)<<32 | uint64(uint32(id))
		if _, ok := g.expected[participant]; ok {
			return errors.New("duplicate broadcast spill parallel ID")
		}
		g.expected[participant] = struct{}{}
		g.order = append(g.order, participant)
	}
	g.sealedScopes[scope] = struct{}{}
	return nil
}

func (g *JoinSpillGroup) terminal() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.phase == joinSpillFinished || g.phase == joinSpillAborted
}

func (g *JoinSpillGroup) FinishParticipant(epoch, id uint64) {
	g.mu.Lock()
	if epoch != g.epoch || g.spilled || g.phase != joinSpillCollecting {
		g.mu.Unlock()
		return
	}
	if _, ok := g.expected[id]; !ok {
		g.mu.Unlock()
		return
	}
	if g.normalDone == nil {
		g.normalDone = make(map[uint64]struct{})
	}
	g.normalDone[id] = struct{}{}
	done := len(g.sealedScopes) == len(g.expectedScopes) && len(g.normalDone) == len(g.expected)
	g.mu.Unlock()
	if done {
		g.finish(epoch)
	}
}

// PublishMemory transfers the initial map reference to the generation. Actual
// receivers take their own references; planned DOP is not a lifetime counter.
func (g *JoinSpillGroup) PublishMemory(jm *JoinMap) error {
	g.mu.Lock()
	if g.phase != joinSpillCollecting || g.memoryMap != nil {
		g.mu.Unlock()
		if jm != nil {
			jm.Free()
		}
		return errors.New("broadcast generation is unavailable")
	}
	g.memoryMap = jm
	g.mu.Unlock()
	return nil
}

func (g *JoinSpillGroup) AcquireMemory(epoch uint64, jm *JoinMap) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if epoch != g.epoch || g.phase != joinSpillCollecting || g.memoryMap != jm {
		return false
	}
	jm.IncRef(1)
	return true
}
