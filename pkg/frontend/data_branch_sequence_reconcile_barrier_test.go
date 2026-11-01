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

package frontend

// Deterministic barrier oracles for the reconcile unit's canonical
// validate-before-proceed lock order (design §9 L2/K2; #30359 review).
// The harness runs the REAL production reconcile function
// (reconcileMergeSequenceWatermarks -> SequenceAutonomousEnv.Run ->
// plan/validate/SELECT FOR UPDATE/monotone conditional UPDATE per identity)
// in competing participant goroutines against a model catalog, row store,
// and lock service. Interleavings are driven by gates, never by sleeps: the
// production plan hook parks a unit at its plan point until the director
// releases that exact arrival, and one-shot lock parks freeze a competitor
// while holding a named row. A rank witness fails the test the moment any
// unit blocks on a row whose rank is not strictly ahead of every row rank
// it already holds (the canonical-order no-cycle invariant), and a watchdog
// fails a hang instead of letting a deadlock pass silently.
//
// Row visibility models the production read-committed shape: a SELECT FOR
// UPDATE reads the committed-latest row under the held row lock, updates
// accumulate in the attempt's overlay, and commit applies the overlay
// atomically while abort discards it. Catalog probes read committed-latest
// identities, so a committed drop+recreate drifts a planned identity
// exactly the way the design's committed-latest catalog path exposes.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------
// Model catalog: committed name -> (relation id, 7-column row). Engine
// probes read committed-latest state so a competitor's committed DDL
// (drop+recreate) drifts a planned identity between plan and validation.
// Attempt writes accumulate in per-attempt overlays; commit applies them
// atomically under the store lock.
// ---------------------------------------------------------------------

type seqBarrierOverlay struct {
	rowWrites map[[2]string]seqStoreRow
	deletes   map[[2]string]bool
	idWrites  map[[2]string]uint64
}

func newSeqBarrierOverlay() *seqBarrierOverlay {
	return &seqBarrierOverlay{
		rowWrites: map[[2]string]seqStoreRow{},
		deletes:   map[[2]string]bool{},
		idWrites:  map[[2]string]uint64{},
	}
}

type seqBarrierStore struct {
	mu   sync.Mutex
	ids  map[[2]string]uint64
	rows seqRows
}

func newSeqBarrierStore() *seqBarrierStore {
	return &seqBarrierStore{ids: map[[2]string]uint64{}, rows: seqRows{}}
}

func (s *seqBarrierStore) add(db, table string, id uint64, row seqStoreRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids[[2]string{db, table}] = id
	s.rows[[2]string{db, table}] = row
}

func (s *seqBarrierStore) committedID(db, table string) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.ids[[2]string{db, table}]
	return id, ok
}

func (s *seqBarrierStore) committedRow(db, table string) (seqStoreRow, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[[2]string{db, table}]
	return row, ok
}

func (s *seqBarrierStore) get(db, table string) (seqStoreRow, bool) {
	return s.committedRow(db, table)
}

func (s *seqBarrierStore) commit(o *seqBarrierOverlay) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name := range o.deletes {
		delete(s.ids, name)
		delete(s.rows, name)
	}
	for name, id := range o.idWrites {
		s.ids[name] = id
	}
	for name, row := range o.rowWrites {
		s.rows[name] = row
	}
}

// ---------------------------------------------------------------------
// Model lock service: shared catalog fences (the lock_meta.go statement
// plan fence) and exclusive DDL catalog locks, plus exclusive sequence row
// locks. All waits block on one condition variable; everything a txn held
// releases when its attempt finishes. Rank witness: a txn may only ever
// BLOCK on a row whose rank is strictly ahead of every row rank it holds;
// any descending wait is the canonical-order violation the design forbids.
// parkAfterHold arms a one-shot freeze granted to the NEXT acquirer of the
// named row, so the director can stage exact interleavings.
// ---------------------------------------------------------------------

type seqBarrierLocks struct {
	mu           sync.Mutex
	cond         *sync.Cond
	catShared    map[[2]string]map[int]bool
	catExclusive map[[2]string]int
	rows         map[[2]string]int
	heldRows     map[int][]int
	pauseOnce    map[[2]string]chan struct{}
	emit         func(format string, args ...any)
	rankViolate  func(format string, args ...any)
}

func newSeqBarrierLocks(emit func(string, ...any), rankViolate func(string, ...any)) *seqBarrierLocks {
	l := &seqBarrierLocks{
		catShared:    map[[2]string]map[int]bool{},
		catExclusive: map[[2]string]int{},
		rows:         map[[2]string]int{},
		heldRows:     map[int][]int{},
		pauseOnce:    map[[2]string]chan struct{}{},
		emit:         emit,
		rankViolate:  rankViolate,
	}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func (l *seqBarrierLocks) parkAfterHold(db, table string) func() {
	ch := make(chan struct{})
	l.mu.Lock()
	l.pauseOnce[[2]string{db, table}] = ch
	l.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

func (l *seqBarrierLocks) acquireCatalogShared(txn int, name [2]string) {
	l.mu.Lock()
	for {
		owner, held := l.catExclusive[name]
		if !held || owner == txn {
			break
		}
		l.emit("txn%d waits shared-catalog %s.%s (exclusive held by txn%d)", txn, name[0], name[1], owner)
		l.cond.Wait()
	}
	if l.catShared[name] == nil {
		l.catShared[name] = map[int]bool{}
	}
	l.catShared[name][txn] = true
	l.emit("txn%d holds shared-catalog %s.%s", txn, name[0], name[1])
	l.mu.Unlock()
}

func (l *seqBarrierLocks) acquireCatalogExclusive(txn int, name [2]string) {
	l.mu.Lock()
	for {
		sharedOther := false
		for holder := range l.catShared[name] {
			if holder != txn {
				sharedOther = true
				break
			}
		}
		owner, excl := l.catExclusive[name]
		if !sharedOther && (!excl || owner == txn) {
			break
		}
		l.emit("txn%d waits exclusive-catalog %s.%s", txn, name[0], name[1])
		l.cond.Wait()
	}
	l.catExclusive[name] = txn
	l.emit("txn%d holds exclusive-catalog %s.%s", txn, name[0], name[1])
	l.mu.Unlock()
}

// holdsRow reports whether the txn already holds the named row lock.
func (l *seqBarrierLocks) holdsRow(txn int, name [2]string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	owner, held := l.rows[name]
	return held && owner == txn
}

func (l *seqBarrierLocks) acquireRow(txn int, name [2]string, rank int) {
	l.mu.Lock()
	if owner, held := l.rows[name]; held && owner != txn {
		for _, heldRank := range l.heldRows[txn] {
			if heldRank >= rank {
				l.rankViolate(
					"txn%d blocks on row rank %d while holding rank %d: descending wait violates canonical order",
					txn, rank, heldRank)
				break
			}
		}
	}
	for {
		owner, held := l.rows[name]
		if !held || owner == txn {
			break
		}
		l.emit("txn%d waits row %s.%s (held by txn%d)", txn, name[0], name[1], owner)
		l.cond.Wait()
	}
	l.rows[name] = txn
	l.heldRows[txn] = append(l.heldRows[txn], rank)
	l.emit("txn%d holds row %s.%s (rank %d)", txn, name[0], name[1], rank)
	ch, parked := l.pauseOnce[name]
	if parked {
		delete(l.pauseOnce, name)
	}
	l.mu.Unlock()
	if parked {
		l.emit("txn%d parked holding row %s.%s (rank %d)", txn, name[0], name[1], rank)
		<-ch
		l.emit("txn%d resumed holding row %s.%s", txn, name[0], name[1])
	}
}

func (l *seqBarrierLocks) releaseAll(txn int) {
	l.mu.Lock()
	for name, owner := range l.catExclusive {
		if owner == txn {
			delete(l.catExclusive, name)
		}
	}
	for name, shared := range l.catShared {
		delete(shared, txn)
		if len(shared) == 0 {
			delete(l.catShared, name)
		}
	}
	for name, owner := range l.rows {
		if owner == txn {
			delete(l.rows, name)
		}
	}
	delete(l.heldRows, txn)
	l.emit("txn%d released all locks", txn)
	l.cond.Broadcast()
	l.mu.Unlock()
}

// ---------------------------------------------------------------------
// Model engine stubs (hand-written; only the surface the reconcile unit
// touches). Database/Relation probes resolve against committed-latest
// catalog state, the design's committed-resolution path.
// ---------------------------------------------------------------------

type seqBarrierEngine struct {
	engine.Engine
	store *seqBarrierStore
}

func (e *seqBarrierEngine) Database(_ context.Context, name string, _ client.TxnOperator) (engine.Database, error) {
	return &seqBarrierDatabase{store: e.store, db: name}, nil
}

type seqBarrierDatabase struct {
	engine.Database
	store *seqBarrierStore
	db    string
}

func (d *seqBarrierDatabase) Relation(_ context.Context, tbl string, _ any) (engine.Relation, error) {
	id, ok := d.store.committedID(d.db, tbl)
	if !ok {
		return nil, moerr.NewNoSuchTableNoCtx(d.db, tbl)
	}
	return &seqBarrierRelation{id: id}, nil
}

type seqBarrierRelation struct {
	engine.Relation
	id uint64
}

func (r *seqBarrierRelation) GetTableID(context.Context) uint64 { return r.id }

func (r *seqBarrierRelation) TableDefs(context.Context) ([]engine.TableDef, error) {
	return []engine.TableDef{
		&engine.PropertiesDef{Properties: []engine.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel},
		}},
	}, nil
}

// ---------------------------------------------------------------------
// The model SQL executor shared by every participant, like the production
// internal executor. Statement shapes are the exact SELECT FOR UPDATE and
// monotone conditional UPDATE the production reconcile/allocation paths
// issue, plus the barrier script's DDL competitor shapes (catalog fence,
// drop+recreate, ALTER-style restart). Every attempt gets a fresh txn id;
// its overlay commits atomically or rolls back completely.
// ---------------------------------------------------------------------

type seqBarrierExecutor struct {
	store  *seqBarrierStore
	locks  *seqBarrierLocks
	mp     *mpool.MPool
	trace  *seqBarrierTrace
	nextID seqBarrierAtomic
}

var _ executor.SQLExecutor = (*seqBarrierExecutor)(nil)

type seqBarrierAtomic struct {
	mu sync.Mutex
	v  int
}

func (a *seqBarrierAtomic) Add(d int) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.v += d
	return a.v
}

func (e *seqBarrierExecutor) ExecTxn(_ context.Context, fn func(executor.TxnExecutor) error, _ executor.Options) error {
	id := e.nextID.Add(1)
	te := &seqBarrierTxn{owner: e, id: id, overlay: newSeqBarrierOverlay()}
	e.trace.addf("txn%d attempt begins", id)
	err := fn(te)
	e.locks.releaseAll(id)
	if err != nil {
		e.trace.addf("txn%d attempt ROLLED BACK: %v", id, err)
		return err
	}
	e.store.commit(te.overlay)
	e.trace.addf("txn%d attempt COMMITTED", id)
	return nil
}

func (e *seqBarrierExecutor) Exec(_ context.Context, _ string, _ executor.Options) (executor.Result, error) {
	return executor.Result{}, moerr.NewInternalErrorNoCtx("seq barrier executor: in-caller route unexpected")
}

type seqBarrierTxn struct {
	owner   *seqBarrierExecutor
	id      int
	overlay *seqBarrierOverlay
}

func (te *seqBarrierTxn) Use(string) {}

func (te *seqBarrierTxn) Txn() client.TxnOperator { return nil }

func (te *seqBarrierTxn) LockTable(table string) error {
	return moerr.NewInternalErrorNoCtx("seq barrier executor: unexpected LockTable " + table)
}

// readRow applies RC visibility under the held row lock: the attempt's own
// staged write when present, else the committed-latest row.
func (te *seqBarrierTxn) readRow(name [2]string) (seqStoreRow, bool) {
	if te.overlay.deletes[name] {
		return seqStoreRow{}, false
	}
	if row, ok := te.overlay.rowWrites[name]; ok {
		return row, true
	}
	return te.owner.store.committedRow(name[0], name[1])
}

// resolveID returns the attempt-view relation id: a staged recreate first,
// then the committed-latest catalog.
func (te *seqBarrierTxn) resolveID(name [2]string) (uint64, bool) {
	if te.overlay.deletes[name] {
		return 0, false
	}
	if id, ok := te.overlay.idWrites[name]; ok {
		return id, true
	}
	return te.owner.store.committedID(name[0], name[1])
}

func (te *seqBarrierTxn) Exec(sql string, _ executor.StatementOption) (executor.Result, error) {
	if db, table, ok := parseSeqSelectSequenceRow(sql); ok {
		name := [2]string{db, table}
		// The statement's plan-time catalog fence precedes its row lock,
		// exactly like lock_meta.go's plan fence ordering.
		te.owner.locks.acquireCatalogShared(te.id, name)
		id, found := te.resolveID(name)
		if !found {
			return executor.Result{}, moerr.NewNoSuchTableNoCtx(db, table)
		}
		te.owner.locks.acquireRow(te.id, name, int(id))
		row, found := te.readRow(name)
		if !found {
			return executor.Result{}, moerr.NewNoSuchTableNoCtx(db, table)
		}
		te.owner.trace.addf("txn%d locked+read %s.%s (rank %d, last %d called %v)",
			te.id, db, table, id, int64(row.last), row.called)
		return seqSelectSequenceRowResult(te.owner.mp, row)
	}
	if db, table, floorLit, cmp, ok := parseSeqConditionalUpdate(sql); ok {
		name := [2]string{db, table}
		if !te.owner.locks.holdsRow(te.id, name) {
			return executor.Result{}, moerr.NewInternalErrorNoCtx(
				"seq barrier executor: conditional update without the row lock")
		}
		row, found := te.readRow(name)
		if !found {
			return executor.Result{}, moerr.NewNoSuchTableNoCtx(db, table)
		}
		one := seqRows{name: row}
		matched, err := applySeqConditionalUpdate(one, db, table, floorLit, cmp)
		if err != nil {
			return executor.Result{}, err
		}
		if matched {
			te.overlay.rowWrites[name] = one[name]
		}
		te.owner.trace.addf("txn%d conditional-update %s.%s matched=%v", te.id, db, table, matched)
		res := executor.Result{}
		if matched {
			res.AffectedRows = 1
		}
		return res, nil
	}
	// Competitor DDL shapes: an exclusive catalog fence, then either a
	// committed drop+recreate of the identity or an ALTER-style restart of
	// the row, both staged in the attempt overlay until commit.
	if db, table, ok := parseSeqBarrierDDLLock(sql); ok {
		te.owner.locks.acquireCatalogExclusive(te.id, [2]string{db, table})
		te.owner.trace.addf("txn%d ddl-locked %s.%s", te.id, db, table)
		return executor.Result{}, nil
	}
	if db, table, newID, row, ok := parseSeqBarrierDDLRecreate(sql); ok {
		name := [2]string{db, table}
		if err := te.requireExclusive(name); err != nil {
			return executor.Result{}, err
		}
		te.overlay.deletes[name] = true
		te.overlay.idWrites[name] = newID
		te.overlay.rowWrites[name] = row
		te.owner.trace.addf("txn%d recreated %s.%s as id %d", te.id, db, table, newID)
		return executor.Result{}, nil
	}
	if db, table, last, ok := parseSeqBarrierDDLRestart(sql); ok {
		name := [2]string{db, table}
		if err := te.requireExclusive(name); err != nil {
			return executor.Result{}, err
		}
		// ALTER's own statement path row-locks the sequence row too.
		id, _ := te.resolveID(name)
		te.owner.locks.acquireRow(te.id, name, int(id))
		row, found := te.readRow(name)
		if !found {
			return executor.Result{}, moerr.NewNoSuchTableNoCtx(db, table)
		}
		row.last = last
		row.called = false
		te.overlay.rowWrites[name] = row
		te.owner.trace.addf("txn%d restarted %s.%s uncalled at %d", te.id, db, table, int64(last))
		return executor.Result{}, nil
	}
	return executor.Result{}, moerr.NewInternalErrorNoCtx("seq barrier executor: unexpected SQL: " + sql)
}

func (te *seqBarrierTxn) requireExclusive(name [2]string) error {
	te.owner.locks.mu.Lock()
	defer te.owner.locks.mu.Unlock()
	owner, held := te.owner.locks.catExclusive[name]
	if !held || owner != te.id {
		return moerr.NewInternalErrorNoCtx(
			"seq barrier executor: ddl write without the exclusive catalog fence")
	}
	return nil
}

// parseSeqBarrierDDLLock recognizes the barrier script's DDL fence.
func parseSeqBarrierDDLLock(sql string) (string, string, bool) {
	fields := strings.Fields(sql)
	if len(fields) == 3 && fields[0] == "barrier-ddl-lock" {
		return fields[1], fields[2], true
	}
	return "", "", false
}

// parseSeqBarrierDDLRecreate recognizes the barrier script's drop+recreate.
func parseSeqBarrierDDLRecreate(sql string) (string, string, uint64, seqStoreRow, bool) {
	var db, table string
	var id, last, minv, maxv, start uint64
	var incr int64
	if n, _ := fmt.Sscanf(sql,
		"barrier-ddl-recreate %s %s id=%d last=%d min=%d max=%d start=%d incr=%d",
		&db, &table, &id, &last, &minv, &maxv, &start, &incr); n == 8 {
		return db, table, id,
			seqStoreRow{last: last, minv: minv, maxv: maxv, start: start, incr: incr}, true
	}
	return "", "", 0, seqStoreRow{}, false
}

// parseSeqBarrierDDLRestart recognizes the barrier script's ALTER-style
// restart: the new uncalled position rides in the statement, like START
// WITH / RESTART WITH does in the production DDL.
func parseSeqBarrierDDLRestart(sql string) (string, string, uint64, bool) {
	fields := strings.Fields(sql)
	if len(fields) != 4 || fields[0] != "barrier-ddl-restart" {
		return "", "", 0, false
	}
	var last uint64
	if n, _ := fmt.Sscanf(fields[3], "last=%d", &last); n != 1 {
		return "", "", 0, false
	}
	return fields[1], fields[2], last, true
}

// ---------------------------------------------------------------------
// Trace and director: a serialized event log plus gate/waitEvent helpers
// with a 30-second watchdog, so any hang fails with the full trace.
// ---------------------------------------------------------------------

type seqBarrierTrace struct {
	mu     sync.Mutex
	lines  []string
	events chan string
}

func newSeqBarrierTrace() *seqBarrierTrace {
	return &seqBarrierTrace{events: make(chan string, 4096)}
}

func (tr *seqBarrierTrace) addf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	tr.mu.Lock()
	tr.lines = append(tr.lines, line)
	tr.mu.Unlock()
	tr.events <- line
}

func (tr *seqBarrierTrace) dump() string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return strings.Join(tr.lines, "\n")
}

func (tr *seqBarrierTrace) allLines() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]string(nil), tr.lines...)
}

// count returns how many trace lines contain want.
func (tr *seqBarrierTrace) count(want string) int {
	n := 0
	for _, line := range tr.allLines() {
		if strings.Contains(line, want) {
			n++
		}
	}
	return n
}

// seqBarrierHook parks a unit at its production plan point until the
// director releases that exact arrival. A fresh release channel per arrival
// is mandatory: attempts after the first must block again, not fall through
// an already-closed channel. One instance per unit, identified by tag.
type seqBarrierHook struct {
	unit   string
	arrive chan<- seqBarrierArrival
}

type seqBarrierArrival struct {
	unit    string
	ids     []uint64
	release chan struct{}
}

func (h *seqBarrierHook) AfterPlan(ids []uint64) {
	release := make(chan struct{})
	h.arrive <- seqBarrierArrival{unit: h.unit, ids: append([]uint64(nil), ids...), release: release}
	<-release
}

// seqBarrierDirector scripts the interleaving deterministically: it parks
// units at plan gates, starts competitors, and fails on watchdog expiry.
type seqBarrierDirector struct {
	t      *testing.T
	trace  *seqBarrierTrace
	arrive chan seqBarrierArrival
	events chan string
}

func newSeqBarrierDirector(t *testing.T, trace *seqBarrierTrace) *seqBarrierDirector {
	return &seqBarrierDirector{
		t:      t,
		trace:  trace,
		arrive: make(chan seqBarrierArrival, 16),
		events: trace.events,
	}
}

// gate waits for the next plan arrival, asserts it is the named unit, and
// returns control of its release.
func (d *seqBarrierDirector) gate(unit string) seqBarrierArrival {
	d.t.Helper()
	select {
	case a := <-d.arrive:
		require.Equal(d.t, unit, a.unit, "plan gate order violation; trace:\n%s", d.trace.dump())
		return a
	case <-time.After(30 * time.Second):
		d.t.Fatalf("watchdog: plan gate for %s never arrived; trace:\n%s", unit, d.trace.dump())
		return seqBarrierArrival{}
	}
}

// release frees a parked unit's plan arrival.
func (d *seqBarrierDirector) release(a seqBarrierArrival) {
	close(a.release)
}

// waitEvent blocks until a trace line containing want appears (watchdog
// bounded); already-emitted lines are re-scanned first.
func (d *seqBarrierDirector) waitEvent(want string) {
	d.t.Helper()
	if d.trace.count(want) > 0 {
		return
	}
	for {
		select {
		case line := <-d.events:
			if strings.Contains(line, want) {
				return
			}
		case <-time.After(30 * time.Second):
			d.t.Fatalf("watchdog: event %q never fired; trace:\n%s", want, d.trace.dump())
		}
	}
}

// result collects a unit's reconcile error with the same watchdog.
func (d *seqBarrierDirector) result(name string, ch <-chan error) error {
	d.t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(30 * time.Second):
		d.t.Fatalf("watchdog: unit %s never finished; trace:\n%s", name, d.trace.dump())
		return nil
	}
}

// ---------------------------------------------------------------------
// Shared barrier fixture.
// ---------------------------------------------------------------------

type seqBarrierFixture struct {
	store     *seqBarrierStore
	locks     *seqBarrierLocks
	exec      *seqBarrierExecutor
	trace     *seqBarrierTrace
	dir       *seqBarrierDirector
	mp        *mpool.MPool
	bh        *seqReconcileBackgroundExec
	mu        sync.Mutex
	violating []string
}

func newSeqBarrierFixture(t *testing.T) *seqBarrierFixture {
	t.Helper()
	trace := newSeqBarrierTrace()
	f := &seqBarrierFixture{store: newSeqBarrierStore(), trace: trace}
	f.dir = newSeqBarrierDirector(t, trace)
	f.locks = newSeqBarrierLocks(
		func(format string, args ...any) { trace.addf(format, args...) },
		func(format string, args ...any) {
			msg := fmt.Sprintf(format, args...)
			trace.addf("RANK VIOLATION: %s", msg)
			f.mu.Lock()
			f.violating = append(f.violating, msg)
			f.mu.Unlock()
		})
	f.mp = mpool.MustNewZero()
	f.exec = &seqBarrierExecutor{store: f.store, locks: f.locks, trace: trace, mp: f.mp}
	f.bh = &seqReconcileBackgroundExec{op: newTestTxnOp()}
	f.bh.init()
	return f
}

func (f *seqBarrierFixture) assertNoViolations(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Empty(t, f.violating, "rank witness violations; trace:\n%s", f.trace.dump())
}

// sessionFor hands one participant unit a session wired to the barrier
// engine, exactly the way the production merge-driver envelope provides the
// engine: through the txn handler only (no defines.EngineKey on proc.Ctx).
func (f *seqBarrierFixture) sessionFor(t *testing.T) *Session {
	t.Helper()
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	t.Cleanup(ses.Close)
	eng := &seqBarrierEngine{store: f.store}
	ses.proc = testutil.NewProc(t)
	op := newTestTxnOp()
	ses.txnHandler = InitTxnHandler("", eng, context.Background(), op)
	ses.proc.Base.TxnOperator = op
	installSeqTestExecutor(t, ses, f.exec)
	return ses
}

// startUnit launches one participant's reconcile in its own goroutine,
// parked at the production plan hook under the unit's tag.
func (f *seqBarrierFixture) startUnit(ses *Session, unit string, wms ...*sequenceWatermark) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		tracker := newWatermarkTracker(wms[0].db, wms...)
		hook := &seqBarrierHook{unit: unit, arrive: f.dir.arrive}
		ctx := withSequenceReconcilePlanHook(context.Background(), hook)
		errCh <- reconcileMergeSequenceWatermarks(ctx, ses, f.bh, tracker)
	}()
	return errCh
}

// heldRanks extracts the rank sequence of one txn's row acquisitions from
// the trace, in acquisition order.
func heldRanks(trace *seqBarrierTrace, txn int) []int {
	prefix := fmt.Sprintf("txn%d holds row ", txn)
	var ranks []int
	for _, line := range trace.allLines() {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		var name string
		var rank int
		if n, _ := fmt.Sscanf(line, prefix+"%s (rank %d)", &name, &rank); n == 2 {
			ranks = append(ranks, rank)
		}
	}
	return ranks
}

func requireStrictlyAscending(t *testing.T, trace *seqBarrierTrace, txn int) {
	t.Helper()
	ranks := heldRanks(trace, txn)
	require.NotEmpty(t, ranks, "txn%d acquired no rows; trace:\n%s", txn, trace.dump())
	for i := 1; i < len(ranks); i++ {
		require.Greater(t, ranks[i], ranks[i-1],
			"txn%d acquired ranks %v, not strictly ascending; trace:\n%s", txn, ranks, trace.dump())
	}
}

// ---------------------------------------------------------------------
// Oracles.
// ---------------------------------------------------------------------

// seqBarrierWm builds a single-value watermark through the production
// capture classification, so barrier units exercise the stored shape the
// merge driver would have produced (#30359 pre-PR review N3).
func seqBarrierWm(db, table string, v uint64, rows int64) *sequenceWatermark {
	wm := &sequenceWatermark{db: db, table: table, rowsCount: rows}
	wm.accumulate(sequenceWatermarkUintValue(v))
	return wm
}

// L2: two multi-identity units tracking the same identity set in reversed
// capture orders both complete without deadlock, each acquires rows in
// canonical ascending order, and both wait directions stay ascending under
// the rank witness. Terminal committed rows equal the monotone max over the
// initial row and every admitted watermark, regardless of interleave.
func TestSeqReconcileBarrierReversedPairsBothComplete(t *testing.T) {
	const maxv = 1 << 30
	f := newSeqBarrierFixture(t)
	f.store.add("d1", "s1", 101, seqStoreRow{last: 100, minv: 1, maxv: maxv, start: 1, incr: 1, called: true})
	f.store.add("d1", "s2", 102, seqStoreRow{last: 100, minv: 1, maxv: maxv, start: 1, incr: 1, called: true})

	sesA := f.sessionFor(t)
	sesB := f.sessionFor(t)

	// Unit A captured s2 then s1; unit B captured s1 then s2. The tracker
	// keys identities into a map either way: the only order the protocol
	// may ever rely on is the plan's canonical sort.
	wmA2 := seqBarrierWm("d1", "s2", 120, 1)
	wmA1 := seqBarrierWm("d1", "s1", 150, 1)
	wmB1 := seqBarrierWm("d1", "s1", 140, 1)
	wmB2 := seqBarrierWm("d1", "s2", 160, 1)

	errA := f.startUnit(sesA, "A", wmA2, wmA1)
	gateA := f.dir.gate("A")
	require.Equal(t, []uint64{101, 102}, gateA.ids, "unit A must plan in canonical order")
	errB := f.startUnit(sesB, "B", wmB1, wmB2)
	gateB := f.dir.gate("B")
	require.Equal(t, []uint64{101, 102}, gateB.ids, "unit B must plan in canonical order")

	// Reverse the release order too, then stage real contention: B parks
	// holding the s1 row while A blocks on it.
	unpark := f.locks.parkAfterHold("d1", "s1")
	f.dir.release(gateB)
	f.dir.waitEvent("txn2 holds row d1.s1 (rank 101)")
	f.dir.release(gateA)
	f.dir.waitEvent("txn1 waits row d1.s1 (held by txn2)")
	unpark()
	f.dir.waitEvent("txn2 attempt COMMITTED")

	require.NoError(t, f.dir.result("B", errB))
	require.NoError(t, f.dir.result("A", errA))
	f.assertNoViolations(t)
	requireStrictlyAscending(t, f.trace, 1)
	requireStrictlyAscending(t, f.trace, 2)

	// Monotone terminal state: each row is the max of its start row and
	// every admitted watermark, independent of which unit wrote it.
	row, ok := f.store.get("d1", "s1")
	require.True(t, ok)
	require.Equal(t, uint64(150), row.last)
	require.True(t, row.called)
	row, ok = f.store.get("d1", "s2")
	require.True(t, ok)
	require.Equal(t, uint64(160), row.last)
	require.True(t, row.called)
}

// K2 (ALTER competitor): a multi-identity unit blocks on the ALTER's
// exclusive catalog fence without deadlock; after the ALTER commits its
// restart, the unit reads the committed-latest row and advances it past
// both the restart position and its own watermark.
func TestSeqReconcileBarrierAlterCompetitorAtBarrier(t *testing.T) {
	const maxv = 1 << 30
	f := newSeqBarrierFixture(t)
	f.store.add("d1", "s1", 101, seqStoreRow{last: 100, minv: 1, maxv: maxv, start: 1, incr: 1, called: true})
	f.store.add("d1", "s2", 102, seqStoreRow{last: 50, minv: 1, maxv: maxv, start: 1, incr: 1, called: true})

	sesA := f.sessionFor(t)
	wmA1 := seqBarrierWm("d1", "s1", 150, 1)
	wmA2 := seqBarrierWm("d1", "s2", 120, 1)
	errA := f.startUnit(sesA, "A", wmA1, wmA2)
	gateA := f.dir.gate("A")
	require.Equal(t, []uint64{101, 102}, gateA.ids)

	// ALTER SEQUENCE s2 competitor: exclusive catalog fence, parked before
	// the restart write until the director has A blocked on the fence.
	alterGo := make(chan struct{})
	alterErr := make(chan error, 1)
	go func() {
		alterErr <- f.exec.ExecTxn(context.Background(), func(te executor.TxnExecutor) error {
			if _, err := te.Exec("barrier-ddl-lock d1 s2", executor.StatementOption{}); err != nil {
				return err
			}
			<-alterGo
			// Restart s2 at 100 (uncalled): the final state must end at the
			// watermark 120, proving reconcile advanced past the ALTER.
			if _, err := te.Exec("barrier-ddl-restart d1 s2 last=100", executor.StatementOption{}); err != nil {
				return err
			}
			return nil
		}, executor.Options{})
	}()
	f.dir.waitEvent("txn2 holds exclusive-catalog d1.s2")

	f.dir.release(gateA)
	// A advances s1, then its s2 catalog fence blocks behind the ALTER's.
	f.dir.waitEvent("txn1 holds row d1.s1 (rank 101)")
	f.dir.waitEvent("txn1 waits shared-catalog d1.s2")
	close(alterGo)
	f.dir.waitEvent("txn2 restarted d1.s2 uncalled at 100")
	f.dir.waitEvent("txn2 attempt COMMITTED")

	require.NoError(t, <-alterErr)
	require.NoError(t, f.dir.result("A", errA))
	f.assertNoViolations(t)
	requireStrictlyAscending(t, f.trace, 1)

	row, ok := f.store.get("d1", "s1")
	require.True(t, ok)
	require.Equal(t, uint64(150), row.last)
	require.True(t, row.called)
	// Monotone max(ALTER restart 100, watermark 120) with the row issued:
	// the reconcile read the committed-latest restarted row and advanced.
	row, ok = f.store.get("d1", "s2")
	require.True(t, ok)
	require.Equal(t, seqStoreRow{last: 120, minv: 1, maxv: maxv, start: 1, incr: 1, called: true}, row)
	// And the unit really consumed the post-ALTER row, not a stale plan.
	require.Greater(t, f.trace.count("txn1 waits shared-catalog d1.s2"), 0,
		"the unit must have waited on the ALTER's catalog fence; trace:\n%s", f.trace.dump())
}

// K2 (drop+recreate between plan and acquire): the validation probe
// observes the committed identity drift, aborts the attempt deterministically
// (lock-table-bind-changed), and the bounded wrapper replans from fresh
// identities and completes against the recreated generation.
func TestSeqReconcileBarrierRecreateBetweenPlanAndAcquire(t *testing.T) {
	const maxv = 1 << 30
	f := newSeqBarrierFixture(t)
	f.store.add("d1", "s1", 101, seqStoreRow{last: 100, minv: 1, maxv: maxv, start: 1, incr: 1, called: true})
	f.store.add("d1", "s2", 102, seqStoreRow{last: 100, minv: 1, maxv: maxv, start: 1, incr: 1, called: true})

	sesA := f.sessionFor(t)
	wmA1 := seqBarrierWm("d1", "s1", 150, 1)
	wmA2 := seqBarrierWm("d1", "s2", 120, 1)
	errA := f.startUnit(sesA, "A", wmA1, wmA2)
	gateA := f.dir.gate("A")
	require.Equal(t, []uint64{101, 102}, gateA.ids)

	// Competitor drops and recreates s1 as relation id 201 while A is
	// parked at its plan point; the change commits before A validates.
	recreateErr := make(chan error, 1)
	go func() {
		recreateErr <- f.exec.ExecTxn(context.Background(), func(te executor.TxnExecutor) error {
			if _, err := te.Exec("barrier-ddl-lock d1 s1", executor.StatementOption{}); err != nil {
				return err
			}
			_, err := te.Exec(
				"barrier-ddl-recreate d1 s1 id=201 last=100 min=1 max=1000 start=1 incr=1",
				executor.StatementOption{})
			return err
		}, executor.Options{})
	}()
	f.dir.waitEvent("txn2 attempt COMMITTED")
	require.NoError(t, <-recreateErr)

	// A's first attempt must abort at validation; the wrapper replans.
	f.dir.release(gateA)
	f.dir.waitEvent("txn1 attempt ROLLED BACK")
	gateA2 := f.dir.gate("A")
	require.Equal(t, []uint64{102, 201}, gateA2.ids,
		"the replanned attempt must bind the recreated identities in canonical order")
	f.dir.release(gateA2)

	require.NoError(t, f.dir.result("A", errA))
	f.assertNoViolations(t)
	// Attempt 1 made no writes; attempt 2 acquired strictly ascending.
	requireStrictlyAscending(t, f.trace, 3)

	// The recreated generation advanced, the untouched one too, both by the
	// monotone predicate against the replanned identities.
	id, ok := f.store.committedID("d1", "s1")
	require.True(t, ok)
	require.Equal(t, uint64(201), id)
	row, ok := f.store.get("d1", "s1")
	require.True(t, ok)
	require.Equal(t, uint64(150), row.last)
	require.True(t, row.called)
	row, ok = f.store.get("d1", "s2")
	require.True(t, ok)
	require.Equal(t, uint64(120), row.last)
	require.True(t, row.called)
}

// §4 D same-path oracle: a single-identity allocation-equivalent unit (the
// same production reconcile/lock pipeline with a one-element plan)
// competing with a two-identity unit. Contention is staged exactly: the
// single-identity unit parks holding row s2 while the multi-identity unit,
// already holding row s1, blocks on it — the protocol's canonical ascending
// wait. Both complete; the terminal row is the monotone max over both
// watermarks.
func TestSeqReconcileBarrierSingleIdentityAllocatorWithMultiIdentity(t *testing.T) {
	const maxv = 1 << 30
	f := newSeqBarrierFixture(t)
	f.store.add("d1", "s1", 101, seqStoreRow{last: 100, minv: 1, maxv: maxv, start: 1, incr: 1, called: true})
	f.store.add("d1", "s2", 102, seqStoreRow{last: 100, minv: 1, maxv: maxv, start: 1, incr: 1, called: true})

	sesA := f.sessionFor(t)
	sesB := f.sessionFor(t)
	wmA1 := seqBarrierWm("d1", "s1", 150, 1)
	wmA2 := seqBarrierWm("d1", "s2", 120, 1)
	wmB2 := seqBarrierWm("d1", "s2", 110, 1)

	errA := f.startUnit(sesA, "A", wmA1, wmA2)
	gateA := f.dir.gate("A")
	require.Equal(t, []uint64{101, 102}, gateA.ids)
	errB := f.startUnit(sesB, "B", wmB2)
	gateB := f.dir.gate("B")
	require.Equal(t, []uint64{102}, gateB.ids)

	// B takes row s2 first and parks; A advances s1, then blocks on s2
	// while holding only the lower-ranked s1 row: the allowed wait shape.
	unpark := f.locks.parkAfterHold("d1", "s2")
	f.dir.release(gateB)
	f.dir.waitEvent("txn2 holds row d1.s2 (rank 102)")
	f.dir.release(gateA)
	f.dir.waitEvent("txn1 holds row d1.s1 (rank 101)")
	f.dir.waitEvent("txn1 waits row d1.s2 (held by txn2)")
	unpark()
	f.dir.waitEvent("txn2 attempt COMMITTED")

	require.NoError(t, f.dir.result("B", errB))
	require.NoError(t, f.dir.result("A", errA))
	f.assertNoViolations(t)
	requireStrictlyAscending(t, f.trace, 1)

	row, ok := f.store.get("d1", "s1")
	require.True(t, ok)
	require.Equal(t, uint64(150), row.last)
	require.True(t, row.called)
	// Monotone max(allocator 110, merge 120): the later acquirer read the
	// committed-latest 110 and advanced to 120, never regressing.
	row, ok = f.store.get("d1", "s2")
	require.True(t, ok)
	require.Equal(t, uint64(120), row.last)
	require.True(t, row.called)
}
