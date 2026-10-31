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

package compile

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Tier-2 Compile.Run oracles against the REAL allocator and the REAL prober
// (design §4 D, v21 A1/A2/A3/B1/B2/B3/Z1/Z2; the §9 retry-boundary list's
// controlled-proof entries): every statement below runs
// `select nextval('testdb.<seq>')` through NewCompile -> Compile.Run, so the
// tier-1 attempt loop, the conversion pre-filter, the quiescence-boundary
// arbitration with its admission sampling, the probe-first expiry decisions
// and the episode lifecycle are all exercised with no stage mocked out. The
// only fakes are the leaf resources a UT cannot have: the catalogue engine
// (stub), the client txn pair (gomock, frozen RC autocommit envelope) and
// the internal SQL executor whose ExecTxn is the single boundary every
// sequence read/write/probe passes through. Timing is compressed by the
// exported test hook (window/renewal interval 300 ms, probe round 1 s,
// backoff step 1 ms) so episodes elapse in milliseconds on the wall clock.
//
// Reachability note for the conjunctive arm (B3): a single statement
// evaluates its allocations in projection order and aborts on the first
// failure. Retaining {A,B} live across rebuilds therefore needs
// projection-order churn: the EARLIER-projected identity must succeed
// while the LATER one opens the episode, then the earlier one contends
// under the still-live window — its failure aborts the projection before
// the later identity can resolve, so both watch entries stay recorded
// across the rebuild (C1–C3 below). Rotation (the first contended
// identity resolving before the next contends) instead yields DISTINCT
// episodes whose resident sets never overlap (T7): the resolved
// identity's removal retires its window (Z1) and the next contention
// re-admits fresh with its own baseline and deadline.

const (
	crSeqA        = "testdb.seqa"
	crSeqB        = "testdb.seqb"
	crRelA uint64 = 7001
	crRelB uint64 = 7002

	crWindow = 300 * time.Millisecond

	// Compile.Run numbers generations 0-based (the first attempt is 0, the
	// k-th retry is k); crGenAny scripts every generation.
	crGenAny uint64 = ^uint64(0)
)

var crWatchA = process.WatchIdentity{DBName: "testdb", TblName: "seqa", WantRel: crRelA}
var crWatchB = process.WatchIdentity{DBName: "testdb", TblName: "seqb", WantRel: crRelB}

// crStore serves the sequence-inner SQL issued against the fake executor:
// the 7-column metadata SELECT (probe round and allocation FOR UPDATE) and
// the counter UPDATE, keyed by the `\`db\`.\`tbl\` target in the text.
type crStore struct {
	mu   sync.Mutex
	last map[string]int64
	call map[string]bool
}

func newCrStore() *crStore {
	return &crStore{
		last: map[string]int64{crSeqA: 1, crSeqB: 1},
		call: map[string]bool{crSeqA: true, crSeqB: true},
	}
}

func (s *crStore) advance(name string, last int64) {
	s.mu.Lock()
	s.last[name], s.call[name] = last, true
	s.mu.Unlock()
}

func (s *crStore) state(name string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last[name], s.call[name]
}

var crSeqColumnTypes = []types.Type{
	types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
	types.T_int64.ToType(), types.T_int64.ToType(), types.T_bool.ToType(),
	types.T_bool.ToType(),
}

// crTarget extracts the `\`db\`.\`tbl\` sequence target from the statement
// text (both the SELECT and the UPDATE carry it fully qualified).
func crTarget(sql string) (string, error) {
	start := strings.Index(sql, "`")
	if start < 0 {
		return "", fmt.Errorf("crTarget: no quoted target in %q", sql)
	}
	rest := sql[start+1:]
	mid := strings.Index(rest, "`.`")
	if mid < 0 {
		return "", fmt.Errorf("crTarget: malformed target in %q", sql)
	}
	db := rest[:mid]
	after := rest[mid+3:]
	end := strings.Index(after, "`")
	if end < 0 {
		return "", fmt.Errorf("crTarget: malformed target in %q", sql)
	}
	return db + "." + after[:end], nil
}

func (s *crStore) execSQL(mp *mpool.MPool, sql string) (executor.Result, error) {
	normalized := strings.ToLower(strings.TrimSpace(sql))
	if strings.HasPrefix(normalized, "select last_seq_num") {
		target, err := crTarget(normalized)
		if err != nil {
			return executor.Result{}, err
		}
		last, called := s.state(target)
		mem := executor.NewMemResult(crSeqColumnTypes, mp)
		mem.NewBatchWithRowCount(1)
		for col, values := range []any{
			[]int64{last}, []int64{1}, []int64{1 << 40}, []int64{1}, []int64{1},
			[]bool{false}, []bool{called},
		} {
			switch v := values.(type) {
			case []int64:
				if err := executor.AppendFixedRows(mem, col, v); err != nil {
					return executor.Result{}, err
				}
			case []bool:
				if err := executor.AppendFixedRows(mem, col, v); err != nil {
					return executor.Result{}, err
				}
			}
		}
		return mem.GetResult(), nil
	}
	if strings.HasPrefix(normalized, "update ") {
		target, err := crTarget(normalized)
		if err != nil {
			return executor.Result{}, err
		}
		setIdx := strings.Index(normalized, " set ")
		if setIdx < 0 {
			return executor.Result{}, fmt.Errorf("unexpected sequence SQL: %s", sql)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, piece := range strings.Split(normalized[setIdx+len(" set "):], ",") {
			kv := strings.SplitN(strings.TrimSpace(piece), "=", 2)
			if len(kv) != 2 {
				return executor.Result{}, fmt.Errorf("unexpected sequence assignment %q: %s", piece, sql)
			}
			switch key, raw := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1]); key {
			case "last_seq_num":
				v, err := strconv.ParseInt(raw, 10, 64)
				if err != nil {
					return executor.Result{}, err
				}
				s.last[target] = v
			case "is_called":
				s.call[target] = raw == "true"
			default:
				return executor.Result{}, fmt.Errorf("unexpected sequence assignment %q: %s", piece, sql)
			}
		}
		return executor.Result{}, nil
	}
	return executor.Result{}, fmt.Errorf("unexpected sequence SQL: %s", sql)
}

// crExec is the scriptable internal SQL executor. Every transaction the
// expiry machinery opens (tuple probes, lock checks, baseline samples)
// arrives on the probe lane (detected by the production probe marker);
// every tier-1 attempt arrives unmarked on the allocation lane. failGens
// keys "db.seq" to the generations whose FOR UPDATE read must keep aborting
// with an honest lock-wait shape (crGenAny is the wildcard; real
// generations start at 0); sleepGens
// slows every attempt of a generation so the tier-1 loop crosses the sticky
// window in flight; blockOnceGen arms ONE in-flight block on the attempt's
// own derived context (the A2 route).
type crExec struct {
	store *crStore
	mp    *mpool.MPool
	txn   *mock_frontend.MockTxnOperator

	mu           sync.Mutex
	gen          uint64
	failGens     map[string]map[uint64]bool
	sleepGens    map[uint64]time.Duration
	blockOnceGen uint64
	blockArmed   bool
	onBlock      func()
	probeHeld    map[string]bool
	onProbeTuple func()
	allocTries   int
	onAllocTry   func(tries int)
	plainProbes  int
	lockProbes   int
	lockResults  []bool
}

func newCrExec(t *testing.T, store *crStore, mp *mpool.MPool) *crExec {
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	op.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	return &crExec{
		store:     store,
		mp:        mp,
		txn:       op,
		failGens:  map[string]map[uint64]bool{},
		sleepGens: map[uint64]time.Duration{},
		probeHeld: map[string]bool{},
	}
}

// failAlloc scripts total allocation contention for the given sequence on
// the listed generations (0 = first attempt, k = k-th retry); no
// generations means every generation.
func (e *crExec) failAlloc(seq string, gens ...uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.failGens[seq] == nil {
		e.failGens[seq] = map[uint64]bool{}
	}
	if len(gens) == 0 {
		e.failGens[seq][crGenAny] = true
		return
	}
	for _, g := range gens {
		e.failGens[seq][g] = true
	}
}

// resolveAlloc lifts every scripted contention for the sequence; the next
// generation's tier-1 attempt succeeds. beforeGen hooks use it to end
// contention once a decision-relative condition (a renewal) is observed.
func (e *crExec) resolveAlloc(seq string) {
	e.mu.Lock()
	delete(e.failGens, seq)
	e.mu.Unlock()
}

func (e *crExec) setGen(gen uint64) {
	e.mu.Lock()
	e.gen = gen
	e.mu.Unlock()
}

func (e *crExec) counts() (plain, locks, tries int, lockResults []bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.plainProbes, e.lockProbes, e.allocTries,
		append([]bool(nil), e.lockResults...)
}

func (e *crExec) failsNow(sql string) bool {
	target, err := crTarget(strings.ToLower(sql))
	if err != nil {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	gens := e.failGens[target]
	return gens[crGenAny] || gens[e.gen]
}

func (e *crExec) sleepNow() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sleepGens[e.gen]
}

func (e *crExec) Exec(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
	return e.store.execSQL(e.mp, sql)
}

func (e *crExec) ExecTxn(ctx context.Context, fn func(executor.TxnExecutor) error, _ executor.Options) error {
	if function.IsSequenceProbeCtx(ctx) {
		// Probe lane: tuple probe (plain select) or availability check
		// (FOR UPDATE). The per-sequence held flag fails only the FOR
		// UPDATE read; the cancelled caller hook fires at the tuple probe.
		te := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
			if strings.Contains(strings.ToLower(sql), "for update") {
				target, _ := crTarget(strings.ToLower(sql))
				e.mu.Lock()
				held := e.probeHeld[target]
				e.lockProbes++
				e.lockResults = append(e.lockResults, !held)
				e.mu.Unlock()
				if held {
					return executor.Result{}, moerr.NewLockWaitTimeoutNoCtx()
				}
				return e.store.execSQL(e.mp, sql)
			}
			e.mu.Lock()
			e.plainProbes++
			onTuple := e.onProbeTuple
			e.mu.Unlock()
			if onTuple != nil {
				onTuple()
			}
			return e.store.execSQL(e.mp, sql)
		}, e.txn)
		return fn(te)
	}
	// Allocation lane: count the attempt, optionally block this attempt on
	// its derived context (in-flight budget expiry, A2), slow it per the
	// per-generation script, then abort on the FOR UPDATE read while the
	// generation's contention is scripted on.
	e.mu.Lock()
	e.allocTries++
	tries := e.allocTries
	onTry := e.onAllocTry
	block := e.blockArmed && e.blockOnceGen == e.gen
	if block {
		e.blockArmed = false
	}
	onBlock := e.onBlock
	e.mu.Unlock()
	if onTry != nil {
		onTry(tries)
	}
	if block {
		if onBlock != nil {
			onBlock()
		}
		<-ctx.Done()
		return ctx.Err()
	}
	// The scripted per-generation sleep parks the FULL duration even when
	// the attempt's derived context expires mid-park: choreography that
	// must straddle the window-derived attempt deadline (see
	// TestCompileRunSequenceRetryTerminalNamesRecordedIdentity) is clipped
	// to exactly that deadline by a ctx.Done select, re-creating the
	// wall-clock boundary the sleep exists to escape. Script users park at
	// or below the window scale only.
	if d := e.sleepNow(); d > 0 {
		time.Sleep(d)
	}
	te := executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		if strings.Contains(strings.ToLower(sql), "for update") && e.failsNow(sql) {
			return executor.Result{}, moerr.NewLockWaitTimeoutNoCtx()
		}
		return e.store.execSQL(e.mp, sql)
	}, e.txn)
	return fn(te)
}

// crSink records the retry episode as Compile.Run sees it: one BeginAttempt
// per generation (with the deadline the state carries into it), and every
// abort cause. beforeGen injects per-generation choreography (sleeps past
// the window, cancellation arming) BEFORE the generation's pipeline starts
// and stamps the executor's generation so the contention script tracks
// Compile.Run's rebuilds.
type crSink struct {
	mu        sync.Mutex
	exec      *crExec
	beforeGen map[uint64]func()
	// writeHook, when armed, runs after each recorded non-nil Write. The
	// retained-output discard test (N1) models the frontend cursor's
	// retained row set with it.
	writeHook func()
	begins    int
	writes    int
	deadlines []time.Time
	aborts    []string
}

func (s *crSink) BeginAttempt(_ context.Context, generation uint64, proc *process.Process) error {
	s.mu.Lock()
	s.begins++
	if st := proc.GetSequenceRetryState(); st != nil {
		if d, ok := st.RetryDeadline(); ok {
			s.deadlines = append(s.deadlines, d)
		}
	}
	hook := s.beforeGen[generation]
	s.mu.Unlock()
	s.exec.setGen(generation)
	if hook != nil {
		hook()
	}
	return nil
}

func (s *crSink) Write(_ uint64, bat *batch.Batch, _ *perfcounter.CounterSet) error {
	if bat == nil {
		return nil
	}
	s.mu.Lock()
	s.writes++
	hook := s.writeHook
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (s *crSink) SealAttempt(uint64) error { return nil }

func (s *crSink) AbortAttempt(generation uint64, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aborts = append(s.aborts, fmt.Sprintf("%d:%v", generation, cause))
	return nil
}

// crFixture is one Compile.Run statement: the process on a frozen RC
// autocommit envelope, the stub engine carrying testdb.seqa/seqb, the
// scriptable executor installed as the statement's internal SQL executor,
// and the compressed tier-2 timing schedule.
type crFixture struct {
	proc    *process.Process
	compile *Compile
	sink    *crSink
	exec    *crExec
	store   *crStore
	state   *process.SequenceRetryState
	cancel  context.CancelFunc
}

func newCrFixture(t *testing.T, sql string) *crFixture {
	t.Helper()
	return newCrFixtureWindow(t, sql, crWindow)
}

// newCrFixtureWindow is newCrFixture with a caller-chosen retry window. The
// default 300 ms window fits a single 8-attempt tier-1 cycle only with slim
// margin (one cycle costs ~90-330 ms on this harness), so a join or a
// decision whose conversion must land strictly inside the first window gets
// a doubled window where the choreography notes say so (C1-C3). The probe
// budget is a wall-clock ceiling on each catalog probe round (admission
// baseline sample, decision tuple probes), not choreography: it must stay
// well above the descheduling jitter of a loaded shared host, or a starved
// sample leaves an entry unbaselined and the first decision legitimately
// rides (Z2), adding a probe the exact-count oracles do not expect.
func newCrFixtureWindow(t *testing.T, sql string, window time.Duration) *crFixture {
	t.Helper()
	restore := function.SetSequenceAutonomousTimingForTesting(
		window, time.Second, time.Millisecond)
	f := &crFixture{store: newCrStore()}

	ctx, cancel := context.WithCancel(defines.AttachAccountId(context.Background(), catalog.System_Account))
	f.cancel = cancel
	proc := testutil.NewProcess(t)
	f.proc = proc
	proc.GetSessionInfo().Buf = buffer.New()
	proc.InitSeq()
	// Sequence names resolve in the session's current database (the
	// documented one-argument nextval surface): pin it like `USE testdb`.
	proc.Base.SessionInfo.Database = "testdb"
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		switch name {
		case plan2.SQLSelectLimitVariable:
			return ^uint64(0), nil
		case "lower_case_table_names":
			return int64(1), nil
		}
		return "STRICT_TRANS_TABLES", nil
	})

	compilerCtx := plan2.NewEmptyCompilerContext(newPlanTestProcess(t))
	compilerCtx.SetContext(ctx)
	stmts, err := mysql.Parse(ctx, sql, 1)
	require.NoError(t, err)
	query, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], false)
	require.NoError(t, err)
	pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}

	eng := newStubEngine()
	db := newStubDatabase("testdb")
	db.rels["seqa"] = &stubRelation{name: "seqa", tableID: crRelA, defs: crSequenceRelDef()}
	db.rels["seqb"] = &stubRelation{name: "seqb", tableID: crRelB, defs: crSequenceRelDef()}
	eng.dbs["testdb"] = db

	txnCli, txnOp := newTier2AutocommitRCTxn(t)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp
	proc.Ctx = context.WithValue(ctx, defines.EngineKey{}, eng)
	proc.ReplaceTopCtx(proc.Ctx)

	f.exec = newCrExec(t, f.store, proc.Mp())
	rt := runtime.ServiceRuntime(proc.GetService())
	old, hadOld := rt.GetGlobalVariables(runtime.InternalSQLExecutor)
	rt.SetGlobalVariables(runtime.InternalSQLExecutor, f.exec)

	c := NewCompile("test", "test", sql, "", "", eng, proc, stmts[0], false, nil, time.Now())
	f.compile = c
	f.state = proc.GetSequenceRetryState()
	require.NotNil(t, f.state, "NewCompile installs the per-statement retry state")
	require.NoError(t, c.Compile(ctx, pn, func(*batch.Batch, *perfcounter.CounterSet) error { return nil }))
	proc.Session = &remoteWarningCollector{}
	f.sink = &crSink{exec: f.exec, beforeGen: map[uint64]func(){}}
	c.SetResultSink(f.sink)
	t.Cleanup(func() {
		if hadOld {
			rt.SetGlobalVariables(runtime.InternalSQLExecutor, old)
		}
		restore()
		c.Release()
		proc.GetSessionInfo().Buf.Free()
	})
	return f
}

func crSequenceRelDef() []engine.TableDef {
	return []engine.TableDef{
		&engine.PropertiesDef{Properties: []engine.Property{
			{Key: catalog.SystemRelAttr_Kind, Value: catalog.SystemSequenceRel},
		}},
	}
}

func (f *crFixture) run() error {
	_, runErr := f.compile.Run(0)
	return runErr
}

func (f *crFixture) distinctDeadlines() []time.Time {
	f.sink.mu.Lock()
	defer f.sink.mu.Unlock()
	out := make([]time.Time, 0, len(f.sink.deadlines))
	for _, d := range f.sink.deadlines {
		if len(out) == 0 || !out[len(out)-1].Equal(d) {
			out = append(out, d)
		}
	}
	return out
}

func (f *crFixture) namedTerminalAborts() int {
	f.sink.mu.Lock()
	defer f.sink.mu.Unlock()
	n := 0
	for _, a := range f.sink.aborts {
		if strings.Contains(a, "within 8 attempts") {
			n++
		}
	}
	return n
}

// T1 — progress renewal through a conversion-site expiry decision. Gen 0
// (the first attempt) exhausts under scripted contention and admits the
// episode; gen 1 (the first retry) keeps failing while the peer herd
// commits row state forward (attempt hook), so its decision at the
// elapsed window renews instead of terminating; gen 2 drains: the
// statement SUCCEEDS from the herd-advanced committed state, the watch
// set empties (Z1 closure) and no availability check is ever needed.
func TestCompileRunSequenceRetryProgressRenewalSucceeds(t *testing.T) {
	f := newCrFixture(t, "select nextval('seqa')")
	f.exec.failAlloc(crSeqA, 0, 1)
	f.exec.sleepGens[1] = 40 * time.Millisecond
	advanced := false
	f.exec.onAllocTry = func(tries int) {
		if tries >= 10 && !advanced {
			advanced = true
			f.store.advance(crSeqA, 5)
		}
	}

	runErr := f.run()

	require.NoError(t, runErr, "progress renewal must let the drained statement succeed")
	require.Equal(t, "6", f.proc.GetSessionInfo().SeqAddValues[crRelA],
		"the final allocation continues from the herd-advanced committed state")
	last, _ := f.store.state(crSeqA)
	require.Equal(t, int64(6), last)
	plain, locks, _, _ := f.exec.counts()
	require.Equal(t, 2, plain, "one admission sample plus one decision tuple probe")
	require.Equal(t, 0, locks, "progress renews without an availability check")
	deadlines := f.distinctDeadlines()
	require.GreaterOrEqual(t, len(deadlines), 2, "renewal extends the window across the rebuild")
	require.True(t, deadlines[1].After(deadlines[0]), "renewed window is later than the original")
	require.False(t, f.state.HasWatch(crWatchA), "success removes the watch entry")
	if _, ok := f.state.RetryDeadline(); ok {
		t.Fatal("the episode closes when the last recorded contention resolves")
	}
	if _, terminal := f.state.TerminalIdentity(); terminal {
		t.Fatal("no terminal verdict on the renewal path")
	}
}

// T2 — in-flight derived-budget expiry (A2): the first retry's (gen 1's)
// first attempt blocks INSIDE the attempt until its D-derived context
// fires; the tagged expiry routes into the shared decision, observable
// progress renews, and the next generation succeeds from the advanced
// state.
func TestCompileRunSequenceRetryDerivedBudgetRenewalSucceeds(t *testing.T) {
	f := newCrFixture(t, "select nextval('seqa')")
	f.exec.failAlloc(crSeqA, 0)
	f.exec.blockOnceGen = 1
	f.exec.blockArmed = true
	f.exec.onBlock = func() { f.store.advance(crSeqA, 5) }

	runErr := f.run()

	require.NoError(t, runErr, "a derived-budget expiry with observable progress renews and succeeds")
	require.Equal(t, "6", f.proc.GetSessionInfo().SeqAddValues[crRelA])
	plain, locks, _, _ := f.exec.counts()
	require.Equal(t, 2, plain, "admission sample plus the A2 decision's tuple probe")
	require.Equal(t, 0, locks)
	deadlines := f.distinctDeadlines()
	require.GreaterOrEqual(t, len(deadlines), 2)
	require.True(t, deadlines[1].After(deadlines[0]), "the A2 route renews the window it expired against")
	require.False(t, f.state.HasWatch(crWatchA))
	if _, ok := f.state.RetryDeadline(); ok {
		t.Fatal("success after renewal leaves no residual window")
	}
}

// T3 — static-and-held no-progress terminal: the committed tuple never
// changes and the availability check keeps failing safe to held, so the
// first eligible expiry decision surfaces the named terminal exactly once.
func TestCompileRunSequenceRetryStaticHeldTerminal(t *testing.T) {
	f := newCrFixture(t, "select nextval('seqa')")
	f.exec.failAlloc(crSeqA)
	f.exec.probeHeld[crSeqA] = true
	f.sink.beforeGen[1] = func() {
		// Park the first retry generation past the window plus a fixed
		// margin. The compressed schedule sets the renewal interval equal
		// to the window, so a natural first arrival overshoots the
		// deadline by about one generation cycle — the same order as the
		// admission sample's stamping delay, which is a coin flip against
		// the Z2 young-baseline ride (terminal eligibility needs a
		// baseline one full interval old). Parking past the margin makes
		// the first decision's baseline age deterministically exceed the
		// interval, keeping the exact probe counts below.
		sleepUntilDeadlineDelta(f.state, 100*time.Millisecond)
	}

	runErr := f.run()

	require.Error(t, runErr)
	require.ErrorContains(t, runErr,
		"sequence testdb.seqa: allocation could not acquire the sequence row within 8 attempts",
		"the no-progress terminal is the named exhaustion of the recorded identity")
	identity, terminal := f.state.TerminalIdentity()
	require.True(t, terminal, "the terminal verdict is exactly-once stamped")
	require.Equal(t, crWatchA, identity)
	plain, locks, _, lockResults := f.exec.counts()
	require.Equal(t, 2, plain, "admission sample plus the terminal decision's tuple probe")
	require.Equal(t, 1, locks, "one availability check decides the terminal")
	require.Equal(t, []bool{false}, lockResults, "held: the row never became acquirable")
	require.Equal(t, 1, f.namedTerminalAborts(),
		"the terminal named error surfaces exactly once; admitted retries carry the transient cause")
}

// T4 — caller cancellation during the zero-attempt decision's tuple probe:
// the F2 precedence through the real stack. The cancelled decision neither
// renews nor retires the window, stamps no terminal, and the statement
// surfaces the caller's own cancellation, never a stale named error.
func TestCompileRunSequenceRetryCallerCancelAtZeroAttemptDecision(t *testing.T) {
	f := newCrFixture(t, "select nextval('seqa')")
	f.exec.failAlloc(crSeqA, 0)
	f.sink.beforeGen[1] = func() {
		// Block the attempt past the admission window so the allocation
		// enters at an elapsed window and must run the decision first.
		time.Sleep(crWindow + 100*time.Millisecond)
		f.exec.onProbeTuple = func() { f.cancel() }
	}

	runErr := f.run()

	require.ErrorIs(t, runErr, context.Canceled,
		"caller cancellation propagates unchanged through the decision")
	require.NotContains(t, fmt.Sprint(runErr), "within 8 attempts",
		"a cancelled statement never surfaces a stale named error")
	if _, terminal := f.state.TerminalIdentity(); terminal {
		t.Fatal("a cancelled decision never stamps the terminal verdict")
	}
	if _, ok := f.state.RetryDeadline(); !ok {
		t.Fatal("a cancelled decision neither renews nor retires the window")
	}
	plain, locks, _, _ := f.exec.counts()
	require.Equal(t, 2, plain, "admission sample plus the interrupted tuple probe")
	require.Equal(t, 0, locks, "cancellation after the tuple round preempts the availability check")
}

// T5 — caller cancellation during the conversion decision's tuple probe
// after an IN-FLIGHT derived-budget expiry: the A2 tag loses to the
// caller's own cancellation (v21 precedence, F2).
func TestCompileRunSequenceRetryCallerCancelMidConversionDecision(t *testing.T) {
	f := newCrFixture(t, "select nextval('seqa')")
	f.exec.failAlloc(crSeqA)
	f.exec.sleepGens[1] = 60 * time.Millisecond
	f.sink.beforeGen[1] = func() {
		f.exec.onProbeTuple = func() { f.cancel() }
	}

	runErr := f.run()

	require.ErrorIs(t, runErr, context.Canceled)
	require.NotContains(t, fmt.Sprint(runErr), "within 8 attempts")
	if _, terminal := f.state.TerminalIdentity(); terminal {
		t.Fatal("a cancelled decision never stamps the terminal verdict")
	}
	plain, locks, _, _ := f.exec.counts()
	require.Equal(t, 2, plain)
	require.Equal(t, 0, locks)
}

// T6 + T8 — unchanged-but-acquirable resolution closes the episode (Z1)
// and the conversion-facing seam re-admits it FRESH: the new window is
// distinct from the retired one, the new watch entry carries a new
// baseline sample, and the drained generation then succeeds. Choreography:
// gen 0 fails and admits the episode; gen 1 (the first retry) spans the
// window (50 ms per attempt), so its in-flight decision finds the row
// unchanged but acquirable — resolution closes the episode; the retry's
// scripted failures re-conflict after closure, so the conversion-facing
// seam admits a FRESH episode with a new baseline sample and a new
// window; gen 2 is unscripted and drains from the recorded state.
func TestCompileRunSequenceRetryResolutionClosesAndReadmits(t *testing.T) {
	f := newCrFixture(t, "select nextval('seqa')")
	f.exec.failAlloc(crSeqA, 0, 1)
	f.exec.sleepGens[1] = 50 * time.Millisecond

	runErr := f.run()

	require.NoError(t, runErr, "resolved contention closes the episode and the statement drains")
	require.Equal(t, "2", f.proc.GetSessionInfo().SeqAddValues[crRelA],
		"the row never advanced: the final allocation continues from the recorded state")
	plain, locks, _, lockResults := f.exec.counts()
	require.Equal(t, 3, plain,
		"admission sample, decision tuple probe, and the FRESH episode's new baseline sample")
	require.Equal(t, 1, locks)
	require.Equal(t, []bool{true}, lockResults, "acquirable: the contention is verifiably resolved")
	deadlines := f.distinctDeadlines()
	require.GreaterOrEqual(t, len(deadlines), 2,
		"closure and fresh re-admission are observable as two distinct windows")
	require.True(t, deadlines[1].After(deadlines[0]))
	require.False(t, f.state.HasWatch(crWatchA))
	if _, ok := f.state.RetryDeadline(); ok {
		t.Fatal("success after closure leaves no residual window")
	}
}

// T7 — rotation across generations produces DISTINCT episodes, not a
// conjunctive one: seqa conflicts only on the first attempt (gen 0) and
// resolves inside the first retry (its allocation succeeds, so the
// episode's watch set empties and the window retires per Z1); seqb
// becomes contended only afterwards. seqb's failure lands on the CLOSED
// window, so it re-admits FRESH — its own baseline sample and its own
// deadline (observable as the second distinct deadline). An episode
// member's admission NEVER re-anchors a live window: AdmitRetry
// preserves an existing deadline (G2) and a late watch entry only
// records its own creation baseline. seqb never progresses and stays
// held, so the FIRST eligible decision after its fresh admission finds
// it unchanged-eligible-held and stamps the terminal naming SEQB exactly
// once, while seqa's completed values stand. Live {A,B} retention — with
// a stalled identity beside an advancing or young one — is proven below
// by C1–C3; the remaining young-identity timing arms of Z2 stay pinned
// by the fake-clock decision matrix in pkg/vm/process.
func TestCompileRunSequenceRetryTerminalNamesRecordedIdentity(t *testing.T) {
	f := newCrFixture(t, "select nextval('seqa'), nextval('seqb')")
	f.exec.failAlloc(crSeqA, 0)
	f.exec.failAlloc(crSeqB)
	f.exec.probeHeld[crSeqB] = true
	// The terminal must fire at seqb's generation-2 allocation ENTRY, after
	// seqa's same-generation emission stands: parking the whole generation
	// (beforeGen) would run the expiry decision at seqa's entry — its
	// check is first in projection order — and seqa would never emit in
	// the surviving generation. Instead the single generation-2 attempt
	// parks INSIDE its ExecTxn past seqb's deadline plus margin: seqa
	// enters under the still-live window (the generation cycle is far
	// below the 300 ms window on this harness), its window-derived
	// attempt context expires mid-park, the full scripted sleep completes
	// (the fixture's per-generation sleep is not clipped by attempt
	// contexts), seqa succeeds after the deadline, and seqb's entry
	// arbitrates with seqb's baseline deterministically one full renewal
	// interval old — settling the same Z2 young-baseline boundary as the
	// static-held pin above and keeping the exact probe counts below.
	f.exec.sleepGens[2] = crWindow + 100*time.Millisecond

	runErr := f.run()

	require.Error(t, runErr)
	require.ErrorContains(t, runErr,
		"sequence testdb.seqb: allocation could not acquire the sequence row within 8 attempts",
		"the terminal names the identity recorded when the window elapsed")
	identity, terminal := f.state.TerminalIdentity()
	require.True(t, terminal)
	require.Equal(t, crWatchB, identity)
	require.False(t, f.state.HasWatch(crWatchA),
		"seqa resolved inside the episode and left the watch set")
	require.NotEmpty(t, f.proc.GetSessionInfo().SeqAddValues[crRelA],
		"seqa's completed allocations before the terminal stand")
	plain, locks, _, lockResults := f.exec.counts()
	require.Equal(t, 3, plain,
		"seqa admission sample, seqb admission sample, and the terminal decision's tuple probe")
	require.Equal(t, 1, locks)
	require.Equal(t, []bool{false}, lockResults)
	require.Equal(t, 1, f.namedTerminalAborts(), "exactly one terminal named surface")
	deadlines := f.distinctDeadlines()
	require.GreaterOrEqual(t, len(deadlines), 2,
		"Z1 closure retires seqa's window and seqb's fresh admission opens its own: two distinct deadlines")
}

// sleepUntilDeadlineDelta parks until the statement's live retry window
// plus delta (negative for before it). Admission-time jitter otherwise
// makes fixed wall-clock sleeps land on either side of the compressed
// 300 ms window unpredictably.
func sleepUntilDeadlineDelta(st *process.SequenceRetryState, delta time.Duration) {
	d, ok := st.RetryDeadline()
	if !ok {
		return
	}
	if s := time.Until(d.Add(delta)); s > 0 {
		time.Sleep(s)
	}
}

// C1 — RETAINED conjunctive contention across a renewal (#30359 review
// N4; design v21:965–974, B3 conjunctive expiry): an advancing peer never
// rescues a stalled identity. seqb projects first and succeeds on
// generation 0; seqa (projected second) fails and opens the episode. On
// generation 1 seqb contends under the LIVE window: its admission records
// a baseline WITHOUT re-anchoring D (AdmitRetry preserves the live
// deadline, G2), and its failure aborts the projection before seqa can
// resolve, so the watch set retains {seqa, seqb} across every rebuild.
// The window is doubled (600 ms) so seqb's full 8-attempt join cycle
// (~90-330 ms on this harness) always exhausts strictly inside seqa's
// first window — a live-window admission, never the expiry branch.
//
// The episode runs TWO decisions. Window 1: both sequences' peers commit
// forward ((5,1)), so the first decision — one interval after seqa's
// admission — sees seqa CHANGED and renews. Window 2: seqa's committed
// tuple freezes and its row stays held while seqb's peers commit again
// ((7,1)); the second decision sees seqa unchanged for one full renewal
// interval AND held, and seqb CHANGED: the conjunctive terminal names the
// stalled seqa exactly once — observable change on seqb cannot lift it.
// All choreography is decision-relative (the hooks read the live
// deadline): the generation index on which a decision lands varies with
// the fake engine's per-attempt cost, so fixed generation scripts or
// fixed sleeps would flake.
func TestCompileRunSequenceRetryConjunctiveAdvancingPeerDoesNotRescueStalled(t *testing.T) {
	f := newCrFixtureWindow(t, "select nextval('seqb'), nextval('seqa')", 2*crWindow)
	f.exec.failAlloc(crSeqA, 0)
	for g := uint64(1); g <= 64; g++ {
		f.exec.failAlloc(crSeqB, g)
	}
	f.exec.probeHeld[crSeqA] = true
	var d1 time.Time
	advancedAgain := false
	f.sink.beforeGen[1] = func() {
		// seqa's peers commit inside window 1: the first decision must
		// renew, never terminate.
		d1, _ = f.state.RetryDeadline()
		f.store.advance(crSeqA, 5)
	}
	// seqb's committed change vs its admission baseline acquits seqb at
	// the first decision.
	f.sink.beforeGen[2] = func() { f.store.advance(crSeqB, 5) }
	for g := uint64(3); g <= 64; g++ {
		f.sink.beforeGen[g] = func() {
			// Once the renewal is observable (a later live deadline),
			// seqb's peers commit AGAIN inside window 2 while seqa stays
			// frozen.
			if advancedAgain {
				return
			}
			if d, ok := f.state.RetryDeadline(); ok && d.After(d1) {
				advancedAgain = true
				f.store.advance(crSeqB, 7)
			}
		}
	}

	runErr := f.run()

	require.Error(t, runErr)
	require.ErrorContains(t, runErr,
		"sequence testdb.seqa: allocation could not acquire the sequence row within 8 attempts",
		"the conjunctive decision names the stalled identity, never the advancing one")
	identity, terminal := f.state.TerminalIdentity()
	require.True(t, terminal, "the terminal verdict is exactly-once stamped")
	require.Equal(t, crWatchA, identity)
	plain, locks, _, lockResults := f.exec.counts()
	require.Equal(t, 6, plain,
		"two admission samples plus one decision-round tuple probe per watched identity per decision")
	require.Equal(t, 1, locks,
		"only the stalled identity reaches the terminal decision's availability check; seqb's committed change acquits it at both decisions")
	require.Equal(t, []bool{false}, lockResults, "seqa held: the stall is real")
	require.Equal(t, 1, f.namedTerminalAborts())
	deadlines := f.distinctDeadlines()
	require.Len(t, deadlines, 2,
		"the first window and its renewal: seqb's advance buys no extension for stalled seqa")
	require.True(t, deadlines[1].After(deadlines[0]))
	require.Empty(t, f.proc.GetSessionInfo().SeqAddValues[crRelA])
	require.Empty(t, f.proc.GetSessionInfo().SeqAddValues[crRelB],
		"a statement ending in the terminal surfaces no generated values: failed generations are rewound")
	bLast, bCall := f.store.state(crSeqB)
	require.Equal(t, int64(7), bLast)
	require.True(t, bCall, "seqb observably advanced at both decisions")
	aLast, aCall := f.store.state(crSeqA)
	require.Equal(t, int64(5), aLast)
	require.True(t, aCall, "seqa froze where the first decision observed it")
}

// C2 — every recorded identity observably changed: the conjunctive
// barrier lifts and the window RENEWS instead of terminating (B3). Same
// retention choreography as C1, but seqa and seqb both commit forward
// before the first decision, so that decision renews with no availability
// check at all. The renewal is observed decision-relatively (a hook reads
// the live deadline), upon which seqb's contention ends and the next
// generation drains both sequences from the peers' committed state. The
// doubled window keeps this at exactly one decision: seqb's join cycle
// always exhausts inside the first window, and the resolving generation
// lands far inside the renewed window, so no second decision can fire
// and no quantity of generation-index arithmetic is load-sensitive.
func TestCompileRunSequenceRetryConjunctiveBothAdvancingRenews(t *testing.T) {
	f := newCrFixtureWindow(t, "select nextval('seqb'), nextval('seqa')", 2*crWindow)
	f.exec.failAlloc(crSeqA, 0)
	for g := uint64(1); g <= 64; g++ {
		f.exec.failAlloc(crSeqB, g)
	}
	var d1 time.Time
	resolved := false
	f.sink.beforeGen[1] = func() {
		d1, _ = f.state.RetryDeadline()
		// seqa's peers commit inside window 1.
		f.store.advance(crSeqA, 5)
	}
	// seqb's change vs its admission baseline: both identities CHANGED at
	// the first decision.
	f.sink.beforeGen[2] = func() { f.store.advance(crSeqB, 5) }
	for g := uint64(3); g <= 64; g++ {
		f.sink.beforeGen[g] = func() {
			// First generation after the renewal decision: seqb's
			// contention ends, so this generation drains with no further
			// admission, probe or renewal.
			if resolved {
				return
			}
			if d, ok := f.state.RetryDeadline(); ok && d.After(d1) {
				resolved = true
				f.exec.resolveAlloc(crSeqB)
			}
		}
	}

	runErr := f.run()

	require.NoError(t, runErr,
		"conjunctive renewal holds while every contended identity observably changes")
	require.Equal(t, "6", f.proc.GetSessionInfo().SeqAddValues[crRelA],
		"seqa drains from its peers' committed state")
	require.Equal(t, "6", f.proc.GetSessionInfo().SeqAddValues[crRelB],
		"seqb drains from its peers' committed state")
	plain, locks, _, _ := f.exec.counts()
	require.Equal(t, 4, plain,
		"two admission samples plus the single decision round's tuple probes")
	require.Equal(t, 0, locks,
		"observable change acquits both identities without any availability check")
	deadlines := f.distinctDeadlines()
	require.Len(t, deadlines, 2,
		"exactly one renewal: success precedes any second decision")
	require.True(t, deadlines[1].After(deadlines[0]),
		"the renewed window extends past the original deadline")
	require.Equal(t, 0, f.namedTerminalAborts())
	if _, terminal := f.state.TerminalIdentity(); terminal {
		t.Fatal("no terminal verdict on the conjunctive renewal path")
	}
	require.False(t, f.state.HasWatch(crWatchA), "success removes seqa's watch entry")
	require.False(t, f.state.HasWatch(crWatchB), "success removes seqb's watch entry")
}

// C3 — a LATE identity receives its own observation interval (Z2 at
// compiler scale; #30359 review N4): seqb joins during seqa's first
// window, late enough that its baseline stays YOUNG (age below the
// renewal interval) at the first decision: a young identity can be
// neither terminal nor renewable, so seqa (old, unchanged, held)
// terminates immediately and names itself; the late join neither extends
// seqa's window nor delays its verdict. The doubled window plus the
// deadline-relative join (D-450 ms) makes both sides robust: seqb's full
// 8-attempt join cycle (~90-330 ms) always exhausts strictly inside
// seqa's first window — a live-window admission, never the expiry branch
// — and seqb's baseline age at the decision, 450 ms minus one cycle,
// stays inside (0, interval) with >=100 ms margin on both sides. The
// single observed deadline pins that no re-anchoring ever happened.
func TestCompileRunSequenceRetryConjunctiveLateJoinKeepsStalledVerdict(t *testing.T) {
	f := newCrFixtureWindow(t, "select nextval('seqb'), nextval('seqa')", 2*crWindow)
	f.exec.failAlloc(crSeqA, 0)
	for g := uint64(1); g <= 64; g++ {
		f.exec.failAlloc(crSeqB, g)
	}
	f.exec.probeHeld[crSeqA] = true
	// seqb joins 450 ms into seqa's 600 ms window: the join cycle
	// exhausts inside the live window, so the admission samples seqb's
	// baseline; that baseline's age at the first decision (at or just
	// past the deadline) is 450 ms minus one join cycle — young.
	f.sink.beforeGen[1] = func() { sleepUntilDeadlineDelta(f.state, -450*time.Millisecond) }

	runErr := f.run()

	require.Error(t, runErr)
	require.ErrorContains(t, runErr,
		"sequence testdb.seqa: allocation could not acquire the sequence row within 8 attempts",
		"the young identity neither delays nor rescues the stalled verdict")
	identity, terminal := f.state.TerminalIdentity()
	require.True(t, terminal)
	require.Equal(t, crWatchA, identity)
	plain, locks, _, lockResults := f.exec.counts()
	require.Equal(t, 4, plain,
		"two admission samples plus the single decision round's tuple probes")
	require.Equal(t, 1, locks,
		"a young identity takes no availability check: it can be neither terminal nor renewable")
	require.Equal(t, []bool{false}, lockResults, "seqa held is the only checked row")
	require.Equal(t, 1, f.namedTerminalAborts())
	deadlines := f.distinctDeadlines()
	require.Len(t, deadlines, 1,
		"a late join never re-anchors the live window: one deadline, no extension")
}

// N1 (#30359 review): a prepared-cursor EXECUTE keeps its rows off the
// wire (retained for COM_STMT_FETCH), so a failed generation's captured
// prefix stays retained when the statement retries; the retry transition
// must invoke the armed discard hook so FETCH observes exactly the
// retried result. Compile.Run numbers no Write for the failed generation
// (the projection aborts upstream of pipeline output), so the captured
// prefix is scripted explicitly — the REAL cursor capture path and its
// RELEASE (row set, byte reservation, staged saved-result blocks) are
// proven end to end in the frontend UT
// TestRetryDiscardOutputReleasesFailedCursorGeneration. This test proves
// the compiler's transition invokes the hook exactly once per retry and
// that the retained set holds exactly the final generation's result.
func TestCompileRunRetryDiscardOutputInvokedOnTransition(t *testing.T) {
	// The oracle (hook exactly once at the transition; retained set holds the
	// retried generation's result) has no window-timing content, so pin a wide
	// window: at the default 300 ms a load-stalled first tier-1 cycle can let
	// the sticky window lapse before the retry seam's admission, making the
	// expiry decision's bounded probe fail-safe-decline the retry and surface
	// the tier-1 named error instead of draining.
	f := newCrFixtureWindow(t, "select nextval('seqa')", 10*crWindow)
	f.exec.failAlloc(crSeqA, 0)

	var mu sync.Mutex
	discards := 0
	var retained []string
	f.proc.GetSessionInfo().RetryDiscardOutput = func() error {
		mu.Lock()
		discards++
		retained = retained[:0]
		mu.Unlock()
		return nil
	}
	f.sink.writeHook = func() {
		mu.Lock()
		retained = append(retained, "result-batch")
		mu.Unlock()
	}
	f.sink.beforeGen[0] = func() {
		mu.Lock()
		retained = append(retained, "failed-generation-cursor-prefix")
		mu.Unlock()
	}

	runErr := f.run()

	require.NoError(t, runErr, "the retried statement drains after its failed generation")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, discards,
		"one retry transition discards the failed generation's retained output exactly once")
	require.Equal(t, []string{"result-batch"}, retained,
		"FETCH observes exactly the retried generation's result: no stale prefix, no loss")
}

// N1-extension (#30359 review): retained-output consumers beyond the
// cursor row set — SELECT ... INTO @var's per-statement collector and
// PERFORM's saved-result blocks — rely on the same transition-time
// discard: when a failed generation retained statement output, the retry
// transition must run the armed hook after the failed generation quiesced
// and before the retried generation's first emission. The REAL consumer
// resets/aborts are frontend machinery, proven end to end in
// TestRetryDiscardOutputResetsSelectIntoCapture and
// TestRetryDiscardOutputAbortsPerformSavedResult; these pins prove the
// compiler-side ordering contract with generation-aware scripted
// emissions: a one-row retry holds exactly the retried row, and an empty
// retry holds nothing the failed generation left behind.
func TestCompileRunRetryDiscardOutputResetsRetainedConsumers(t *testing.T) {
	for _, tc := range []struct {
		name             string
		retriedEmissions []string
		want             []string
	}{
		{name: "one-row retry", retriedEmissions: []string{"retried-row"}, want: []string{"retried-row"}},
		{name: "empty retry", retriedEmissions: nil, want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Same wide window as the cursor pin: the oracle (discard
			// exactly once, in order; retained set holds exactly the
			// retried generation's emissions) has no window-timing content.
			f := newCrFixtureWindow(t, "select nextval('seqa')", 10*crWindow)
			f.exec.failAlloc(crSeqA, 0)

			var mu sync.Mutex
			var events []string
			var retained []string
			discards := 0
			f.proc.GetSessionInfo().RetryDiscardOutput = func() error {
				mu.Lock()
				discards++
				events = append(events, "discard")
				retained = nil
				mu.Unlock()
				return nil
			}
			f.sink.writeHook = func() {
				mu.Lock()
				events = append(events, "write")
				retained = append(retained, tc.retriedEmissions...)
				mu.Unlock()
			}
			f.sink.beforeGen[0] = func() {
				mu.Lock()
				events = append(events, "gen0-retain")
				retained = append(retained, "failed-generation-row")
				mu.Unlock()
			}

			runErr := f.run()

			require.NoError(t, runErr, "the retried statement drains after its failed generation")
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, 1, discards,
				"one retry transition discards the failed generation's retained output exactly once")
			require.Equal(t, []string{"gen0-retain", "discard", "write"}, events,
				"the discard runs after the failed generation retained output and before the retried emission")
			require.Equal(t, tc.want, retained,
				"the consumer observes exactly the retried generation's emissions")
		})
	}
}

// P3 regression (#30359 review): when the armed attempt-output discard
// reports a cleanup failure, prepareRetryTransition surfaces that error
// and the statement never rebuilds a retried generation over the retained
// output that survived its cleanup — no replay, no client emission; the
// client observes the cleanup error. The frontend half
// (TestRetryDiscardOutputRetainsSaverOnCleanupFailure) proves the failed
// cleanup keeps its owner so terminal statement cleanup can still reach
// the surviving files.
func TestCompileRunRetryDiscardOutputErrorPreventsReplay(t *testing.T) {
	// Same wide window as the sibling pins: the oracle (the hook error
	// surfaces; zero generations rebuilt) has no window-timing content.
	f := newCrFixtureWindow(t, "select nextval('seqa')", 10*crWindow)
	f.exec.failAlloc(crSeqA, 0)

	var mu sync.Mutex
	hookCalls := 0
	genRebuilt := false
	writes := 0
	f.proc.GetSessionInfo().RetryDiscardOutput = func() error {
		mu.Lock()
		hookCalls++
		mu.Unlock()
		return moerr.NewInternalError(context.Background(), "injected attempt-output cleanup failure")
	}
	f.sink.beforeGen[1] = func() {
		mu.Lock()
		genRebuilt = true
		mu.Unlock()
	}
	f.sink.writeHook = func() {
		mu.Lock()
		writes++
		mu.Unlock()
	}

	runErr := f.run()

	require.Error(t, runErr, "a cleanup failure abandons the retry instead of replaying")
	require.Contains(t, runErr.Error(), "injected attempt-output cleanup failure",
		"the client observes the attempt-cleanup error")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, hookCalls, "the transition invokes the discard exactly once")
	require.False(t, genRebuilt, "no retried generation is built over surviving retained output")
	require.Zero(t, writes, "no client output is emitted after the abandoned retry")
}
