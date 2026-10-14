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

package ttl

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type catalogRow struct {
	account         uint32
	id              uint64
	data            []byte
	database, table string
}
type catalogExecutor struct {
	executor.SQLExecutor
	t           *testing.T
	mp          *mpool.MPool
	rows        []catalogRow
	queries     []string
	deleted     []string
	before      func(context.Context, string) error
	handoff     func(string)
	transform   func(*executor.Result)
	transaction func(context.Context, int) error
}

func newCatalogExecutor(t *testing.T, rows []catalogRow) *catalogExecutor {
	t.Helper()
	e := &catalogExecutor{t: t, mp: mpool.MustNewZero(), rows: rows}
	t.Cleanup(func() { require.Zero(t, e.mp.CurrNB()) })
	return e
}

func catalogRows(t *testing.T, n int, policy bool) []catalogRow {
	t.Helper()
	var data []byte
	if policy {
		data = validRetryCandidate(t).Constraint
	}
	rows := make([]catalogRow, n)
	for i := range rows {
		rows[i] = catalogRow{id: uint64(i + 1), data: data, database: fmt.Sprintf("d%d", i+1), table: "t"}
	}
	return rows
}

var seekPattern = regexp.MustCompile(`account_id > (\d+) OR \(account_id = \d+ AND rel_id > (\d+)`)
var upperPattern = regexp.MustCompile(`account_id < (\d+) OR \(account_id = \d+ AND rel_id <= (\d+)`)
var limitPattern = regexp.MustCompile(`LIMIT (\d+)$`)
var widthPattern = regexp.MustCompile("LENGTH\\(`constraint`\\) <= (\\d+)")

func (e *catalogExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	require.True(e.t, opts.HasAccountID())
	require.Zero(e.t, opts.AccountID())
	require.Zero(e.t, e.mp.CurrNB(), "previous page must close before the next query")
	e.queries = append(e.queries, sql)
	if e.before != nil {
		if err := e.before(ctx, sql); err != nil {
			return executor.Result{}, err
		}
	}
	rows := append([]catalogRow(nil), e.rows...)
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].account < rows[j].account || (rows[i].account == rows[j].account && rows[i].id < rows[j].id)
	})
	if !strings.Contains(sql, "rel_version") {
		m := executor.NewMemResult([]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType()}, e.mp)
		owned := true
		defer func() {
			if owned {
				m.GetResult().Close()
			}
		}()
		if len(rows) != 0 {
			row := rows[len(rows)-1]
			m.NewBatchWithRowCount(1)
			require.NoError(e.t, executor.AppendFixedRows(m, 0, []uint32{row.account}))
			require.NoError(e.t, executor.AppendFixedRows(m, 1, []uint64{row.id}))
		}
		if e.handoff != nil {
			e.handoff(sql)
		}
		owned = false
		return m.GetResult(), nil
	}
	seek, upper, limit, width := seekPattern.FindStringSubmatch(sql), upperPattern.FindStringSubmatch(sql), limitPattern.FindStringSubmatch(sql), widthPattern.FindStringSubmatch(sql)
	require.Len(e.t, seek, 3)
	require.Len(e.t, upper, 3)
	require.Len(e.t, limit, 2)
	require.Len(e.t, width, 2)
	parse := func(s string) uint64 { n, err := strconv.ParseUint(s, 10, 64); require.NoError(e.t, err); return n }
	a, id, ua, uid, size, maxWidth := parse(seek[1]), parse(seek[2]), parse(upper[1]), parse(upper[2]), int(parse(limit[1])), int(parse(width[1]))
	selected := make([]catalogRow, 0, size)
	for _, row := range rows {
		if (uint64(row.account) > a || (uint64(row.account) == a && row.id > id)) && (uint64(row.account) < ua || (uint64(row.account) == ua && row.id <= uid)) {
			selected = append(selected, row)
			if len(selected) == size {
				break
			}
		}
	}
	m := executor.NewMemResult([]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint32.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_blob.ToType(), types.T_int64.ToType()}, e.mp)
	owned := true
	defer func() {
		if owned {
			m.GetResult().Close()
		}
	}()
	// Two-row batches also exercise ownership/cancellation between result batches.
	for start := 0; start < len(selected); start += 2 {
		end := min(start+2, len(selected))
		n := end - start
		m.NewBatchWithRowCount(n)
		accounts, ids, versions, lengths := make([]uint32, n), make([]uint64, n), make([]uint32, n), make([]int64, n)
		dbs, tables, data := make([]string, n), make([]string, n), make([][]byte, n)
		for i, row := range selected[start:end] {
			accounts[i], ids[i], lengths[i] = row.account, row.id, int64(len(row.data))
			if len(row.database) <= maxDiscoveryNameBytes {
				dbs[i] = row.database
			}
			if len(row.table) <= maxDiscoveryNameBytes {
				tables[i] = row.table
			}
			if len(row.data) <= maxWidth {
				data[i] = row.data
			}
		}
		require.NoError(e.t, executor.AppendFixedRows(m, 0, accounts))
		require.NoError(e.t, executor.AppendFixedRows(m, 1, ids))
		require.NoError(e.t, executor.AppendFixedRows(m, 2, versions))
		require.NoError(e.t, executor.AppendStringRows(m, 3, dbs))
		require.NoError(e.t, executor.AppendStringRows(m, 4, tables))
		require.NoError(e.t, executor.AppendBytesRows(m, 5, data))
		require.NoError(e.t, executor.AppendFixedRows(m, 6, lengths))
	}
	result := m.GetResult()
	if e.transform != nil {
		e.transform(&result)
	}
	if e.handoff != nil {
		e.handoff(sql)
	}
	owned = false
	return result, nil
}

func (e *catalogExecutor) ExecTxn(ctx context.Context, _ func(executor.TxnExecutor) error, opts executor.Options) error {
	require.Zero(e.t, e.mp.CurrNB(), "discovery result must be released before deletion")
	e.deleted = append(e.deleted, opts.Database())
	if e.transaction != nil {
		return e.transaction(ctx, len(e.deleted))
	}
	return nil
}

func windowIDs(w discoveryWindow) []uint64 {
	ids := make([]uint64, len(w.candidates))
	for i, c := range w.candidates {
		ids[i] = c.TableID
	}
	return ids
}

func nextWindow(t *testing.T, e *catalogExecutor, cursor discoveryCursor, options DiscoveryOptions) discoveryWindow {
	t.Helper()
	var diagnostics discoveryDiagnostics
	w, err := discoverWindow(t.Context(), e, cursor, options, &diagnostics)
	require.NoError(t, err)
	require.NoError(t, diagnostics.err())
	require.Zero(t, e.mp.CurrNB())
	return w
}

func TestTTLDiscoveryPageAndCandidateBudgets(t *testing.T) {
	for _, kind := range []string{"pages", "count", "bytes"} {
		t.Run(kind, func(t *testing.T) {
			e := newCatalogExecutor(t, catalogRows(t, 5, true))
			o := DefaultDiscoveryOptions()
			o.PageSize = 2
			o.MaxPages = 1
			if kind == "count" {
				o.PageSize = 5
				o.MaxCandidates = 2
			}
			if kind == "bytes" {
				o.PageSize = 5
				c := validRetryCandidate(t)
				o.MaxCandidateBytes = 2*candidateCredit(c.Constraint, "d1", "t", "ts") + 1
			}
			w := nextWindow(t, e, discoveryCursor{}, o)
			require.Equal(t, []uint64{1, 2}, windowIDs(w))
			require.Equal(t, uint64(2), w.last.table)
			require.False(t, w.exhausted)
			require.Len(t, e.queries, 2)
			var bytes int
			for _, c := range w.candidates {
				bytes += candidateCredit(c.Constraint, c.Database, c.Table, c.Policy.Column)
				require.Equal(t, "ts", c.Policy.Column)
				require.Equal(t, e.rows[0].data, c.Constraint)
			}
			require.LessOrEqual(t, bytes, o.MaxCandidateBytes)
			w = nextWindow(t, e, discoveryCursor{after: w.last, upper: w.upper, active: true}, o)
			require.Equal(t, []uint64{3, 4}, windowIDs(w))
			w = nextWindow(t, e, discoveryCursor{after: w.last, upper: w.upper, active: true}, o)
			require.Equal(t, []uint64{5}, windowIDs(w))
			require.True(t, w.exhausted)
		})
	}
}

func TestTTLDiscoveryNonTTLPrefixAndTenantKeys(t *testing.T) {
	rows := catalogRows(t, 5, false)
	rows[4].data = validRetryCandidate(t).Constraint
	rows = append(rows, catalogRow{account: 1, id: 1, data: rows[4].data, database: "tenant", table: "t"})
	e := newCatalogExecutor(t, rows)
	o := DefaultDiscoveryOptions()
	o.PageSize = 2
	o.MaxPages = 1
	var cursor discoveryCursor
	for i := 0; i < 3; i++ {
		w := nextWindow(t, e, cursor, o)
		if i < 2 {
			require.Empty(t, w.candidates)
			require.False(t, w.exhausted)
		} else {
			require.Equal(t, []uint64{5, 1}, windowIDs(w))
			require.Equal(t, uint32(1), w.candidates[1].AccountID)
			require.True(t, w.exhausted)
		}
		cursor = discoveryCursor{after: w.last, upper: w.upper, active: true}
	}
	require.Len(t, e.queries, 4)
}

func TestTTLDiscoveryOversizedCorruptAndErrorBounds(t *testing.T) {
	rows := catalogRows(t, 12, true)
	for i := 0; i < 10; i++ {
		rows[i].data = []byte{255}
	}
	rows[0].data = make([]byte, DefaultDiscoveryOptions().MaxConstraintBytes+1)
	rows[1].database = strings.Repeat("n", maxDiscoveryNameBytes+1)
	e := newCatalogExecutor(t, rows)
	var diagnostics discoveryDiagnostics
	w, err := discoverWindow(t.Context(), e, discoveryCursor{}, DefaultDiscoveryOptions(), &diagnostics)
	require.NoError(t, err)
	require.Equal(t, []uint64{11, 12}, windowIDs(w))
	require.True(t, w.exhausted)
	require.Len(t, diagnostics.samples, 8)
	require.Equal(t, 2, diagnostics.omitted)
	require.Less(t, len(diagnostics.err().Error()), 2048)
	var long discoveryDiagnostics
	for i := 0; i < 32; i++ {
		long.add(^uint32(0), ^uint64(0), strings.Repeat("large", 1000))
	}
	require.Len(t, long.samples, 8)
	require.Equal(t, 24, long.omitted)
	require.Less(t, len(long.err().Error()), 2048)
	o := DefaultDiscoveryOptions()
	o.MaxCandidateBytes = candidateFixedCredit
	var small discoveryDiagnostics
	w, err = discoverWindow(t.Context(), e, discoveryCursor{}, o, &small)
	require.NoError(t, err)
	require.Empty(t, w.candidates)
	require.True(t, w.exhausted)
	require.Error(t, small.err())
}

func TestTTLDiscoveryCancelPageHandoff(t *testing.T) {
	for _, n := range []int{32, 64} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			e := newCatalogExecutor(t, catalogRows(t, n, true))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			e.handoff = func(sql string) {
				if strings.Contains(sql, "rel_version") {
					cancel()
				}
			}
			got, err := Discover(ctx, e)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, got)
			require.Len(t, e.queries, 2)
			require.Zero(t, e.mp.CurrNB())
		})
	}
}

type cancelDuringDecode struct {
	context.Context
	cancel context.CancelFunc
	calls  atomic.Int32
}

func (c *cancelDuringDecode) Err() error {
	if c.calls.Add(1) == 5 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestTTLDiscoveryDecodeCancellation(t *testing.T) {
	properties := make([]*plan.Property, 16, 17)
	for i := range properties {
		properties[i] = &plan.Property{Key: "not_ttl", Value: "v"}
	}
	properties = append(properties, &plan.Property{Key: PropertyKey, Value: Encode(&Policy{Version: 1, Column: "ts", Seconds: 1})})
	c := engine.ConstraintDef{Cts: []engine.Constraint{&engine.StreamConfigsDef{Configs: properties}}}
	data, err := c.MarshalBinary()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p, err := policyFromConstraint(&cancelDuringDecode{Context: ctx, cancel: cancel}, data)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, p)
}

func TestTTLTaskContinuationFenceAndWrap(t *testing.T) {
	rows := catalogRows(t, 33, true)
	for i := range rows {
		rows[i].id += 9
		rows[i].database = fmt.Sprintf("d%d", rows[i].id)
	}
	e := newCatalogExecutor(t, rows)
	invoke := TaskExecutor(e)
	require.NoError(t, invoke(t.Context(), nil))
	require.Len(t, e.deleted, 32)
	e.rows = append(e.rows, catalogRow{id: 5, data: rows[0].data, database: "d5", table: "t"}, catalogRow{id: 100, data: rows[0].data, database: "d100", table: "t"})
	require.NoError(t, invoke(t.Context(), nil))
	require.Len(t, e.deleted, 33)
	require.Equal(t, "d42", e.deleted[32])
	require.NoError(t, invoke(t.Context(), nil))
	require.Equal(t, "d5", e.deleted[33])
	require.NotContains(t, e.deleted, "d100")
	require.NoError(t, invoke(t.Context(), nil))
	require.Contains(t, e.deleted, "d100")
}

func TestTTLRunDoesNotSkipUnattemptedCandidates(t *testing.T) {
	e := newCatalogExecutor(t, catalogRows(t, 3, true))
	options := DefaultWorkerOptions()
	var cursor discoveryCursor
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	e.transaction = func(context.Context, int) error {
		if len(e.deleted) == 2 {
			cancel()
			return context.Canceled
		}
		return nil
	}
	require.ErrorIs(t, run(ctx, e, time.Now(), options, &cursor), context.Canceled)
	require.Equal(t, uint64(1), cursor.after.table)
	e.transaction = nil
	require.NoError(t, run(t.Context(), e, time.Now(), options, &cursor))
	require.Equal(t, []string{"d1", "d2", "d2", "d3"}, e.deleted)
	require.Equal(t, discoveryCursor{}, cursor)
}

func TestTTLDiscoveryFailurePreservesContinuation(t *testing.T) {
	e := newCatalogExecutor(t, catalogRows(t, 3, true))
	injected := errors.New("catalog unavailable")
	e.before = func(context.Context, string) error { return injected }
	cursor := discoveryCursor{after: discoveryKey{0, 1}, upper: discoveryKey{0, 3}, active: true}
	original := cursor
	require.ErrorContains(t, run(t.Context(), e, time.Now(), DefaultWorkerOptions(), &cursor), injected.Error())
	require.Equal(t, original, cursor)
	require.Empty(t, e.deleted)
}

func TestTTLTaskAdmissionCancelAndReuse(t *testing.T) {
	e := newCatalogExecutor(t, nil)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	finished := make(chan struct{})
	var once atomic.Bool
	e.before = func(ctx context.Context, _ string) error {
		if once.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	invoke := TaskExecutor(e)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	t.Cleanup(func() {
		cancel()
		select {
		case <-release:
		default:
			close(release)
		}
		<-finished
	})
	go func() { defer close(finished); done <- invoke(ctx, nil) }()
	select {
	case <-entered:
	case <-t.Context().Done():
		t.Fatal("first invocation did not enter")
	}
	err := invoke(t.Context(), nil)
	require.ErrorContains(t, err, "already running")
	canceled, stop := context.WithCancel(t.Context())
	stop()
	require.ErrorIs(t, invoke(canceled, nil), context.Canceled)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	close(release)
	require.NoError(t, invoke(t.Context(), nil))
}

func TestTTLDiscoveryLimitsAndSQLShape(t *testing.T) {
	defaults := DefaultDiscoveryOptions()
	for _, o := range []DiscoveryOptions{{}, {PageSize: 129, MaxPages: 4, MaxCandidates: 32, MaxCandidateBytes: 128 << 10, MaxConstraintBytes: 16 << 10}} {
		require.Error(t, o.validate())
	}
	q := discoverySQL(discoveryKey{2, 9}, discoveryKey{3, 17}, defaults)
	require.Contains(t, q, "CASE WHEN LENGTH(`constraint`) <= 16384 THEN `constraint` ELSE NULL END")
	require.Contains(t, q, "CASE WHEN LENGTH(reldatabase) <= 256")
	require.Contains(t, q, "CASE WHEN LENGTH(relname) <= 256")
	require.Contains(t, q, "rel_id > 9")
	require.Contains(t, q, "rel_id <= 17")
	require.True(t, strings.HasSuffix(q, "LIMIT 128"))
	require.NotContains(t, q, "OFFSET")
	require.Error(t, (Policy{Version: 1, Column: strings.Repeat("c", 257), Seconds: 1}).Validate())
}

func TestTTLDiscoveryConstantAndNullVectors(t *testing.T) {
	for _, policy := range []bool{false, true} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			e := newCatalogExecutor(t, catalogRows(t, 3, policy))
			e.transform = func(res *executor.Result) {
				for _, b := range res.Batches {
					b.Vecs[0].Free(e.mp)
					var err error
					b.Vecs[0], err = vector.NewConstFixed(types.T_uint32.ToType(), uint32(0), b.RowCount(), e.mp)
					require.NoError(t, err)
					b.Vecs[2].Free(e.mp)
					b.Vecs[2], err = vector.NewConstFixed(types.T_uint32.ToType(), uint32(2), b.RowCount(), e.mp)
					require.NoError(t, err)
					if !policy {
						b.Vecs[5].Free(e.mp)
						b.Vecs[5] = vector.NewConstNull(types.T_blob.ToType(), b.RowCount(), e.mp)
						b.Vecs[6].Free(e.mp)
						b.Vecs[6] = vector.NewConstNull(types.T_int64.ToType(), b.RowCount(), e.mp)
					}
				}
			}
			w := nextWindow(t, e, discoveryCursor{}, DefaultDiscoveryOptions())
			require.True(t, w.exhausted)
			if policy {
				require.Len(t, w.candidates, 3)
				for _, c := range w.candidates {
					require.Equal(t, uint32(2), c.Version)
				}
			} else {
				require.Empty(t, w.candidates)
			}
		})
	}
}

func TestTTLDiscoveryInvalidPagePreservesContinuation(t *testing.T) {
	e := newCatalogExecutor(t, catalogRows(t, 3, true))
	e.transform = func(res *executor.Result) {
		ids := vector.MustFixedColNoTypeCheck[uint64](res.Batches[0].Vecs[1])
		ids[1] = ids[0]
	}
	cursor := discoveryCursor{after: discoveryKey{0, 1}, upper: discoveryKey{0, 3}, active: true}
	original := cursor
	require.ErrorContains(t, run(t.Context(), e, time.Now(), DefaultWorkerOptions(), &cursor), "key order")
	require.Equal(t, original, cursor)
	require.Empty(t, e.deleted)
	require.Zero(t, e.mp.CurrNB())
}

func TestTTLTaskNonTTLWindowsAdvance(t *testing.T) {
	rows := catalogRows(t, 513, false)
	rows[512].data = validRetryCandidate(t).Constraint
	e := newCatalogExecutor(t, rows)
	invoke := TaskExecutor(e)
	require.NoError(t, invoke(t.Context(), nil))
	require.Empty(t, e.deleted)
	require.Len(t, e.queries, 5)
	require.NoError(t, invoke(t.Context(), nil))
	require.Equal(t, []string{"d513"}, e.deleted)
	require.Len(t, e.queries, 6)
}

func TestTTLTaskBoundsFatalAndTableDiagnostics(t *testing.T) {
	for _, phase := range []string{"discovery", "table"} {
		t.Run(phase, func(t *testing.T) {
			e := newCatalogExecutor(t, catalogRows(t, 2, true))
			injected := errors.New(strings.Repeat("large failure", 1000))
			if phase == "discovery" {
				e.before = func(context.Context, string) error { return injected }
			} else {
				e.transaction = func(context.Context, int) error { return injected }
			}
			var cursor discoveryCursor
			err := run(t.Context(), e, time.Now(), DefaultWorkerOptions(), &cursor)
			require.Error(t, err)
			require.Less(t, len(err.Error()), 2048)
			if phase == "table" {
				require.Equal(t, []string{"d1", "d2"}, e.deleted)
			} else {
				require.Empty(t, e.deleted)
			}
		})
	}
	require.LessOrEqual(t, 4*int(unsafe.Sizeof(Candidate{})), candidateFixedCredit, "fixed credit must cover candidate/slice allocation growth")
}

type changedConstraintTxn struct {
	executor.TxnExecutor
	result  executor.Result
	queries []string
	locks   int
}

func (e *changedConstraintTxn) LockTable(string) error { e.locks++; return nil }
func (e *changedConstraintTxn) Exec(sql string, _ executor.StatementOption) (executor.Result, error) {
	e.queries = append(e.queries, sql)
	return e.result, nil
}

type changedConstraintExecutor struct {
	executor.SQLExecutor
	tx *changedConstraintTxn
}

func (e *changedConstraintExecutor) ExecTxn(_ context.Context, fn func(executor.TxnExecutor) error, _ executor.Options) error {
	return fn(e.tx)
}

func TestTTLRevalidationRejectsUncreditedMetadataGrowth(t *testing.T) {
	c := validRetryCandidate(t)
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	m := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_uint32.ToType(), types.T_blob.ToType()}, mp)
	m.NewBatchWithRowCount(1)
	handedOff := false
	t.Cleanup(func() {
		if !handedOff {
			m.GetResult().Close()
		}
	})
	require.NoError(t, executor.AppendFixedRows(m, 0, []uint64{c.TableID}))
	require.NoError(t, executor.AppendFixedRows(m, 1, []uint32{c.Version}))
	// The SQL length guard emits NULL when current metadata grew after discovery.
	m.GetResult().Batches[0].Vecs[2] = vector.NewConstNull(types.T_blob.ToType(), 1, mp)
	tx := &changedConstraintTxn{result: m.GetResult()}
	handedOff = true
	n, err := DeleteBatch(t.Context(), &changedConstraintExecutor{tx: tx}, c, time.Now(), 1)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Equal(t, 1, tx.locks)
	require.Len(t, tx.queries, 1)
	require.Contains(t, tx.queries[0], fmt.Sprintf("CASE WHEN LENGTH(`constraint`) = %d THEN `constraint` ELSE NULL END", len(c.Constraint)))
	require.Zero(t, mp.CurrNB())
}

type partialDiscoveryExecutor struct {
	executor.SQLExecutor
	result executor.Result
	err    error
}

func (e *partialDiscoveryExecutor) Exec(context.Context, string, executor.Options) (executor.Result, error) {
	return e.result, e.err
}
func TestTTLDiscoveryClosesPartialErrorResult(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprint(active), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			m := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, mp)
			m.NewBatchWithRowCount(1)
			owned := true
			t.Cleanup(func() {
				if owned {
					m.GetResult().Close()
				}
			})
			require.NoError(t, executor.AppendFixedRows(m, 0, []uint64{1}))
			injected := errors.New("partial query error")
			e := &partialDiscoveryExecutor{result: m.GetResult(), err: injected}
			owned = false
			var diagnostics discoveryDiagnostics
			w, err := discoverWindow(t.Context(), e, discoveryCursor{upper: discoveryKey{0, 1}, active: active}, DefaultDiscoveryOptions(), &diagnostics)
			require.ErrorIs(t, err, injected)
			require.Empty(t, w.candidates)
			require.Zero(t, mp.CurrNB())
		})
	}
}
