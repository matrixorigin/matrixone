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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// The spill thresholds join_spill_mem, agg_spill_mem and sort_spill_mem are
// dynamic SESSION variables baked into plans at build time. Changing one on
// a live connection must evict the session plan cache and mark prepared
// plan generations for rebuild, otherwise repeated identical SQL keeps
// running with the stale threshold (issue #31836).
//
// Join coverage reuses the canonical minimal physically spilling fixture
// from issue #26408. The spill oracle is the deterministic real-execution
// statistic mo_hashbuild_spill_depth_total (labels action="spill",depth="1"
// and action="respill",depth="2"): every stage reads the counters directly
// before and after executing the statement itself, so a positive stage is
// caused only by the execution under test and a negative stage is a
// positive-control proof. No file scanning and no explain-analyze of
// another statement is used as spill evidence.
//
// Declared coverage per consumer:
//   - join: identical SQL across auto(0) <-> low(32) with exact count/sum
//     oracles and real spill-counter positive/negative controls:
//     (a) the direct cached SQL, (b) a retained SQL PREPARE handle,
//     (c) the same-predicate direct and PREPARE controls, and (d) retained
//     and freshly-prepared binary-protocol handles carrying one real bound
//     parameter across the same SET transitions.
//   - agg/sort: exact-result ladders across SET transitions (generation
//     invalidation through the same-SQL rerun); spill presence for these
//     operators is covered by the canonical CI BVT cases, not asserted here.
//
// Lifecycle contracts: every acquired connection, rows handle and statement
// registers its scoped close immediately after the acquisition succeeds and
// before any fatal assertion. The captured pre-call GLOBAL value plus the
// own catalog are always restored through an independent fresh connection
// with readback, regardless of session failures; the original session is
// restored on the normal tail and independently guaranteed closed on every
// path. Cancellation uses the deterministic server-side max_execution_time
// path only; no driver context cancel is used.
//
// Entries: the canonical embedded-cluster entry runs in CI. The directed
// private-cluster entry executes the exact same scenario source against an
// explicitly provided temporary endpoint (MEOW31836_SPILL_DSN plus
// MEOW31836_SPILL_METRICS; without them it skips) for local private-range
// validation and never targets a shared DSN.
const meowSpillRows = int64(4096)

func TestSessionSpillMemDynamicChangesAffectSameSQLExecution(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists meow31836_spillmem")
		port := cn.GetServiceConfig().CN.Frontend.Port
		runSpillMemScenario(t, fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port), func() (float64, float64, error) {
			return promtestutil.ToFloat64(metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1")),
				promtestutil.ToFloat64(metricv2.HashBuildSpillDepthCounter.WithLabelValues("respill", "2")), nil
		})
	})
}

func TestSessionSpillMemDirectedPrivateCluster(t *testing.T) {
	dsn := os.Getenv("MEOW31836_SPILL_DSN")
	if dsn == "" {
		t.Skip("directed entry: set MEOW31836_SPILL_DSN and MEOW31836_SPILL_METRICS to run")
	}
	base := os.Getenv("MEOW31836_SPILL_METRICS")
	require.NotEmpty(t, base, "MEOW31836_SPILL_METRICS is required together with MEOW31836_SPILL_DSN")
	runSpillMemScenario(t, dsn, scrapeSpillCounters(base))
}

// scrapeSpillCounters reads the deterministic hashbuild spill counters from
// the explicit metrics endpoint of the directed cluster. A missing series
// means the counter has never been admitted, i.e. zero.
func scrapeSpillCounters(base string) func() (float64, float64, error) {
	reSpill := regexp.MustCompile(`(?m)^mo_hashbuild_spill_depth_total\{action="spill",depth="1"\}\s+([0-9eE+\-.]+)$`)
	reRespill := regexp.MustCompile(`(?m)^mo_hashbuild_spill_depth_total\{action="respill",depth="2"\}\s+([0-9eE+\-.]+)$`)
	client := &http.Client{Timeout: 10 * time.Second}
	return func() (float64, float64, error) {
		resp, err := client.Get(strings.TrimSuffix(base, "/") + "/metrics")
		if err != nil {
			return 0, 0, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return 0, 0, err
		}
		read := func(re *regexp.Regexp) float64 {
			m := re.FindSubmatch(body)
			if m == nil {
				return 0
			}
			v, perr := strconv.ParseFloat(string(m[1]), 64)
			if perr != nil {
				return 0
			}
			return v
		}
		return read(reSpill), read(reRespill), nil
	}
}

func runSpillMemScenario(t *testing.T, dsn string, readSpill func() (float64, float64, error)) {
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()

	connected := false
	connectDeadline := time.Now().Add(180 * time.Second)
	for !connected && time.Now().Before(connectDeadline) {
		cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
		_, cerr := db.ExecContext(cctx, "select 1")
		ccancel()
		if cerr == nil {
			connected = true
			break
		}
		time.Sleep(time.Second)
	}
	require.True(t, connected, "no credential accepted within the connect deadline")

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	// The pinned session owns its connection from acquisition on: the close
	// is registered immediately and runs on every exit path, which also
	// guarantees the original session's termination independently of any
	// later session-scoped restore.
	defer conn.Close()

	exec := func(q string) error {
		t.Helper()
		lctx, lcancel := context.WithTimeout(ctx, 120*time.Second)
		defer lcancel()
		_, lerr := conn.ExecContext(lctx, q)
		return lerr
	}
	mustExec := func(q string) {
		t.Helper()
		require.NoError(t, exec(q), q)
	}
	readInt := func(q string) (int64, error) {
		t.Helper()
		lctx, lcancel := context.WithTimeout(ctx, 60*time.Second)
		defer lcancel()
		var v int64
		lerr := conn.QueryRowContext(lctx, q).Scan(&v)
		return v, lerr
	}
	mustReadInt := func(q string) int64 {
		v, lerr := readInt(q)
		require.NoError(t, lerr, q)
		return v
	}

	// Capture the real pre-call values before any write. The registered
	// finally restores the GLOBAL value and the own catalog through an
	// independent fresh connection and reads the restored value back; the
	// captured session values are restored on the pinned connection at the
	// normal tail, with closure guaranteed through the registered close.
	g0 := mustReadInt("select @@global.join_spill_mem")
	s1j := mustReadInt("select @@session.join_spill_mem")
	s1a := mustReadInt("select @@session.agg_spill_mem")
	s1s := mustReadInt("select @@session.sort_spill_mem")
	s1m := mustReadInt("select @@session.max_execution_time")
	t.Logf("captured pre-call g0=%d s1j=%d s1a=%d s1s=%d s1m=%d", g0, s1j, s1a, s1s, s1m)

	defer func() {
		fctx, fcancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer fcancel()
		fdb, ferr := sql.Open("mysql", dsn)
		if ferr != nil {
			t.Errorf("finally-restore open: %v", ferr)
			return
		}
		defer fdb.Close()
		fconn, ferr := fdb.Conn(fctx)
		if ferr != nil {
			t.Errorf("finally-restore conn: %v", ferr)
			return
		}
		defer fconn.Close()
		if _, lerr := fconn.ExecContext(fctx, "set role moadmin"); lerr != nil {
			t.Errorf("finally-restore role: %v", lerr)
		}
		if _, lerr := fconn.ExecContext(fctx, "set session optimizer_hints = \"\""); lerr != nil {
			t.Errorf("finally-restore optimizer_hints: %v", lerr)
		}
		if _, lerr := fconn.ExecContext(fctx, fmt.Sprintf("set global join_spill_mem = %d", g0)); lerr != nil {
			t.Errorf("finally-restore global join_spill_mem: %v", lerr)
		}
		var v int64
		if lerr := fconn.QueryRowContext(fctx, "select @@global.join_spill_mem").Scan(&v); lerr != nil {
			t.Errorf("finally-restore readback: %v", lerr)
		} else if v != g0 {
			t.Errorf("finally-restore readback got %d want %d", v, g0)
		}
		if _, lerr := fconn.ExecContext(fctx, "drop database if exists meow31836_spillmem"); lerr != nil {
			t.Errorf("finally-restore drop catalog: %v", lerr)
		}
	}()

	mustExec("set role moadmin")
	mustExec("create database meow31836_spillmem")
	mustExec("use meow31836_spillmem")
	mustExec("set @@max_dop = 1")
	mustExec("set session optimizer_hints = \"forceOneCN=1\"")

	// Canonical minimal physically spilling fixture (issue #26408): the
	// probe side repeats one key so every probe batch hits the single
	// matching build row; patched statistics shape the shuffle plan while
	// the persisted data stays small.
	mustExec("create table probe_keys (k bigint not null, payload bigint not null) cluster by k")
	mustExec("create table build_keys (k bigint not null, payload bigint not null) cluster by k")
	mustExec(fmt.Sprintf("insert into build_keys select result, result from generate_series(1, %d) g", meowSpillRows))
	mustExec(fmt.Sprintf("insert into probe_keys select 7, result from generate_series(1, %d) g", meowSpillRows))
	spillThreshold := meowSpillRows / 128
	patchStats := func(table string, tableCount int64) {
		t.Helper()
		stats := fmt.Sprintf(`{
		"table_cnt": %d,
		"block_number": 2048,
		"accurate_object_number": 128,
		"approx_object_number": 128,
		"ndv_map": {"k": %d},
		"min_val_map": {"k": 1},
		"max_val_map": {"k": 20000000},
		"shuffle_range_map": {
			"k": {
				"overlap": 0.1,
				"uniform": 1,
				"result": [1, 5000000, 10000000, 15000000, 20000000]
			}
		}
	}`, tableCount, tableCount)
		var patched float64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select table_cnt from table_stats(?, 'patch', ?) g",
			"meow31836_spillmem."+table, stats).Scan(&patched), table)
		require.Equal(t, float64(tableCount), patched, table)
	}
	patchStats("probe_keys", 20_000_000)
	patchStats("build_keys", 10_000_000)

	wantCnt := meowSpillRows
	wantProbeSum := float64(4096 * 4097 / 2)
	wantBuildSum := float64(4096 * 7)
	meowQuery := "select count(*), sum(mc_probe.payload), sum(mc_build.payload) from probe_keys mc_probe join build_keys mc_build on mc_probe.k = mc_build.k"
	// The predicate controls below mirror the parameterized where-clause
	// that failed the discriminator in the directed run: direct SQL and a
	// retained SQL PREPARE handle with the identical predicate both spill at
	// the low threshold, so the literal predicate preserves the shuffle
	// topology. Per the review directive, the genuine bound parameter now
	// sits in the topology-neutral projection of the binary-protocol
	// statement, and fresh-at-low spill remains asserted below.
	directPredicate := " where mc_probe.payload >= 0"
	directPredQuery := meowQuery + directPredicate
	binaryQuery := "select count(*) + ?, sum(mc_probe.payload), sum(mc_build.payload) from probe_keys mc_probe join build_keys mc_build on mc_probe.k = mc_build.k"

	runJoinStage := func(stage, query string, expectSpill bool) {
		t.Helper()
		s0, r0, lerr := readSpill()
		require.NoErrorf(t, lerr, "counter read before %s", stage)
		rows, qerr := conn.QueryContext(ctx, query)
		require.NoErrorf(t, qerr, "%s: %s", stage, query)
		defer rows.Close()
		require.True(t, rows.Next(), stage+": no rows")
		var cnt int64
		var ps, bs string
		require.NoError(t, rows.Scan(&cnt, &ps, &bs), stage)
		require.False(t, rows.Next(), stage+": more than one row")
		require.NoError(t, rows.Err(), stage)
		pv, perr := strconv.ParseFloat(ps, 64)
		require.NoError(t, perr, stage+" probe sum text "+ps)
		bv, berr := strconv.ParseFloat(bs, 64)
		require.NoError(t, berr, stage+" build sum text "+bs)
		s1, r1, lerr := readSpill()
		require.NoErrorf(t, lerr, "counter read after %s", stage)
		require.Equal(t, wantCnt, cnt, stage+" count")
		require.Equal(t, wantProbeSum, pv, stage+" probe sum")
		require.Equal(t, wantBuildSum, bv, stage+" build sum")
		if expectSpill {
			require.Greaterf(t, s1, s0, "%s: expected real hashbuild spill transitions", stage)
			require.Greaterf(t, r1, r0, "%s: expected real hashbuild re-spill transitions", stage)
		} else {
			require.Equal(t, s0, s1, stage+": unexpected spill transition")
			require.Equal(t, r0, r1, stage+": unexpected re-spill transition")
		}
	}

	// The identical SQL text keeps one cache identity across the ladder.
	mustExec("set session join_spill_mem = 0")
	planText, perr := testutils.QueryText(ctx, conn, "explain "+meowQuery)
	require.NoError(t, perr, "explain plan read")
	require.Contains(t, planText, "shuffle: range(", "plan shape must keep the shuffle hash join")
	predPlanText, perr := testutils.QueryText(ctx, conn, "explain "+directPredQuery)
	require.NoError(t, perr, "explain predicate plan read")
	t.Logf("predicate plan keeps shuffle: range: %v", strings.Contains(predPlanText, "shuffle: range("))
	runJoinStage("join-auto-first", meowQuery, false)
	runJoinStage("join-auto-warm-cache-hit", meowQuery, false)
	mustExec(fmt.Sprintf("set session join_spill_mem = %d", spillThreshold))
	require.Equal(t, spillThreshold, mustReadInt("select @@session.join_spill_mem"), "threshold readback")
	runJoinStage("join-auto-to-low", meowQuery, true)
	mustExec("set session join_spill_mem = 0")
	runJoinStage("join-low-to-auto", meowQuery, false)
	mustExec("set session join_spill_mem = 0")
	runJoinStage("join-equivalent-set", meowQuery, false)
	require.Error(t, exec("set session join_spill_mem = -120"), "invalid SET must be rejected")
	require.Equal(t, int64(0), mustReadInt("select @@session.join_spill_mem"),
		"invalid SET must preserve the session value")
	runJoinStage("join-after-invalid-set", meowQuery, false)

	// Retained SQL PREPARE handle of the unparameterized query.
	mustExec("prepare meow_pj from '" + meowQuery + "'")
	runJoinStage("prepare-handle-auto", "execute meow_pj", false)
	mustExec(fmt.Sprintf("set session join_spill_mem = %d", spillThreshold))
	runJoinStage("prepare-handle-low", "execute meow_pj", true)
	mustExec("set session join_spill_mem = 0")
	runJoinStage("prepare-handle-back-auto", "execute meow_pj", false)
	mustExec("deallocate prepare meow_pj")

	// Same-predicate controls plus the binary-protocol discriminator pair.
	// The retained text handle and the retained binary handle are prepared
	// while the threshold is auto; the fresh binary handle is prepared
	// after the low threshold is already set. If the fresh low handle
	// spills, generation building carries the threshold; if the retained
	// handle then fails to spill, the invalidation chain is the gap.
	mustExec("set session join_spill_mem = 0")
	runJoinStage("direct-predicate-auto", directPredQuery, false)
	mustExec("prepare meow_pp from '" + directPredQuery + "'")
	stmtOld, serr := conn.PrepareContext(ctx, binaryQuery)
	require.NoError(t, serr, "binary COM_STMT_PREPARE retained handle")
	defer stmtOld.Close()

	runBinaryStage := func(stage string, stmt *sql.Stmt, expectSpill bool) {
		t.Helper()
		s0, r0, lerr := readSpill()
		require.NoErrorf(t, lerr, "counter read before %s", stage)
		rows, qerr := stmt.QueryContext(ctx, int64(0))
		require.NoErrorf(t, qerr, "%s: binary execute", stage)
		defer rows.Close()
		require.True(t, rows.Next(), stage+": no rows")
		var cnt int64
		var ps, bs string
		require.NoError(t, rows.Scan(&cnt, &ps, &bs), stage)
		require.False(t, rows.Next(), stage+": more than one row")
		require.NoError(t, rows.Err(), stage)
		pv, perr := strconv.ParseFloat(ps, 64)
		require.NoError(t, perr, stage+" binary probe sum text "+ps)
		bv, berr := strconv.ParseFloat(bs, 64)
		require.NoError(t, berr, stage+" binary build sum text "+bs)
		s1, r1, lerr := readSpill()
		require.NoErrorf(t, lerr, "counter read after %s", stage)
		require.Equal(t, wantCnt, cnt, stage+" count")
		require.Equal(t, wantProbeSum, pv, stage+" probe sum")
		require.Equal(t, wantBuildSum, bv, stage+" build sum")
		if expectSpill {
			require.Greaterf(t, s1, s0, "%s: expected real hashbuild spill transitions", stage)
			require.Greaterf(t, r1, r0, "%s: expected real hashbuild re-spill transitions", stage)
		} else {
			require.Equal(t, s0, s1, stage+": unexpected spill transition")
			require.Equal(t, r0, r1, stage+": unexpected re-spill transition")
		}
	}

	runBinaryStage("binary-old-handle-auto", stmtOld, false)
	mustExec(fmt.Sprintf("set session join_spill_mem = %d", spillThreshold))
	require.Equal(t, spillThreshold, mustReadInt("select @@session.join_spill_mem"), "low threshold readback")
	runJoinStage("direct-predicate-low", directPredQuery, true)
	runJoinStage("prepare-predicate-low", "execute meow_pp", true)
	stmtFresh, serr := conn.PrepareContext(ctx, binaryQuery)
	require.NoError(t, serr, "binary COM_STMT_PREPARE fresh low handle")
	defer stmtFresh.Close()
	runBinaryStage("binary-fresh-low-first", stmtFresh, true)
	runBinaryStage("binary-fresh-low-second-reuse", stmtFresh, true)
	runBinaryStage("binary-old-handle-low", stmtOld, true)
	mustExec("set session join_spill_mem = 0")
	runBinaryStage("binary-old-handle-back-auto", stmtOld, false)
	runBinaryStage("binary-fresh-back-auto", stmtFresh, false)
	runJoinStage("prepare-predicate-back-auto", "execute meow_pp", false)
	mustExec("deallocate prepare meow_pp")

	// Aggregate and sort consumers: identical SQL across SET transitions
	// with exact results and ordering. Spill presence for these operators
	// is covered by the canonical CI BVT cases and is not asserted here.
	mustExec("create table a as select result id from generate_series(1, 16384) g")
	mustExec("create table b as select result id from generate_series(1, 32768) g")
	assertAgg := func(stage string) {
		t.Helper()
		rows, qerr := conn.QueryContext(ctx, "select id, count(*) c from a group by id order by id")
		require.NoError(t, qerr, stage)
		defer rows.Close()
		var n, prev int64
		for rows.Next() {
			var rid, c int64
			require.NoError(t, rows.Scan(&rid, &c), stage)
			require.Equal(t, prev+1, rid, stage+" not ordered from 1")
			require.Equal(t, int64(1), c, stage+" group count")
			prev = rid
			n++
		}
		require.NoError(t, rows.Err(), stage)
		require.Equal(t, int64(16384), n, stage+" rows")
	}
	mustExec("set session agg_spill_mem = 1073741824")
	assertAgg("agg-high-first")
	mustExec("set session agg_spill_mem = 100")
	assertAgg("agg-low")
	mustExec("set session agg_spill_mem = 1073741824")
	assertAgg("agg-back-high")
	assertSort := func(stage string) {
		t.Helper()
		rows, qerr := conn.QueryContext(ctx, "select id from b order by id desc")
		require.NoError(t, qerr, stage)
		defer rows.Close()
		var n int64
		prev := int64(32769)
		for rows.Next() {
			var id int64
			require.NoError(t, rows.Scan(&id), stage)
			require.Equal(t, prev-1, id, stage+" not descending")
			prev = id
			n++
		}
		require.NoError(t, rows.Err(), stage)
		require.Equal(t, int64(32768), n, stage+" rows")
	}
	mustExec("set session sort_spill_mem = 1073741824")
	assertSort("sort-high-first")
	mustExec("set session sort_spill_mem = 100")
	assertSort("sort-low")
	mustExec("set session sort_spill_mem = 1073741824")
	assertSort("sort-back-high")

	// GLOBAL changes apply to new logins only; the existing session keeps
	// its own value, invalid GLOBAL writes are rejected, and the captured
	// pre-call GLOBAL value is restored by the registered finally with
	// readback.
	for _, gv := range []int64{100, 250} {
		gid := fmt.Sprint(gv)
		mustExec(fmt.Sprintf("set global join_spill_mem = %d", gv))
		require.Equal(t, int64(0), mustReadInt("select @@session.join_spill_mem"),
			"GLOBAL change must not rewrite the existing session value ("+gid+")")
		require.Error(t, exec("set global join_spill_mem = -9"),
			"invalid GLOBAL SET must be rejected ("+gid+")")
		require.Equal(t, gv, mustReadInt("select @@global.join_spill_mem"),
			"invalid GLOBAL SET must not write the global value ("+gid+")")
		func(gv int64) {
			fresh, ferr := sql.Open("mysql", dsn)
			require.NoError(t, ferr, "new login open ("+gid+")")
			defer fresh.Close()
			fctx, fcancel := context.WithTimeout(ctx, 60*time.Second)
			defer fcancel()
			var nv int64
			require.NoError(t, fresh.QueryRowContext(fctx, "select @@session.join_spill_mem").Scan(&nv),
				"new login session read ("+gid+")")
			require.Equal(t, gv, nv, "new login must pick up the new GLOBAL default ("+gid+")")
		}(gv)
	}

	// Real server-side statement cancellation through the deterministic
	// max_execution_time path (issue #28516 oracle: "Query execution was
	// interrupted, maximum statement execution time exceeded"). The killed
	// statement cannot finish inside the budget because its projection
	// sleeps far longer, so a normal completion must fail this block. The
	// driver watcher is not used, so the connection must survive and stay
	// usable afterwards.
	mustExec(fmt.Sprintf("set session join_spill_mem = %d", spillThreshold))
	mustExec("set session max_execution_time = 2500")
	t.Logf("canceled-statement oracle: budget 2500ms vs projection sleep(15)")
	killedQuery := "select sleep(15), x.cnt from " +
		"(select count(*) cnt from probe_keys q join build_keys w on q.k = w.k) x"
	rows, qerr := conn.QueryContext(ctx, killedQuery)
	var cancelErr error
	if qerr != nil {
		// The deterministic server-side kill may already be reported at
		// statement open, before any response packet, carrying the same
		// real max_execution_time interruption oracle; it is never a
		// manufactured error. A normal completion leaves this nil and the
		// rows error nil, which fails the assertions below.
		cancelErr = qerr
	} else {
		defer rows.Close()
		for rows.Next() {
		}
		cancelErr = rows.Err()
	}
	require.Error(t, cancelErr, "the statement must be interrupted; a normal completion must fail this test")
	require.Contains(t, strings.ToLower(cancelErr.Error()), "interrupted",
		"the interruption oracle must match the documented max_execution_time error")
	require.Equal(t, spillThreshold, mustReadInt("select @@session.join_spill_mem"),
		"the session must survive the cancellation with its value intact")
	t.Logf("cancel error: %v", cancelErr)
	// Same-connection reuse with the full real-consumer oracle.
	runJoinStage("same-conn-reuse-after-cancel", meowQuery, true)

	// Original-session restore on the normal tail; every failure path is
	// still covered by the registered conn.Close plus the independent
	// fresh-context GLOBAL/catalog finally.
	restoreSessionVar := func(name string, value int64) {
		t.Helper()
		if _, lerr := conn.ExecContext(ctx, fmt.Sprintf("set session %s = %d", name, value)); lerr != nil {
			t.Logf("session restore of %s skipped: %v (registered close guarantees termination)", name, lerr)
		}
	}
	restoreSessionVar("max_execution_time", s1m)
	restoreSessionVar("join_spill_mem", s1j)
	restoreSessionVar("agg_spill_mem", s1a)
	restoreSessionVar("sort_spill_mem", s1s)
}
