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

package issues

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// Each callback gets only the remaining budget for its own phase. Time spent
// preparing the second group never consumes query time, and vice versa.
type ivfPhaseBudget struct {
	remaining time.Duration
	now       func() time.Time
}

func (b *ivfPhaseBudget) run(fn func(context.Context) error) error {
	if b.remaining <= 0 {
		return context.DeadlineExceeded
	}
	started := b.now()
	ctx, cancel := context.WithTimeout(context.Background(), b.remaining)
	defer cancel()
	err := fn(ctx)
	elapsed := b.now().Sub(started)
	b.remaining -= elapsed
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && b.remaining <= 0 {
		err = context.DeadlineExceeded
	}
	return err
}

type ivfTestPhase struct{ prepare, query func(context.Context) error }

func runIVFTestPhases(phases []ivfTestPhase, cleanup func(context.Context) error) (err error) {
	// A deferred owner also runs on a test Goexit or panic; assertions are not
	// used in preparation, so its normal failures return through this owner.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err = errors.Join(err, cleanup(ctx))
	}()
	prepare := ivfPhaseBudget{15 * time.Minute, time.Now}
	query := ivfPhaseBudget{5 * time.Minute, time.Now}
	for _, phase := range phases {
		if err = prepare.run(phase.prepare); err != nil {
			return err
		}
		if err = query.run(phase.query); err != nil {
			return err
		}
	}
	return nil
}
func TestIssue28378PhaseBudgetsAreIndependent(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }
	prepare := ivfPhaseBudget{15 * time.Minute, clock}
	query := ivfPhaseBudget{5 * time.Minute, clock}
	consume := func(d time.Duration) func(context.Context) error {
		return func(context.Context) error { now = now.Add(d); return nil }
	}
	require.NoError(t, prepare.run(consume(10*time.Minute)))
	require.NoError(t, query.run(consume(2*time.Minute)))
	require.Equal(t, 5*time.Minute, prepare.remaining)
	require.NoError(t, prepare.run(consume(4*time.Minute)))
	require.Equal(t, 3*time.Minute, query.remaining)
	require.NoError(t, query.run(consume(2*time.Minute)))
	require.ErrorIs(t, prepare.run(consume(time.Minute)), context.DeadlineExceeded)
	called := false
	require.ErrorIs(t, prepare.run(func(context.Context) error { called = true; return nil }), context.DeadlineExceeded)
	require.False(t, called)
	require.Equal(t, time.Minute, query.remaining)
}
func TestIssue28378PreparationFailureSkipsQueryAndCleansOnce(t *testing.T) {
	prepareErr := errors.New("seed insert failed")
	cleanupErr := errors.New("cleanup failed")
	queries, cleanups := 0, 0
	err := runIVFTestPhases([]ivfTestPhase{{
		prepare: func(context.Context) error { return prepareErr },
		query:   func(context.Context) error { queries++; return nil },
	}}, func(ctx context.Context) error {
		cleanups++
		require.NoError(t, ctx.Err())
		return cleanupErr
	})
	require.ErrorIs(t, err, prepareErr)
	require.ErrorIs(t, err, cleanupErr)
	require.Zero(t, queries)
	require.Equal(t, 1, cleanups)
}

type ivfRunState struct {
	workers                  sync.WaitGroup
	workerCtx                context.Context
	cancelWorkers            context.CancelFunc
	databases                []*sql.DB
	conns                    []*sql.Conn
	tx                       *sql.Tx
	cleanupCalls, queryCalls int
}
type ivfCase struct {
	name, col, fn, op, vector string
	desc                      bool
}

func TestIssue28378IVFFlatRemoteLifecycle(t *testing.T) {
	started := time.Now()
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		t.Logf("IVF_CLUSTER_READY elapsed=%s (outside preparation budget)", time.Since(started))
		if !t.Run("preparation_failure", func(t *testing.T) {
			injected := errors.New("injected after first successful seed batch")
			state := new(ivfRunState)
			workerExited := make(chan struct{})
			err := runIssue28378IVF(t, cluster, state, func(ctx context.Context, tx *sql.Tx, state *ivfRunState) error {
				var n int
				if err := tx.QueryRowContext(ctx, "select count(*) from source_vectors").Scan(&n); err != nil {
					return err
				}
				if n != 256 {
					return fmt.Errorf("partial transaction rows=%d, want 256", n)
				}
				// Start a real connection owner during partial setup. The runner must
				// cancel and join it before closing the databases and issuing cleanup DDL.
				ready := make(chan error, 1)
				state.workers.Add(1)
				go func() {
					defer state.workers.Done()
					defer close(workerExited)
					conn, err := state.databases[1].Conn(state.workerCtx)
					if err != nil {
						ready <- err
						return
					}
					defer conn.Close()
					_, err = conn.ExecContext(state.workerCtx, "select 1")
					ready <- err
					if err == nil {
						<-state.workerCtx.Done()
					}
				}()
				select {
				case err := <-ready:
					if err != nil {
						return err
					}
				case <-ctx.Done():
					return ctx.Err()
				}
				return injected
			})
			require.ErrorIs(t, err, injected)
			require.EqualError(t, err, injected.Error(), "cleanup must not add another failure")
			require.Zero(t, state.queryCalls)
			require.Equal(t, 1, state.cleanupCalls)
			require.ErrorIs(t, state.tx.Rollback(), sql.ErrTxDone)
			select {
			case <-workerExited:
			default:
				t.Fatal("preparation worker survived cleanup")
			}
		}) {
			return
		}
		t.Run("queries", func(t *testing.T) { require.NoError(t, runIssue28378IVF(t, cluster, new(ivfRunState), nil)) })
	})
}

func runIssue28378IVF(t *testing.T, cluster embed.Cluster, state *ivfRunState,
	afterInsert func(context.Context, *sql.Tx, *ivfRunState) error) error {
	state.workerCtx, state.cancelWorkers = context.WithCancel(context.Background())
	name := strings.ToLower(testutils.GetDatabaseName(t))
	var ctx context.Context
	var conns []*sql.Conn
	var databases []*sql.DB
	var seedTx *sql.Tx
	var generatedTime, insertedTime, commitTime time.Duration
	var insertedRows int
	var queryVector, normalizedQuery string
	var cases []ivfCase
	created := false
	execSQL := func(statement string) error { _, err := conns[0].ExecContext(ctx, statement); return err }
	exec := func(t *testing.T, conn *sql.Conn, statement string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	prepareIndex := func(tc ivfCase) error {
		started := time.Now()
		defer func() { t.Logf("IVF_INDEX name=%s elapsed=%s", tc.name, time.Since(started)) }()
		for _, sql := range []string{
			"create table " + tc.name + "(id bigint primary key, v vecf32(128))",
			"insert into " + tc.name + " select id," + tc.col + " from source_vectors",
			"create index ivf_idx using ivfflat on " + tc.name + "(v) lists=16 op_type '" + tc.op + "'",
			"select mo_ctl('dn','flush','" + name + "." + tc.name + "')",
		} {
			if err := execSQL(sql); err != nil {
				return err
			}
		}
		return nil
	}
	prepareSource := func(phaseCtx context.Context) error {
		ctx = phaseCtx
		for i := 0; i < 2; i++ {
			cn, err := cluster.GetCNService(i)
			if err != nil {
				return err
			}
			inventory := clusterservice.GetMOCluster(cn.ServiceID())
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				inventory.ForceRefresh(true)
				n := 0
				inventory.GetCNService(clusterservice.NewSelector(), func(metadata.CNService) bool { n++; return true })
				if n >= 2 {
					break
				}
				timer := time.NewTimer(200 * time.Millisecond)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				}
			}
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			if err != nil {
				return err
			}
			databases = append(databases, db)
			state.databases = databases
			conn, err := db.Conn(ctx)
			if err != nil {
				return err
			}
			conns = append(conns, conn)
			state.conns = conns
		}
		if err := execSQL("create database `" + name + "`"); err != nil {
			return err
		}
		created = true
		for _, conn := range conns {
			for _, sql := range []string{"use `" + name + "`", "set experimental_ivf_index=1", "set max_dop=16"} {
				if _, err := conn.ExecContext(ctx, sql); err != nil {
					return err
				}
			}
		}
		if err := execSQL("create table source_vectors(id bigint primary key, v vecf32(128), n vecf32(128))"); err != nil {
			return err
		}
		var err error
		seedTx, err = conns[0].BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		state.tx = seedTx
		rng := rand.New(rand.NewSource(28378))
		for start := 0; start < 65536; start += 256 {
			generationStarted := time.Now()
			values := make([]string, 0, 256)
			for id := start; id < start+256; id++ {
				v := make([]float64, 128)
				var squared float64
				for d := range v {
					v[d] = float64(float32(rng.Float64()*2 - 1))
					squared += v[d] * v[d]
				}
				encode := func(normalize bool) string {
					parts := make([]string, len(v))
					for d, x := range v {
						if normalize {
							x /= math.Sqrt(squared)
						}
						parts[d] = strconv.FormatFloat(float64(float32(x)), 'g', -1, 32)
					}
					return "'[" + strings.Join(parts, ",") + "]'"
				}
				raw, normalized := encode(false), encode(true)
				if id == 0 {
					queryVector, normalizedQuery = raw, normalized
				}
				values = append(values, fmt.Sprintf("(%d,%s,%s)", id, raw, normalized))
			}
			generatedTime += time.Since(generationStarted)
			insertStarted := time.Now()
			_, err := seedTx.ExecContext(ctx, "insert into source_vectors values "+strings.Join(values, ","))
			insertedTime += time.Since(insertStarted)
			if err != nil {
				return err
			}
			insertedRows += 256
			if afterInsert != nil {
				if err := afterInsert(ctx, seedTx, state); err != nil {
					return err
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		commitStarted := time.Now()
		err = seedTx.Commit()
		commitTime = time.Since(commitStarted)
		if err != nil {
			return err
		}

		cases = []ivfCase{
			{"l2", "v", "l2_distance", "vector_l2_ops", queryVector, false},
			{"cosine", "v", "cosine_distance", "vector_cosine_ops", queryVector, false},
			{"normalized_l2", "n", "l2_distance", "vector_l2_ops", normalizedQuery, false},
			{"ip_normalized", "v", "inner_product", "vector_ip_ops", "normalize_l2(" + queryVector + ")", false},
		}
		for _, tc := range cases[:3] {
			if err := prepareIndex(tc); err != nil {
				return err
			}
		}
		return nil
	}
	type result struct {
		id       int64
		distance float64
	}
	query := func(t *testing.T, conn *sql.Conn, statement string, desc bool) []result {
		t.Helper()
		rows, err := conn.QueryContext(ctx, statement)
		require.NoError(t, err)
		defer rows.Close()
		var found []result
		seen := make(map[int64]bool)
		for rows.Next() {
			var row result
			require.NoError(t, rows.Scan(&row.id, &row.distance))
			require.GreaterOrEqual(t, row.id, int64(0))
			require.Less(t, row.id, int64(65536))
			require.False(t, seen[row.id], "duplicate vector id")
			seen[row.id] = true
			require.False(t, math.IsNaN(row.distance) || math.IsInf(row.distance, 0))
			if len(found) > 0 {
				if desc {
					require.LessOrEqual(t, row.distance, found[len(found)-1].distance)
				} else {
					require.GreaterOrEqual(t, row.distance, found[len(found)-1].distance)
				}
			}
			found = append(found, row)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		return found
	}
	remoteCalls := func() uint64 {
		metric := new(dto.Metric)
		require.NoError(t, metricv2.PipelineServerDurationHistogram.Write(metric))
		return metric.GetHistogram().GetSampleCount()
	}
	runCase := func(tc ivfCase) bool {
		return t.Run(tc.name, func(t *testing.T) {
			caseStarted := time.Now()
			defer func() { t.Logf("IVF_QUERY_CASE name=%s elapsed=%s", tc.name, time.Since(caseStarted)) }()
			direction := "asc"
			if tc.desc {
				direction = "desc"
			}
			statement := "select id," + tc.fn + "(v," + tc.vector + ") from " + tc.name + " order by " + tc.fn + "(v," + tc.vector + ") " + direction + " limit 10"
			for coordinator, conn := range conns {
				expression := tc.fn + "(v," + tc.vector + ")"
				statement = "select id," + expression + " from " + tc.name + " order by " + expression + " " + direction + " limit 10"
				plan := queryJoinSpillText(t, ctx, conn, "explain "+statement)
				require.Contains(t, plan, "Vector Index: ivf_idx", "must use the IVF reader")
				exec(t, conn, "set probe_limit=16")
				before := remoteCalls()
				got := query(t, conn, statement, tc.desc)
				require.Len(t, got, 10)
				require.Greater(t, remoteCalls(), before, "coordinator %d must execute a remote pipeline", coordinator)
				wantExpression := tc.fn + "(" + tc.col + "," + tc.vector + ")"
				want := query(t, conn, "select id,"+wantExpression+" from source_vectors order by "+wantExpression+" "+direction+" limit 10", tc.desc)
				require.Len(t, want, 10)
				for i := range want {
					require.Equal(t, want[i].id, got[i].id)
					require.InDelta(t, want[i].distance, got[i].distance, 1e-5)
				}
				exec(t, conn, "set probe_limit=5")
				for _, predicate := range []string{"", " where id < 0", ""} {
					got := query(t, conn, "select id,"+expression+" from "+tc.name+predicate+" order by "+expression+" "+direction+" limit 10", tc.desc)
					if predicate == "" {
						require.Len(t, got, 10)
					} else {
						require.Empty(t, got)
					}
				}

			}
			// Preserve the incident's SELECT id shape under bounded concurrent
			// sessions. A single sql.Conn would serialize the workload.
			completed := make(chan error, 8)
			for worker := 0; worker < 8; worker++ {
				state.workers.Add(1)
				go func(worker int) {
					defer state.workers.Done()
					completed <- func() error {
						conn, err := databases[worker%2].Conn(ctx)
						if err != nil {
							return err
						}
						defer conn.Close()
						for _, setting := range []string{"use `" + name + "`", "set probe_limit=5", "set max_dop=16", "set experimental_ivf_index=1"} {
							if _, err := conn.ExecContext(ctx, setting); err != nil {
								return err
							}
						}
						statement := "select id from " + tc.name + " order by " + tc.fn + "(v," + tc.vector + ") " + direction + " limit 10"
						for iteration := 0; iteration < 20; iteration++ {
							rows, err := conn.QueryContext(ctx, statement)
							if err != nil {
								return err
							}
							err = func() (runErr error) {
								defer func() {
									if closeErr := rows.Close(); runErr == nil {
										runErr = closeErr
									}
								}()
								count := 0
								seen := make(map[int64]bool)
								for rows.Next() {
									var id int64
									if err := rows.Scan(&id); err != nil {
										return err
									}
									if id < 0 || id >= 65536 || seen[id] {
										return fmt.Errorf("invalid vector id %d", id)
									}
									seen[id] = true
									count++
								}
								if err := rows.Err(); err != nil {
									return err
								}
								if count != 10 {
									return fmt.Errorf("got %d rows, want 10", count)
								}
								return nil
							}()
							if err != nil {
								return err
							}
						}
						return nil
					}()
				}(worker)
			}
			// Join every worker before assertions/DDL cleanup, including failures.
			var firstErr error
			for worker := 0; worker < 8; worker++ {
				if err := <-completed; err != nil && firstErr == nil {
					firstErr = err
				}
			}
			t.Logf("IVF_WORKERS_JOINED distance=%s count=8", tc.name)
			require.NoError(t, firstErr)
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()
			cancelledRows, err := conns[0].QueryContext(cancelCtx, statement)
			if cancelledRows != nil {
				defer cancelledRows.Close()
				if rowsErr := cancelledRows.Err(); rowsErr != nil {
					require.ErrorIs(t, rowsErr, context.Canceled)
				}
			}
			require.ErrorIs(t, err, context.Canceled)
			// This checks cancellation before SQL admission, not cancellation
			// of an active remote pipeline. Run again after all workers joined.
			followup := query(t, conns[0], statement, tc.desc)
			require.Len(t, followup, 10)
		})
	}

	queryGroup := func(group []ivfCase, phaseCtx context.Context) error {
		ctx = phaseCtx
		state.queryCalls++
		for _, tc := range group {
			if !runCase(tc) {
				return fmt.Errorf("IVF query assertions failed: %s", tc.name)
			}
			if err := execSQL("drop table " + tc.name); err != nil {
				return err
			}
		}
		return nil
	}
	cleanup := func(cleanupCtx context.Context) error {
		state.cleanupCalls++
		state.cancelWorkers()
		var errs []error
		if seedTx != nil {
			if err := seedTx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				errs = append(errs, err)
			}
		}
		state.workers.Wait()
		if created {
			// Use a fresh connection because cancellation may poison the setup one.
			_, err := databases[0].ExecContext(cleanupCtx, "drop database `"+name+"`")
			errs = append(errs, err)
			if err == nil {
				var n int
				err = databases[0].QueryRowContext(cleanupCtx, "select count(*) from mo_catalog.mo_database where datname = ?", name).Scan(&n)
				errs = append(errs, err)
				if err == nil && n != 0 {
					errs = append(errs, fmt.Errorf("database survived cleanup: %s", name))
				}
			}
		}
		for _, conn := range conns {
			errs = append(errs, conn.Close())
		}
		for _, db := range databases {
			errs = append(errs, db.Close())
		}
		t.Logf("IVF_CLEANUP inserted_rows=%d generation=%s insertion=%s commit=%s cleanup_calls=%d", insertedRows, generatedTime, insertedTime, commitTime, state.cleanupCalls)
		return errors.Join(errs...)
	}
	timed := func(name string, fn func(context.Context) error) func(context.Context) error {
		return func(ctx context.Context) error {
			started := time.Now()
			defer func() { t.Logf("IVF_PHASE name=%s elapsed=%s", name, time.Since(started)) }()
			return fn(ctx)
		}
	}
	return runIVFTestPhases([]ivfTestPhase{
		{prepare: timed("prepare_primary", prepareSource), query: timed("query_primary", func(ctx context.Context) error { return queryGroup(cases[:3], ctx) })},
		{prepare: timed("prepare_ip", func(phaseCtx context.Context) error { ctx = phaseCtx; return prepareIndex(cases[3]) }), query: timed("query_ip", func(ctx context.Context) error { return queryGroup(cases[3:], ctx) })},
	}, cleanup)
}
