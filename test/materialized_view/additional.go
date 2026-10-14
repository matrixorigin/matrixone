// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"sync"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

func typedAggregateOracle() {
	actual := rows("select k,n,nv,s,lo,hi,av from totals order by k")
	expected := [][]string{{"<NULL>", "1", "1", "7", "7", "7", "7"}, {"1", "2", "2", "30", "10", "20", "15"}, {"2", "2", "1", "-2", "-2", "-2", "-2"}}
	if len(actual) != len(expected) {
		panic("typed oracle group count")
	}
	for i := range actual {
		for j, value := range actual[i] {
			want := expected[i][j]
			if want == "<NULL>" {
				if value != want {
					panic("typed oracle NULL")
				}
				continue
			}
			got, ok := new(big.Rat).SetString(value)
			if !ok {
				panic("invalid numeric aggregate")
			}
			target, _ := new(big.Rat).SetString(want)
			if got.Cmp(target) != 0 {
				panic(fmt.Sprintf("typed aggregate oracle group %d column %d", i, j))
			}
		}
	}
	pass("independent typed aggregate oracle")
}

func runAdditionalCases(dsn string) {
	before := rows("select * from totals order by k")
	exec("refresh materialized view totals")
	exec("refresh materialized view totals")
	if !reflect.DeepEqual(before, rows("select * from totals order by k")) {
		panic("repeated refresh duplicated or changed rows")
	}
	pass("repeat refresh idempotent rows")
	exec("create materialized view if not exists totals refresh manual as select * from facts")
	if !reflect.DeepEqual(before, rows("select * from totals order by k")) {
		panic("IF NOT EXISTS replaced existing definition")
	}
	pass("IF NOT EXISTS no replacement")
	expectError("object kind collision", "create materialized view facts refresh manual as select * from dimension")
	expectError("duplicate output heading", "create materialized view duplicate_names refresh manual as select k as x,v as x from facts")
	expectError("minimum schedule interval", "create materialized view too_frequent refresh every interval 1 second as select * from facts")
	expectError("MV nesting", "create materialized view nested refresh manual as select * from totals")
	expectError("outer join", "create materialized view outer_join refresh manual as select * from facts f left join dimension d on f.k=d.k")
	expectError("random sample", "create materialized view sampled refresh manual as select sample(k,1 rows) from facts")
	exec("create temporary table temp_source(id int)")
	expectError("temporary source", "create materialized view temporary_mv refresh manual as select * from temp_source")
	exec("drop table temp_source")
	exec("create materialized view grouped_empty refresh manual as select k,count(*) as n from facts where id<0 group by k")
	exec("refresh materialized view grouped_empty")
	if len(rows("select * from grouped_empty")) != 0 {
		panic("grouped empty returned rows")
	}
	pass("grouped empty has zero rows")
	exec("drop materialized view grouped_empty")
	exec("create materialized view all_null refresh manual as select count(*) as n,count(v) as nv,sum(v) as s,avg(v) as av from facts where id=3")
	exec("refresh materialized view all_null")
	if !reflect.DeepEqual(rows("select n,nv,s,av from all_null"), [][]string{{"1", "0", "<NULL>", "<NULL>"}}) {
		panic("all NULL aggregate oracle")
	}
	pass("all NULL typed oracle")
	exec("drop materialized view all_null")
	exec("create materialized view projected refresh manual as select id,k,v from facts where v>0")
	exec("refresh materialized view projected")
	equal("projection and filter", "select id,k,v from facts where v>0 order by id", "select * from projected order by id")
	exec("drop materialized view projected")
	exec("update dimension set label='ONE' where k=1")
	exec("refresh materialized view joined")
	equal("dimension mutation", "select d.label,sum(f.v),count(*) from facts f inner join dimension d on f.k=d.k group by d.label order by d.label", "select * from joined order by label")
	exec("set sql_mode='PIPES_AS_CONCAT,NO_BACKSLASH_ESCAPES'")
	exec("create materialized view mode_capture refresh manual as select id,'a' || 'b' as label from facts")
	exec("set sql_mode=''")
	exec("refresh materialized view mode_capture")
	actual := rows("select label from mode_capture")
	for _, row := range actual {
		if row[0] != "ab" {
			panic("captured SQL mode changed")
		}
	}
	pass("captured SQL mode")
	exec("drop materialized view mode_capture")
	exec("set sql_mode='NO_BACKSLASH_ESCAPES'")
	exec("create table `slash\\source`(v int)")
	exec("insert into `slash\\source` values(17)")
	exec("create materialized view escaped_source refresh manual as select v from `slash\\source`")
	exec("set sql_mode=''")
	exec("refresh materialized view escaped_source")
	equal("captured context with escaped source identity", "select v from `slash\\source`", "select v from escaped_source")
	exec("drop materialized view escaped_source")
	exec("drop table `slash\\source`")
	exec("set time_zone='+00:00'")
	exec("create table tz_source(ts timestamp)")
	exec("insert into tz_source values('2025-12-31 20:00:00')")
	exec("set time_zone='+08:00'")
	exec("create materialized view tz_mv refresh manual as select year(ts) as y from tz_source")
	exec("set time_zone='+00:00'")
	exec("refresh materialized view tz_mv")
	if !reflect.DeepEqual(rows("select y from tz_mv"), [][]string{{"2026"}}) {
		panic("fixed-offset authoring timezone was lost")
	}
	pass("captured fixed-offset timezone")
	exec("drop materialized view tz_mv")
	exec("drop table tz_source")
	exec("begin")
	expectError("create MV in user transaction", "create materialized view in_txn refresh manual as select * from facts")
	exec("rollback")
	expectError("rename dependent source", "alter table facts rename to renamed_facts")
	expectError("foreign key into MV", "create table references_mv(k int,foreign key(k) references totals(k))")
	stmt, err := db.PrepareContext(ctx, "select k,s,n from totals order by k")
	if err != nil {
		panic(err)
	}
	func() {
		defer stmt.Close()
		want := rows("select k,sum(v),count(*) from facts group by k order by k")
		if !reflect.DeepEqual(preparedRows(stmt), want) {
			panic("prepared read differs from base query before refresh")
		}
		exec("refresh materialized view totals")
		if !reflect.DeepEqual(preparedRows(stmt), want) {
			panic("prepared read differs from base query after refresh")
		}
	}()
	pass("prepared read after refresh")
	concurrencyCases(dsn)
	privilegeCases(dsn)
	tenantIsolationCases(dsn)
}

func concurrencyCases(dsn string) {
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		panic(err)
	}
	config.DBName = "mv_validation"
	reader, err := openConfigured(config)
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	reader.SetMaxOpenConns(2)
	exec("create table atomic_source(id int primary key,v int)")
	exec("insert into atomic_source values(1,1),(2,1),(3,1),(4,1)")
	exec("create materialized view atomic_mv refresh manual as select count(*) as n,sum(v) as s,min(v) as lo,max(v) as hi from atomic_source")
	exec("refresh materialized view atomic_mv")
	started := make(chan struct{})
	stop := make(chan struct{})
	failures := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
			}
			var n, s, lo, hi int64
			err := reader.QueryRowContext(ctx, "select n,s,lo,hi from atomic_mv").Scan(&n, &s, &lo, &hi)
			if err != nil {
				failures <- err
				return
			}
			if n != 4 || lo != hi || s != 4*lo {
				failures <- fmt.Errorf("reader observed partial or mixed publication: %d %d %d %d", n, s, lo, hi)
				return
			}
		}
	})
	<-started
	for version := 2; version <= 6; version++ {
		exec(fmt.Sprintf("update atomic_source set v=%d", version))
		exec("refresh materialized view atomic_mv")
	}
	close(stop)
	wg.Wait()
	select {
	case err := <-failures:
		panic(err)
	default:
	}
	pass("concurrent readers see complete publications")
	// Separate connections issue refresh at once; conflicts can be rejected, but
	// the final committed view must still match a complete source snapshot.
	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Go(func() { <-start; _, err := reader.ExecContext(ctx, "refresh materialized view atomic_mv"); errs <- err })
	}
	close(start)
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
		}
	}
	if successes == 0 {
		panic("both concurrent refreshes failed")
	}
	equal("concurrent refresh publication", "select count(*),sum(v),min(v),max(v) from atomic_source", "select * from atomic_mv")
	exec("drop materialized view atomic_mv")
	exec("drop table atomic_source")
}

func privilegeCases(dsn string) {
	exec("create role mv_validation_reader")
	defer exec("drop role mv_validation_reader")
	exec("create user mv_validation_user identified by 'mv-validation-only'")
	defer exec("drop user mv_validation_user")
	exec("grant select on table mv_validation.totals to mv_validation_reader")
	exec("grant mv_validation_reader to mv_validation_user")
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		panic(err)
	}
	config.User = "sys:mv_validation_user:mv_validation_reader"
	config.Passwd = "mv-validation-only"
	config.DBName = "mv_validation"
	restricted, err := openConfigured(config)
	if err != nil {
		panic(err)
	}
	defer restricted.Close()
	restricted.SetMaxOpenConns(1)
	if err = restricted.PingContext(ctx); err != nil {
		panic(err)
	}
	if _, err = restricted.ExecContext(ctx, "select * from totals"); err == nil {
		panic("MV grant alone bypassed source privilege")
	}
	pass("MV grant requires source SELECT")
	exec("grant select on table mv_validation.facts to mv_validation_reader")
	want := rows("select * from totals order by k")
	if !reflect.DeepEqual(queryRows(restricted, "select * from totals order by k"), want) {
		panic("authorized read differs from owner read")
	}
	pass("MV plus source grant permits read")
	prepared, err := restricted.PrepareContext(ctx, "select * from totals order by k")
	if err != nil {
		panic(err)
	}
	defer prepared.Close()
	if !reflect.DeepEqual(preparedRows(prepared), want) {
		panic("authorized prepared read differs from owner read")
	}
	exec("revoke select on table mv_validation.facts from mv_validation_reader")
	r, err := prepared.QueryContext(ctx)
	if err == nil {
		defer r.Close()
		if len(decodeRows(r)) != 0 {
			panic("prepared read exposed rows after source ACL revocation")
		}
		err = r.Err()
	}
	requireServerRejection("prepared source ACL revocation", err)
	pass("prepared source ACL revocation")
	if _, err = restricted.ExecContext(ctx, "refresh materialized view totals"); err == nil {
		panic("nonowner refreshed MV")
	}
	pass("nonowner refresh rejected")
	if _, err = restricted.ExecContext(ctx, "drop materialized view totals"); err == nil {
		panic("nonowner dropped MV")
	}
	pass("nonowner drop rejected")
	result, err := restricted.QueryContext(ctx, "show materialized views")
	if err != nil {
		panic(err)
	}
	defer result.Close()
	if result.Next() {
		panic("SHOW exposed another owner's definitions")
	}
	if err = result.Err(); err != nil {
		panic(err)
	}
	pass("SHOW owner isolation")
}

// runRestartCheck verifies persisted objects without modifying their sources.
// Use with main's -keep flag and restart the service between the two invocations.
func runRestartCheck() {
	exec("use mv_validation")
	equal("restart persisted materialization", "select k,sum(v),count(*) from bench group by k order by k", "select k,s,n from bench_mv order by k")
	if len(rows("show materialized view refreshes bench_mv")) == 0 {
		panic("history lost on restart")
	}
	pass("restart persisted history")
	exec("refresh materialized view bench_mv")
	pass("refresh after restart")
}

// Keep imports/context checks explicit for the test harness's cancellation mode.
func timeoutRead(dsn string) {
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		panic(err)
	}
	config.DBName = "mv_validation"
	probe, err := openConfigured(config)
	if err != nil {
		panic(err)
	}
	defer probe.Close()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := probe.QueryContext(cancelled, "select * from totals")
	if err == nil {
		defer r.Close()
		decodeRows(r)
		err = r.Err()
	}
	if !errors.Is(err, context.Canceled) {
		panic("cancelled query did not terminate")
	}
	pass("cancelled client read")
}

func tenantIsolationCases(dsn string) {
	for _, name := range []string{"mv_validation_a", "mv_validation_b"} {
		exec("create account " + name + " admin_name='admin' identified by 'mv-validation-only'")
	}
	defer func() {
		for _, name := range []string{"mv_validation_a", "mv_validation_b"} {
			if _, err := db.ExecContext(ctx, "drop account if exists "+name); err != nil {
				fmt.Fprintln(os.Stderr, "tenant cleanup:", err)
			}
		}
	}()
	tenants := make([]*sql.DB, 0, 2)
	for i, name := range []string{"mv_validation_a", "mv_validation_b"} {
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			panic(err)
		}
		cfg.User = name + ":admin"
		cfg.Passwd = "mv-validation-only"
		cfg.DBName = ""
		connection, err := openConfigured(cfg)
		if err != nil {
			panic(err)
		}
		defer connection.Close()
		connection.SetMaxOpenConns(1)
		for _, q := range []string{"create database mv_validation", "use mv_validation", "set experimental_materialized_view=1", "create table facts(k int,v int)", fmt.Sprintf("insert into facts values(1,%d)", (i+1)*10), "create materialized view totals refresh manual as select k,sum(v) as s from facts group by k", "refresh materialized view totals"} {
			if _, err = connection.ExecContext(ctx, q); err != nil {
				panic(fmt.Errorf("tenant %s: %w", name, err))
			}
		}
		tenants = append(tenants, connection)
	}
	for i, connection := range tenants {
		var amount int
		if err := connection.QueryRowContext(ctx, "select s from totals").Scan(&amount); err != nil {
			panic(err)
		}
		if amount != (i+1)*10 {
			panic("tenant materialization crossed accounts")
		}
		if _, err := connection.ExecContext(ctx, "drop database mv_validation"); err == nil {
			panic("DROP DATABASE bypassed surviving MV restriction")
		} else {
			requireServerRejection("tenant DROP DATABASE RESTRICT", err)
		}
		if _, err := connection.ExecContext(ctx, "create materialized view periodic refresh every interval 1 minute as select k,sum(v) as s from facts group by k"); err != nil {
			panic(err)
		}
		if _, err := connection.ExecContext(ctx, "refresh materialized view periodic"); err != nil {
			panic(err)
		}
	}
	scheduledAccountLifecycleCases("mv_validation_a", dsn)
	pass("same-name multi-tenant source and MV isolation")
	pass("tenant DROP DATABASE restricts surviving MVs")
	// Surviving manual/populated and periodic MVs are intentionally retained.
	// Administrative deletion must retire them; explicit DROP MV masks the bug.
	for _, name := range []string{"mv_validation_a", "mv_validation_b"} {
		account := rows("select account_id from mo_catalog.mo_account where account_name='" + name + "'")[0][0]
		if name == "mv_validation_a" {
			accountRetirementRollbackCase(name, account, dsn)
		}
		exec("drop account " + name)
		for _, table := range []string{"mo_materialized_views", "mo_mv_dependencies", "mo_mv_refresh_runs", "mo_mv_account_state"} {
			if rows("select count(*) from mo_catalog." + table + " where account_id=" + account)[0][0] != "0" {
				panic("DROP ACCOUNT leaked " + table)
			}
		}
		if rows("select count(*) from mo_catalog.mo_tables where account_id=" + account)[0][0] != "0" {
			panic("DROP ACCOUNT leaked native objects")
		}
		if rows("select count(*) from mo_task.sys_async_task where task_metadata_executor=17 and json_extract(cast(task_metadata_context as varchar),'$.Key.Account')=" + account)[0][0] != "0" {
			panic("DROP ACCOUNT leaked queued MV tasks")
		}
	}
	pass("DROP ACCOUNT retires surviving populated and periodic MVs")
}

// NewConnector preserves account:user:role usernames; reparsing FormatDSN
// would interpret their first colon as the password separator.
func openConfigured(cfg *mysql.Config) (*sql.DB, error) {
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

func runNativeFailureAndCleanup() {
	// Own this fixture independently of the benchmark/restart deployment.
	exec("create database if not exists mv_validation_failure")
	exec("use mv_validation_failure")
	exec("set experimental_materialized_view=1")
	exec("drop materialized view if exists failure_mv")
	exec("drop table if exists failure_source")
	exec("create table failure_source(id int,v bigint)")
	exec("insert into failure_source values(1,0)")
	exec("create materialized view failure_mv refresh manual as select id,v+9223372036854775807 as v from failure_source")
	exec("refresh materialized view failure_mv")
	old := rows("select * from failure_mv")
	exec("update failure_source set v=1")
	expectError("native source-expression refresh failure", "refresh materialized view failure_mv")
	if !reflect.DeepEqual(old, rows("select * from failure_mv")) {
		panic("failed native refresh changed last-good result")
	}
	pass("native failed refresh preserves rows")
	runs := rows("show materialized view refreshes failure_mv")
	if len(runs) < 2 || runs[0][1] != "FAILED" {
		panic("native failed refresh history missing")
	}
	pass("native failed refresh diagnostic")
	exec("drop materialized view failure_mv")
	if len(rows("show materialized views")) != 0 {
		panic("drop left MV metadata")
	}
	pass("drop removes MV state")
	exec("drop database mv_validation_failure")
	pass("dependency cleanup permits database drop")
}

// Make the real reconciler due immediately and poll its durable terminal
// refresh history, rather than waiting through several full policy intervals;
// source rows and last-good metadata are the independent public oracles.
func scheduledAccountLifecycleCases(name string, dsn string) {
	account := rows("select account_id from mo_catalog.mo_account where account_name='" + name + "'")[0][0]
	definition := rows("select mv_id,epoch from mo_catalog.mo_materialized_views where account_id=" + account + " and name='periodic'")[0]
	exec("update mo_catalog.mo_materialized_views set next_due=date_add(current_timestamp(),interval 1 day) where account_id=" + account + " and name='periodic'")
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		panic(err)
	}
	cfg.User = name + ":admin"
	cfg.Passwd = "mv-validation-only"
	cfg.DBName = "mv_validation"
	for _, status := range []string{"suspend", "restricted"} {
		func() {
			// Own a fresh session for each status transition; suspension kills it.
			active, err := openConfigured(cfg)
			if err != nil {
				panic(err)
			}
			defer active.Close()
			active.SetMaxOpenConns(1)
			before := rows("select successful_sequence,last_run,source_physical,source_logical from mo_catalog.mo_materialized_views where account_id=" + account + " and name='periodic'")
			oldRows := queryRows(active, "select * from periodic order by k")
			if _, err := active.ExecContext(ctx, "update facts set v=v+1"); err != nil {
				panic(err)
			}
			exec("alter account " + name + " " + status)
			awaitScheduledMaterializedViewTask(account, definition, false)
			after := rows("select successful_sequence,last_run,source_physical,source_logical from mo_catalog.mo_materialized_views where account_id=" + account + " and name='periodic'")
			if !reflect.DeepEqual(before, after) {
				panic(status + " account published MV metadata")
			}
			exec("alter account " + name + " open")
			recovered, err := openConfigured(cfg)
			if err != nil {
				panic(err)
			}
			defer recovered.Close()
			recovered.SetMaxOpenConns(1)
			if !reflect.DeepEqual(oldRows, queryRows(recovered, "select * from periodic order by k")) {
				panic(status + " account changed last-good rows")
			}
			pass("scheduled refresh blocked for " + status + " account preserves rows and provenance")
			awaitScheduledMaterializedViewTask(account, definition, true)
			if !reflect.DeepEqual(queryRows(recovered, "select k,sum(v) as s from facts group by k order by k"), queryRows(recovered, "select * from periodic order by k")) {
				panic("reopened account refresh mismatch")
			}
			// Keep the normal reconciler away from the next status-boundary fixture.
			exec("update mo_catalog.mo_materialized_views set next_due=date_add(current_timestamp(),interval 1 day) where account_id=" + account + " and name='periodic'")
			pass("scheduled refresh resumes after reopening " + status + " account")
		}()
	}
}

func awaitScheduledMaterializedViewTask(account string, definition []string, success bool) {
	predicate := "account_id=" + account + " and mv_id=" + definition[0]
	before := rows("select requested_sequence from mo_catalog.mo_materialized_views where " + predicate)[0][0]
	// A system account may update its MV lifecycle catalog, but public sessions
	// cannot insert internal mo_task rows. Let the real reconciler own dispatch.
	exec("update mo_catalog.mo_materialized_views set next_due=current_timestamp() where " + predicate)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		result := rows("select status,run_id from mo_catalog.mo_mv_refresh_runs where " + predicate + " and sequence>" + before + " order by sequence desc limit 1")
		if len(result) == 1 && (result[0][0] == "SUCCEEDED" || result[0][0] == "FAILED") {
			if len(result[0][1]) != 64 {
				panic("scheduled run is not a typed task envelope")
			}
			queued := rows("select task_status from mo_task.sys_async_task where task_metadata_id='mv:" + result[0][1][:40] + "'")
			// FAILED history precedes bounded retries. Await queue completion
			// before changing account state. taskService.cleanupTasks deletes
			// only Completed rows: terminal typed history plus an absent queue
			// row also proves completion/GC in this exclusively owned fixture.
			// A present Created/Running row is never treated as terminal.
			if len(queued) > 1 {
				panic("duplicate typed task queue identity")
			}
			if len(queued) == 1 && queued[0][0] != "2" {
				select {
				case <-ctx.Done():
					panic(ctx.Err())
				case <-time.After(200 * time.Millisecond):
				}
				continue
			}
			if (result[0][0] == "SUCCEEDED") != success {
				panic(fmt.Sprintf("scheduled task expected success=%v got %v", success, result))
			}
			return
		}
		select {
		case <-ctx.Done():
			panic(ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	panic(fmt.Sprintf("account-lifecycle task did not terminate; state=%v history=%v queue=%v", rows("select epoch,interval_seconds,next_due,requested_sequence,successful_sequence from mo_catalog.mo_materialized_views where "+predicate), rows("select status,run_id,sequence from mo_catalog.mo_mv_refresh_runs where "+predicate+" order by sequence desc limit 3"), rows("select task_metadata_id,task_status,result_code from mo_task.sys_async_task where task_metadata_executor=17 and json_extract(cast(task_metadata_context as varchar),'$.Key.Account')="+account)))
}

// Corrupt only a task-owned retirement target name to force failure after the
// first MV has been dropped in the account transaction. This exercises real
// rollback without a product hook, sleeps or scheduler luck.
func accountRetirementRollbackCase(name, account, dsn string) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		panic(err)
	}
	cfg.User = name + ":admin"
	cfg.Passwd = "mv-validation-only"
	cfg.DBName = "mv_validation"
	connection, err := openConfigured(cfg)
	if err != nil {
		panic(err)
	}
	defer connection.Close()
	connection.SetMaxOpenConns(1)
	beforeRows := queryRows(connection, "select * from totals order by k")
	beforePeriodic := queryRows(connection, "select * from periodic order by k")
	exec("update mo_catalog.mo_materialized_views set name='retirement_missing' where account_id=" + account + " and name='periodic'")
	defer exec("update mo_catalog.mo_materialized_views set name='periodic' where account_id=" + account + " and name='retirement_missing'")
	query := "select mv_id,epoch,interval_seconds,next_due,successful_sequence,last_run from mo_catalog.mo_materialized_views where account_id=" + account + " order by mv_id"
	before := rows(query)
	_, err = db.ExecContext(ctx, "drop account "+name)
	requireServerRejection("account retirement injected missing target", err)
	if !reflect.DeepEqual(before, rows(query)) {
		panic("failed account deletion changed MV policy/state")
	}
	if !reflect.DeepEqual(beforeRows, queryRows(connection, "select * from totals order by k")) || !reflect.DeepEqual(beforePeriodic, queryRows(connection, "select * from periodic order by k")) {
		panic("failed account deletion lost native MV rows")
	}
	if rows("select count(*) from mo_catalog.mo_account where account_id=" + account)[0][0] != "1" {
		panic("failed account deletion removed account")
	}
	pass("DROP ACCOUNT failure rolls back retired objects, policy and metadata")
}
