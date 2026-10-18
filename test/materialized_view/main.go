// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
// Runs independent SQL correctness checks and a paired warm-query benchmark.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

type report struct {
	Experiments      []pairedExperiment `json:"experiments"`
	Started          time.Time          `json:"started"`
	Cases            []string           `json:"passed_cases"`
	Rows             int                `json:"source_rows"`
	RefreshMS        float64            `json:"refresh_ms"`
	BaselineMS       []float64          `json:"baseline_ms"`
	MaterializedMS   []float64          `json:"materialized_ms"`
	Speedup          float64            `json:"median_speedup"`
	RawPlan          [][]string         `json:"raw_plan"`
	MVPlan           [][]string         `json:"materialized_plan"`
	MaterializedRows int                `json:"materialized_rows"`
	Error            string             `json:"error,omitempty"`
}

var evidence report
var db *sql.DB
var ctx context.Context

func exec(q string) {
	if _, err := db.ExecContext(ctx, q); err != nil {
		panic(fmt.Errorf("%s: %w", q, err))
	}
}
func rows(q string) [][]string {
	return queryRows(db, q)
}
func queryRows(connection *sql.DB, q string) [][]string {
	r, err := connection.QueryContext(ctx, q)
	if err != nil {
		panic(fmt.Errorf("%s: %w", q, err))
	}
	defer r.Close()
	out := decodeRows(r)
	if err = r.Err(); err != nil {
		panic(err)
	}
	return out
}
func preparedRows(stmt *sql.Stmt) [][]string {
	r, err := stmt.QueryContext(ctx)
	if err != nil {
		panic(err)
	}
	defer r.Close()
	out := decodeRows(r)
	if err = r.Err(); err != nil {
		panic(err)
	}
	return out
}

// Callers own closing the result and checking its terminal stream error.
func decodeRows(r *sql.Rows) [][]string {
	cols, err := r.Columns()
	if err != nil {
		panic(err)
	}
	out := make([][]string, 0)
	for r.Next() {
		values := make([]sql.RawBytes, len(cols))
		ptr := make([]any, len(cols))
		for i := range values {
			ptr[i] = &values[i]
		}
		if err = r.Scan(ptr...); err != nil {
			panic(err)
		}
		line := make([]string, len(cols))
		for i, v := range values {
			if v == nil {
				line[i] = "<NULL>"
			} else {
				line[i] = string(v)
			}
		}
		out = append(out, line)
	}
	return out
}
func expectError(name, q string) {
	_, err := db.ExecContext(ctx, q)
	requireServerRejection(name, err)
	pass(name)
}
func requireServerRejection(name string, err error) {
	if err == nil {
		panic(name + ": expected rejection")
	}
	var serverError *mysql.MySQLError
	if !errors.As(err, &serverError) || serverError.Number == 1046 {
		panic(fmt.Errorf("%s: unexpected failure: %w", name, err))
	}
}
func equal(name, a, b string) {
	if !reflect.DeepEqual(rows(a), rows(b)) {
		panic(name + ": result mismatch")
	}
	pass(name)
}
func pass(name string) { evidence.Cases = append(evidence.Cases, name); fmt.Println("PASS", name) }
func measure(q string) float64 {
	start := time.Now()
	rows(q)
	return float64(time.Since(start).Microseconds()) / 1000
}
func median(a []float64) float64 {
	b := append([]float64(nil), a...)
	sort.Float64s(b)
	if len(b)%2 == 0 {
		return (b[len(b)/2-1] + b[len(b)/2]) / 2
	}
	return b[len(b)/2]
}
func main() {
	dsn := flag.String("dsn", "dump:111@tcp(127.0.0.1:6001)/?timeout=5s&readTimeout=330s&writeTimeout=330s", "isolated local cluster DSN")
	output := flag.String("output", "/workspace/mv-build/sql-results.json", "evidence JSON")
	n := flag.Int("rows", 1000000, "benchmark source rows")
	catalogMissing := flag.Bool("catalog-missing", false, "isolated pre-MV kernel fixture only: validate ordinary DDL without catalogs")
	consumerCheck := flag.Bool("consumer-check", false, "verify MV native loading, source DELETE and SHOW export consumers")
	tenantCheck := flag.Bool("tenant-check", false, "run isolated account lifecycle contracts only")
	functional := flag.Bool("functional", false, "run SQL contracts without performance experiments")
	keep := flag.Bool("keep", false, "retain test database for restart checks")
	edge := flag.Bool("edge-check", false, "verify native failure rollback and clean retained fixtures")
	restart := flag.Bool("restart-check", false, "check a retained database after restarting service")
	scheduled := flag.Bool("scheduled", true, "verify background refresh (up to 120 seconds)")
	secondDSN := flag.String("second-dsn", "", "run cross-CN checks using a second SQL endpoint instead of the main suite")
	flag.Parse()
	evidence.Started = time.Now().UTC()
	evidence.Rows = *n
	if *functional || *tenantCheck {
		evidence.Rows = 0 // No generated benchmark source is created in this mode.
	}
	defer func() {
		if failure := recover(); failure != nil {
			evidence.Error = fmt.Sprint(failure)
		}
		data, _ := json.MarshalIndent(evidence, "", "  ")
		if err := os.WriteFile(*output, data, 0644); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		if evidence.Error != "" {
			fmt.Fprintln(os.Stderr, evidence.Error)
			os.Exit(1)
		}
	}()
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	var err error
	db, err = sql.Open("mysql", *dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	if err = db.PingContext(ctx); err != nil {
		panic(err)
	}
	if *catalogMissing {
		evidence.Rows = 0
		runMissingMaterializedViewCatalog()
		return
	}
	if *consumerCheck {
		evidence.Rows = 0
		runConsumerCases()
		return
	}
	if *tenantCheck {
		tenantIsolationCases(*dsn)
		return
	}
	if *secondDSN != "" {
		runMultiCN(*secondDSN)
		return
	}
	if *edge {
		runNativeFailureAndCleanup()
		return
	}
	if *restart {
		runRestartCheck()
		return
	}
	exec("create database if not exists mv_validation")
	exec("use mv_validation")
	// Pin one connection because USE and session variables are session-local.
	db.SetMaxOpenConns(1)
	for _, name := range []string{"bench_mv", "joined", "empty_total", "totals", "unsupported", "gated", "escaped_source"} {
		exec("drop materialized view if exists " + name)
	}
	exec("drop table if exists `slash\\source`")
	for _, name := range []string{"facts", "dimension", "bench"} {
		exec("drop table if exists " + name)
	}
	exec("set experimental_materialized_view=0")
	exec("create table facts (id int primary key,k int,v decimal(18,2))")
	exec("insert into facts values(1,1,10),(2,1,20),(3,2,null),(4,null,7),(5,2,-2)")
	expectError("experimental gate", "create materialized view gated refresh manual as select * from facts")
	exec("set experimental_materialized_view=1")
	const aggregate = "select k,count(*) as n,count(v) as nv,sum(v) as s,min(v) as lo,max(v) as hi,avg(v) as av from facts group by k"
	exec("create materialized view totals refresh manual as " + aggregate)
	expectError("deferred population", "select * from totals")
	exec("refresh materialized view totals")
	equal("NULL and aggregate semantics", aggregate+" order by k", "select * from totals order by k")
	typedAggregateOracle()
	saved := rows("select * from totals order by k")
	exec("insert into facts values(6,1,90),(7,3,12)")
	exec("update facts set v=25 where id=1")
	exec("delete from facts where id=2")
	if !reflect.DeepEqual(saved, rows("select * from totals order by k")) {
		panic("manual view changed before refresh")
	}
	pass("last committed materialization")
	exec("refresh materialized view totals")
	equal("insert update delete refresh", aggregate+" order by k", "select * from totals order by k")
	for _, q := range []string{"insert into totals(k) values(4)", "update totals set k=4", "delete from totals", "truncate table totals", "replace into totals(k) values(4)", "alter table totals add x int", "drop table totals"} {
		expectError("read only: "+q, q)
	}
	for _, q := range []string{"alter table facts add x int", "truncate table facts", "drop table facts", "drop database mv_validation"} {
		expectError("dependency restrict: "+q, q)
	}
	if !reflect.DeepEqual(rows("select database()"), [][]string{{"mv_validation"}}) {
		panic("failed dependency DROP changed current database")
	}
	pass("failed DROP preserves current database")
	for _, source := range []string{"select rand() from facts", "select now() from facts", "select distinct k from facts", "select count(distinct k) from facts", "select * from facts limit 1", "select row_number() over(order by id) from facts", "select * from totals"} {
		expectError("unsupported definition: "+source, "create materialized view unsupported refresh manual as "+source)
	}
	exec("create table dimension(k int primary key,label varchar(20))")
	exec("insert into dimension values(1,'one'),(2,'two')")
	const joined = "select d.label,sum(f.v) as s,count(*) as n from facts f inner join dimension d on f.k=d.k group by d.label"
	exec("create materialized view joined refresh manual as " + joined)
	exec("refresh materialized view joined")
	equal("inner join refresh", joined+" order by label", "select * from joined order by label")
	exec("create materialized view empty_total refresh manual as select count(*) as n,sum(v) as s from facts where id<0")
	exec("refresh materialized view empty_total")
	equal("empty aggregate", "select count(*),sum(v) from facts where id<0", "select * from empty_total")
	exec("begin")
	expectError("refresh in user transaction", "refresh materialized view totals")
	exec("rollback")
	exec("alter materialized view totals refresh every interval 1 minute")
	creation := rows("show create materialized view totals")
	if len(creation) != 1 {
		panic("SHOW CREATE missing")
	}
	pass("SHOW CREATE")
	if *scheduled {
		exec("insert into facts values(8,4,123)")
		deadline := time.Now().Add(120 * time.Second)
		matched := false
		for time.Now().Before(deadline) {
			if reflect.DeepEqual(rows(aggregate+" order by k"), rows("select * from totals order by k")) {
				matched = true
				break
			}
			time.Sleep(2 * time.Second)
		}
		if !matched {
			panic("scheduled refresh did not publish within 120 seconds")
		}
		pass("scheduled refresh")
	}
	exec("alter materialized view totals refresh manual")
	state := rows("show materialized views")
	if len(state) != 3 {
		panic("SHOW list mismatch")
	}
	pass("SHOW state and full source timestamp")
	history := rows("show materialized view refreshes totals")
	if len(history) == 0 {
		panic("missing refresh history")
	}
	pass("refresh history")
	runAdditionalCases(*dsn)
	timeoutRead(*dsn)
	if *functional || *tenantCheck {
		for _, name := range []string{"joined", "empty_total", "totals"} {
			exec("drop materialized view " + name)
		}
		exec("drop database mv_validation")
		pass("drop cleanup")
		return
	}
	// Explicit view scan versus the same raw aggregation: P1 has no automatic rewrite.
	exec("create table bench(id bigint,k int,v bigint)")
	exec(fmt.Sprintf("insert into bench select result,result%%128,result%%1000 from generate_series(1,%d) g", *n))
	const baseline = "select k,sum(v) as s,count(*) as n from bench group by k"
	const materialized = "select k,s,n from bench_mv"
	exec("create materialized view bench_mv refresh manual as " + baseline)
	start := time.Now()
	exec("refresh materialized view bench_mv")
	evidence.RefreshMS = float64(time.Since(start).Microseconds()) / 1000
	equal("benchmark result equality", baseline+" order by k", materialized+" order by k")
	evidence.RawPlan = rows("explain " + baseline)
	evidence.MVPlan = rows("explain " + materialized)
	evidence.MaterializedRows = len(rows(materialized))
	rows(baseline)
	rows(materialized)
	for i := 0; i < 50; i++ {
		if i%2 == 0 {
			evidence.BaselineMS = append(evidence.BaselineMS, measure(baseline))
			evidence.MaterializedMS = append(evidence.MaterializedMS, measure(materialized))
		} else {
			evidence.MaterializedMS = append(evidence.MaterializedMS, measure(materialized))
			evidence.BaselineMS = append(evidence.BaselineMS, measure(baseline))
		}
	}
	evidence.Speedup = median(evidence.BaselineMS) / median(evidence.MaterializedMS)
	fmt.Printf("rows=%d refresh_ms=%.3f raw_median_ms=%.3f mv_median_ms=%.3f speedup=%.2fx\n", *n, evidence.RefreshMS, median(evidence.BaselineMS), median(evidence.MaterializedMS), evidence.Speedup)
	performanceCases()
	if *keep {
		pass("retained restart fixture")
		return
	}
	for _, name := range []string{"bench_mv", "joined", "empty_total", "totals"} {
		exec("drop materialized view " + name)
	}
	exec("drop database mv_validation")
	pass("drop cleanup")
}
