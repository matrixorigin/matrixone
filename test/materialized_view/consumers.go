// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"
)

func runConsumerCases() {
	db.SetMaxOpenConns(1)
	exec("create database mv_consumer_validation")
	defer func() {
		failure := recover()
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var failures []string
		for _, name := range []string{"stored_copy", "policy_copy", "policy", "stored"} {
			if _, err := db.ExecContext(cleanup, "drop materialized view if exists mv_consumer_validation."+name); err != nil {
				failures = append(failures, err.Error())
			}
		}
		if _, err := db.ExecContext(cleanup, "drop database mv_consumer_validation"); err != nil {
			failures = append(failures, err.Error())
		}
		if len(failures) > 0 {
			panic(fmt.Sprintf("consumer cleanup: %v; original=%v", failures, failure))
		}
		if failure != nil {
			panic(failure)
		}
	}()
	exec("use mv_consumer_validation")
	exec("set experimental_materialized_view=1")
	exec("set time_zone='+00:00'")
	exec("set delete_opt_to_truncate=1")
	exec("create table facts(id int primary key,k int unique,skey int,index sk(skey))")
	const insert = "insert into facts values(1,11,10),(2,22,20)"
	exec(insert)
	// This statement predates dependency creation: cached eligibility must not
	// turn valid source DML into a rejected physical-identity replacement.
	prepared, err := db.PrepareContext(ctx, "delete from facts")
	if err != nil {
		panic(err)
	}
	defer prepared.Close()
	exec("create materialized view stored refresh manual as select id,k,skey from facts")
	exec("refresh materialized view stored")
	const publication = "select successful_sequence,last_run,source_physical,source_logical from mo_catalog.mo_materialized_views where account_id=0 and database_name='mv_consumer_validation' and name='stored'"
	before := rows(publication)
	exec("set experimental_materialized_view=0")
	deleted, err := prepared.ExecContext(ctx)
	if err != nil {
		panic(fmt.Errorf("prepared source DELETE after MV creation: %w", err))
	}
	if !reflect.DeepEqual(rows("select count(*) from facts"), [][]string{{"0"}}) || !reflect.DeepEqual(rows("select count(*) from stored"), [][]string{{"2"}}) || !reflect.DeepEqual(before, rows(publication)) {
		panic("prepared source DELETE changed MV publication")
	}
	affected, err := deleted.RowsAffected()
	if err != nil || affected != 2 {
		panic(fmt.Sprintf("prepared DELETE affected rows: %d %v", affected, err))
	}
	pass("prepared unrestricted source DELETE preserves MV publication")
	if !reflect.DeepEqual(rows("select count(*) from facts force index(sk) where skey=10"), [][]string{{"0"}}) {
		panic("source DELETE left secondary index rows")
	}
	exec(insert)
	pass("source DELETE maintains secondary and unique indexes")
	_, err = db.ExecContext(ctx, "truncate table facts")
	requireServerRejection("explicit source TRUNCATE remains protected", err)
	if !strings.Contains(err.Error(), "dependent materialized views") {
		panic(err)
	}
	pass("explicit source TRUNCATE remains protected")
	tablesBefore := rows("select relname from mo_catalog.mo_tables where account_id=0 and reldatabase='mv_consumer_validation' order by relname")
	_, err = db.ExecContext(ctx, "alter table facts algorithm=copy, add column copy_col int")
	requireServerRejection("dependent source ALTER COPY", err)
	if !strings.Contains(err.Error(), "dependent materialized views") {
		panic(err)
	}
	if !reflect.DeepEqual(before, rows(publication)) || !reflect.DeepEqual(tablesBefore, rows("select relname from mo_catalog.mo_tables where account_id=0 and reldatabase='mv_consumer_validation' order by relname")) {
		panic("rejected ALTER COPY changed publication or left a prepared relation")
	}
	pass("ALTER COPY rejects dependent source without preparation residue")

	deleted, err = db.ExecContext(ctx, "delete from facts")
	if err != nil {
		panic(err)
	}
	affected, err = deleted.RowsAffected()
	if err != nil || affected != 2 {
		panic(fmt.Sprintf("direct DELETE affected rows: %d %v", affected, err))
	}
	if !reflect.DeepEqual(rows("select count(*) from facts"), [][]string{{"0"}}) || !reflect.DeepEqual(before, rows(publication)) {
		panic("direct source DELETE changed identity/publication")
	}
	pass("direct unrestricted source DELETE keeps dependency identity")
	deleted, err = db.ExecContext(ctx, "delete from facts")
	if err != nil {
		panic(err)
	}
	affected, err = deleted.RowsAffected()
	if err != nil || affected != 0 {
		panic(fmt.Sprintf("empty DELETE affected rows: %d %v", affected, err))
	}
	pass("empty dependent source DELETE returns zero affected rows")
	for _, verb := range []string{"dump table stored to ", "load table stored from "} {
		_, err := db.ExecContext(ctx, verb+"'file:///tmp/mv-consumer-missing-dump'")
		requireServerRejection("native dump/load MV admission", err)
		if !strings.Contains(err.Error(), "DUMP TABLE and LOAD TABLE with views") {
			panic(fmt.Errorf("expected MV kind rejection before file access: %w", err))
		}
		if !reflect.DeepEqual(rows("select count(*) from stored"), [][]string{{"2"}}) || !reflect.DeepEqual(before, rows(publication)) {
			panic("dump/load changed MV rows or provenance")
		}
		pass("MV rejected at native boundary: " + verb)
	}
	replay := func(name, target string) {
		ddl := rows("show create table " + name)
		explicit := rows("show create materialized view " + name)
		if len(ddl) != 1 || len(ddl[0]) != 2 || len(explicit) != 1 || ddl[0][1] != explicit[0][1] || !strings.HasPrefix(ddl[0][1], "create materialized view ") || strings.Contains(ddl[0][1], "mo_materialized_view") {
			panic(fmt.Sprintf("inconsistent MV export: %v %v", ddl, explicit))
		}
		old := "`mv_consumer_validation`.`" + name + "`"
		renamed := strings.Replace(ddl[0][1], old, "`mv_consumer_validation`.`"+target+"`", 1)
		if renamed == ddl[0][1] {
			panic("export omitted qualified target")
		}
		exec(renamed)
	}
	exec("set experimental_materialized_view=1")
	replay("stored", "stored_copy")
	exec("refresh materialized view stored_copy")
	if !reflect.DeepEqual(rows("select count(*) from stored_copy"), [][]string{{"0"}}) {
		panic("replayed manual MV did not refresh from current source")
	}
	pass("SHOW CREATE TABLE round-trips a manual MV")
	exec("create materialized view policy refresh every interval 60 second as select id from facts")
	initialPolicy := rows("show create table policy")
	if len(initialPolicy) != 1 || !strings.Contains(initialPolicy[0][1], "every interval 60 second") {
		panic("initial export lost periodic policy")
	}
	exec("alter materialized view policy refresh every interval 120 second")
	replay("policy", "policy_copy")
	if !reflect.DeepEqual(rows("select interval_seconds from mo_catalog.mo_materialized_views where account_id=0 and database_name='mv_consumer_validation' and name='policy_copy'"), [][]string{{"120"}}) {
		panic("export replay lost current altered policy")
	}
	exec("alter materialized view policy refresh manual")
	exec("alter materialized view policy_copy refresh manual")
	pass("SHOW CREATE TABLE exports the authoritative altered refresh policy")
}
