// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package main

import (
	"fmt"
	"os"
)

// Use only an isolated, initialized pre-MV catalog with automatic upgrade off.
// If normal authentication rejects the old cluster at tenant-version admission,
// a test-only skipCheckUser session can isolate this kernel guard. It does not
// establish mixed-version/login compatibility; privilege checks remain enabled.
func runMissingMaterializedViewCatalog() {
	if rows("select count(*) from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname in ('mo_materialized_views','mo_mv_dependencies','mo_mv_refresh_runs','mo_mv_account_state')")[0][0] != "0" {
		panic("catalog-missing requires all four MV catalogs absent")
	}
	exec("set experimental_materialized_view=0")
	exec("drop database if exists mv_validation_pre_migration")
	exec("create database mv_validation_pre_migration")
	defer func() {
		original := recover()
		_, err := db.ExecContext(ctx, "drop database if exists mv_validation_pre_migration")
		if original != nil {
			if err != nil {
				fmt.Fprintln(os.Stderr, "fixture cleanup:", err)
			}
			panic(original)
		}
		if err != nil {
			panic(err)
		}
	}()
	exec("create table mv_validation_pre_migration.facts(id int primary key,v int)")
	exec("insert into mv_validation_pre_migration.facts values(1,7)")
	for _, item := range []struct{ name, sql string }{
		{"ordinary ALTER before MV migration", "alter table mv_validation_pre_migration.facts add column extra int"},
		{"ordinary CREATE INDEX before MV migration", "create index v_idx on mv_validation_pre_migration.facts(v)"},
		{"ordinary DROP INDEX before MV migration", "drop index v_idx on mv_validation_pre_migration.facts"},
		{"ordinary TRUNCATE before MV migration", "truncate table mv_validation_pre_migration.facts"},
		{"ordinary DROP TABLE before MV migration", "drop table mv_validation_pre_migration.facts"},
		{"ordinary DROP DATABASE before MV migration", "drop database mv_validation_pre_migration"},
	} {
		exec(item.sql)
		pass(item.name)
	}
}
