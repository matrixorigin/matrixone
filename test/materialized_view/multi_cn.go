// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"

	mysql "github.com/go-sql-driver/mysql"
)

// The second CN shares the same TN and storage. No fixture from the benchmark
// is reused: the topology checks own a small, separate database.
func runMultiCN(secondDSN string) {
	const database = "mv_validation_multi_cn"
	evidence.Rows = 4
	db.SetMaxOpenConns(1)
	exec("create database if not exists " + database)
	exec("use " + database)
	exec("set experimental_materialized_view=1")
	exec("drop materialized view if exists atomic_mv")
	exec("drop table if exists atomic_source")
	defer func() {
		exec("drop materialized view if exists atomic_mv")
		exec("drop table if exists atomic_source")
		exec("drop database " + database)
	}()
	exec("create table atomic_source(id int primary key,v bigint)")
	exec("insert into atomic_source values(1,1),(2,1),(3,1),(4,1)")
	exec("create materialized view atomic_mv refresh manual as select count(*) as n,sum(v) as s,min(v) as lo,max(v) as hi from atomic_source")
	config, err := mysql.ParseDSN(secondDSN)
	if err != nil {
		panic(err)
	}
	config.DBName = database
	other, err := openConfigured(config)
	if err != nil {
		panic(err)
	}
	defer other.Close()
	other.SetMaxOpenConns(1)
	if err = other.PingContext(ctx); err != nil {
		panic(err)
	}
	if _, err = other.ExecContext(ctx, "select * from atomic_mv"); err == nil {
		panic("second CN accepted an unpopulated view")
	} else {
		var serverError *mysql.MySQLError
		if !errors.As(err, &serverError) || serverError.Number == 1046 {
			panic(fmt.Errorf("second CN population check: %w", err))
		}
	}
	pass("second CN rejects unpopulated view")
	exec("refresh materialized view atomic_mv")
	read := func(connection *sql.DB) int64 {
		var n, s, lo, hi int64
		if err := connection.QueryRowContext(ctx, "select n,s,lo,hi from atomic_mv").Scan(&n, &s, &lo, &hi); err != nil {
			panic(err)
		}
		if n != 4 || lo != hi || s != 4*lo {
			panic(fmt.Sprintf("partial cross-CN publication: %d %d %d %d", n, s, lo, hi))
		}
		return lo
	}
	if read(other) != 1 {
		panic("second CN did not see first publication")
	}
	pass("cross-CN publication visibility")
	if _, err = other.ExecContext(ctx, "refresh materialized view atomic_mv"); err != nil {
		panic(fmt.Errorf("standalone second-CN refresh: %w", err))
	}
	if read(db) != 1 {
		panic("first CN did not see the second CN's publication")
	}
	pass("second CN can refresh independently")
	var originalID uint64
	if err = db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='"+database+"' and relname='atomic_mv'").Scan(&originalID); err != nil {
		panic(err)
	}
	var previousPhysical int64
	var previousLogical uint32
	for version := int64(2); version <= 6; version++ {
		publisher, reader := db, other
		if version%2 == 1 {
			publisher, reader = other, db
		}
		if _, err = publisher.ExecContext(ctx, fmt.Sprintf("update atomic_source set v=%d", version)); err != nil {
			panic(err)
		}
		start := make(chan struct{})
		results := make(chan error, 2)
		var wg sync.WaitGroup
		for _, connection := range []*sql.DB{db, other} {
			wg.Go(func() {
				<-start
				_, refreshErr := connection.ExecContext(ctx, "refresh materialized view atomic_mv")
				results <- refreshErr
			})
		}
		close(start)
		wg.Wait()
		close(results)
		succeeded := 0
		for refreshErr := range results {
			if refreshErr == nil {
				succeeded++
			} else {
				var serverError *mysql.MySQLError
				if !errors.As(refreshErr, &serverError) || serverError.Number != 20619 {
					panic(fmt.Errorf("unexpected cross-CN refresh failure: %w", refreshErr))
				}
				fmt.Println("concurrent refresh rejected:", refreshErr)
			}
		}
		if succeeded == 0 || read(reader) != version || read(publisher) != version {
			panic("cross-CN refresh did not publish the latest complete fixture")
		}
		var id uint64
		var physical int64
		var logical uint32
		if err = reader.QueryRowContext(ctx, "select mv_id,source_physical,source_logical from mo_catalog.mo_materialized_views where database_name='"+database+"' and name='atomic_mv'").Scan(&id, &physical, &logical); err != nil {
			panic(err)
		}
		if id != originalID || physical < previousPhysical || (physical == previousPhysical && logical < previousLogical) {
			panic("cross-CN identity or source timestamp regressed")
		}
		previousPhysical, previousLogical = physical, logical
		pass(fmt.Sprintf("two-CN concurrent publication version %d", version))
	}
	var successes, pending int
	if err = other.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_mv_refresh_runs where mv_id=? and status='SUCCEEDED'", originalID).Scan(&successes); err != nil {
		panic(err)
	}
	if err = other.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_mv_refresh_runs where mv_id=? and status='RUNNING'", originalID).Scan(&pending); err != nil {
		panic(err)
	}
	if successes < 7 || pending != 0 {
		panic(fmt.Sprintf("cross-CN history not terminal: successful=%d pending=%d", successes, pending))
	}
	pass("cross-CN terminal history")
}
