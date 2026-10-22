// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package main

import (
	"fmt"
	"time"
)

type pairedExperiment struct {
	Name        string  `json:"name"`
	RefreshMS   float64 `json:"refresh_ms"`
	ResultRows  int     `json:"result_rows"`
	RawMS, MVMS []float64
	Ratio       float64 `json:"median_ratio"`
}

func paired(name, raw, stored string, refreshMS float64) pairedExperiment {
	equal(name+" result equality", raw+" order by 1", stored+" order by 1")
	result := pairedExperiment{Name: name, RefreshMS: refreshMS, ResultRows: len(rows(stored))}
	rows(raw)
	rows(stored)
	for i := 0; i < 50; i++ {
		if i%2 == 0 {
			result.RawMS = append(result.RawMS, measure(raw))
			result.MVMS = append(result.MVMS, measure(stored))
		} else {
			result.MVMS = append(result.MVMS, measure(stored))
			result.RawMS = append(result.RawMS, measure(raw))
		}
	}
	result.Ratio = median(result.RawMS) / median(result.MVMS)
	fmt.Printf("experiment=%s rows=%d raw_ms=%.3f mv_ms=%.3f ratio=%.2fx\n", name, result.ResultRows, median(result.RawMS), median(result.MVMS), result.Ratio)
	return result
}
func performanceCases() {
	// Explicit rollup uses mergeable SUM/COUNT, with an independent raw oracle.
	evidence.Experiments = append(evidence.Experiments, paired("B02 explicit rollup", "select k div 16 as g,sum(v) as s,count(*) as n from bench group by k div 16", "select k div 16 as g,sum(s) as s,sum(n) as n from bench_mv group by k div 16", 0))
	exec("create table bench_dimension(k int primary key,label int)")
	exec("insert into bench_dimension select result-1,(result-1)%8 from generate_series(1,128) g")
	const joined = "select d.label,sum(b.v) as s,count(*) as n from bench b inner join bench_dimension d on b.k=d.k group by d.label"
	exec("create materialized view bench_join_mv refresh manual as " + joined)
	start := time.Now()
	exec("refresh materialized view bench_join_mv")
	elapsed := float64(time.Since(start).Microseconds()) / 1000
	evidence.Experiments = append(evidence.Experiments, paired("B03 repeated join", joined, "select label,s,n from bench_join_mv", elapsed))
	exec("drop materialized view bench_join_mv")
	exec("drop table bench_dimension")
	// A small, near-source-grain control exposes weak/no improvement and costs.
	const granular = "select id,sum(v) as s,count(*) as n from bench where id<=10000 group by id"
	exec("create materialized view bench_grain_mv refresh manual as " + granular)
	start = time.Now()
	exec("refresh materialized view bench_grain_mv")
	elapsed = float64(time.Since(start).Microseconds()) / 1000
	evidence.Experiments = append(evidence.Experiments, paired("B05 high cardinality control", granular, "select id,s,n from bench_grain_mv", elapsed))
	exec("drop materialized view bench_grain_mv")
}
