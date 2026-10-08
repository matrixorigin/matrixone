//go:build fulltext2_base_file_reuse

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package fulltext2

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// In-memory SQL producer, real spill/checksum/mmap/decode/Free. This isolates
// local materialization cost, NOT remote SQL latency or whole Search.Load CPU.
// Cold pool includes its creation, both existing decodes and final Close.
// Hit excludes only the explicitly prefilled file, never the new Segment decode.
func BenchmarkBaseFileLoadCost(b *testing.B) {
	for _, docs := range []int{256, 10000, 50000} {
		b.Run(fmt.Sprintf("docs=%d", docs), func(b *testing.B) {
			builder := NewBuilder("bench", int32(types.T_int64))
			for d := 0; d < docs; d++ {
				for term := 0; term < 8; term++ {
					if err := builder.Add(fmt.Sprintf("term%03d", (d+term)%128), int32(term*8), int64(d)); err != nil {
						b.Fatal(err)
					}
				}
			}
			source, err := builder.Finish()
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(source.Free)
			buf, err := source.Serialize()
			if err != nil {
				b.Fatal(err)
			}
			size, checksum := int64(len(buf)), vectorindex.CheckSumFromBuffer(buf)
			sp := sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(context.Background(), "load-cost", nil, 0, nil))
			mp := mpool.MustNewZero()
			b.Cleanup(func() { mpool.DeleteMPool(mp) })
			var streamed atomic.Int64
			oldSQL, oldContextSQL, oldStream := runSql, runSqlWithContext, runStreamingSql
			b.Cleanup(func() { runSql, runSqlWithContext, runStreamingSql = oldSQL, oldContextSQL, oldStream })
			runSql = func(*sqlexec.SqlProcess, string) (executor.Result, error) {
				return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, checksum, size, 1)}}, nil
			}
			runSqlWithContext = func(_ context.Context, sp *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				return runSql(sp, sql)
			}
			runStreamingSql = func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
				streamed.Add(size)
				sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, buf)}}
				return executor.Result{}, nil
			}
			for _, mode := range []string{"ordinary", "cold-pool", "hit", "capacity-fallback"} {
				b.Run(mode, func(b *testing.B) {
					var pool *baseFilePool
					if mode == "hit" || mode == "capacity-fallback" {
						budget := size
						if mode == "capacity-fallback" {
							budget--
						}
						pool = newBaseFilePool(budget, 1)
						b.Cleanup(pool.Close)
						if mode == "hit" {
							s, err := loadFromStorageWithPool(sp, testStorageCfg(), "bench", pool)
							if err != nil {
								b.Fatal(err)
							}
							s.Free()
						}
					}
					streamed.Store(0)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var s *Segment
						var err error
						if mode == "ordinary" {
							s, err = LoadFromStorage(sp, testStorageCfg(), "bench")
						} else {
							if mode == "cold-pool" {
								pool = newBaseFilePool(size, 1)
							}
							s, err = loadFromStorageWithPool(sp, testStorageCfg(), "bench", pool)
						}
						if err != nil {
							if pool != nil {
								pool.Close()
							}
							b.Fatal(err)
						}
						s.Free()
						if mode == "cold-pool" {
							pool.Close()
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(size), "base-bytes")
					b.ReportMetric(float64(streamed.Load())/float64(b.N), "streamed-B/op")
				})
			}
		})
	}
}
