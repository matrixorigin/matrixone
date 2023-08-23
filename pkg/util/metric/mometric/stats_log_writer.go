// Copyright 2023 Matrix Origin
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

package mometric

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
)

type StatsLogWriter struct {
	isRunning int32
	cancel    context.CancelFunc
	stopWg    sync.WaitGroup

	registry       *stats.Registry
	gatherInterval time.Duration

	logger *log.MOLogger
}

func newStatsLogWriter(registry *stats.Registry, logger *log.MOLogger, gatherInterval time.Duration) *StatsLogWriter {
	return &StatsLogWriter{
		registry:       registry,
		gatherInterval: gatherInterval,
		logger:         logger,
	}
}

func (e *StatsLogWriter) Start(inputCtx context.Context) bool {
	if atomic.SwapInt32(&e.isRunning, 1) == 1 {
		return false
	}
	ctx, cancel := context.WithCancel(inputCtx)
	e.cancel = cancel
	e.stopWg.Add(1)
	go func() {
		defer e.stopWg.Done()
		ticker := time.NewTicker(e.gatherInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				e.gatherAndWrite(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
	return true
}

func (e *StatsLogWriter) Stop(_ bool) (<-chan struct{}, bool) {
	if atomic.SwapInt32(&e.isRunning, 0) == 0 {
		return nil, false
	}
	e.cancel()
	stopCh := make(chan struct{})
	go func() { e.stopWg.Wait(); close(stopCh) }()
	return stopCh, true
}

// gatherAndWriter gathers all the logs from Stats and Writes to Log
// Example log output:
// 2023/03/15 02:37:31.767463 -0500 INFO cn-service mometric/stats_log_writer.go:86 MockServiceStats stats  {"uuid": "test", "reads": 2, "hits": 1}
// 2023/03/15 02:37:33.767659 -0500 INFO cn-service mometric/stats_log_writer.go:86 MockServiceStats stats  {"uuid": "test", "reads": 0, "hits": 0}
// 2023/03/15 02:37:35.767608 -0500 INFO cn-service mometric/stats_log_writer.go:86 MockServiceStats stats  {"uuid": "test", "reads": 0, "hits": 0}
func (e *StatsLogWriter) gatherAndWrite(ctx context.Context) {
	statsFamilies := e.registry.ExportLog()
	for statsFName, statsFamily := range statsFamilies {
		if len(statsFamily) > 1 {
			e.logger.Info(statsFName, statsFamily...)
		}
	}

	// logging block read statistics info here
	v := ctx.Value(ServiceTypeKey).(string)
	if v == metadata.ServiceType_name[int32(metadata.ServiceType_CN)] || v == LaunchMode {
		e.writeBlkReadStats()
	}
}

func (e *StatsLogWriter) writeBlkReadStats() {
	blkHit, blkTotal := objectio.BlkReadStats.BlkCacheHitStats.ExportW()
	blkHitRate := float32(1)
	if blkTotal != 0 {
		blkHitRate = float32(blkHit) / float32(blkTotal)
	}

	entryHit, entryTotal := objectio.BlkReadStats.EntryCacheHitStats.ExportW()
	entryHitRate := float32(1)
	if entryTotal != 0 {
		entryHitRate = float32(entryHit) / float32(entryTotal)
	}

	readerNum, blkNum := objectio.BlkReadStats.BlksByReaderStats.ExportW()
	blksInEachReader := float32(1)
	if readerNum != 0 {
		blksInEachReader = float32(blkNum) / float32(readerNum)
	}

	e.logger.Info(fmt.Sprintf("duration: %d, "+
		"blk hit rate: %d/%d=%.4f, entry hit rate: %d/%d=%.4f, (average) blks in each reader: %d/%d=%.4f",
		e.gatherInterval,
		blkHit, blkTotal, blkHitRate,
		entryHit, entryTotal, entryHitRate,
		blkNum, readerNum, blksInEachReader))

}
