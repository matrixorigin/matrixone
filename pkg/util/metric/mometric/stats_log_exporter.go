package mometric

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"sync"
	"sync/atomic"
	"time"
)

type StatsLogExporter struct {
	nodeUUID  string
	role      string
	isRunning int32
	registry  *metric.StatsRegistry
	cancel    context.CancelFunc
	stopWg    sync.WaitGroup
}

func newStatsLogExporter(node, role string) *StatsLogExporter {
	return &StatsLogExporter{
		registry: &metric.DefaultStatsRegistry,
		nodeUUID: node,
		role:     role,
	}
}

func (e *StatsLogExporter) Start(inputCtx context.Context) bool {
	if atomic.SwapInt32(&e.isRunning, 1) == 1 {
		return false
	}
	ctx, cancel := context.WithCancel(inputCtx)
	e.cancel = cancel
	e.stopWg.Add(1)
	go func() {
		defer e.stopWg.Done()
		ticker := time.NewTicker(metric.GetGatherInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				e.gatherAndLog()
			case <-ctx.Done():
				return
			}
		}
	}()
	return true
}

func (e *StatsLogExporter) Stop(_ bool) (<-chan struct{}, bool) {
	if atomic.SwapInt32(&e.isRunning, 0) == 0 {
		return nil, false
	}
	e.cancel()
	stopCh := make(chan struct{})
	go func() { e.stopWg.Wait(); close(stopCh) }()
	return stopCh, true
}

func (e *StatsLogExporter) gatherAndLog() {
	metrics := e.registry.Gather()
	logutil.Info(metrics)
}
