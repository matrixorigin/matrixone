// Copyright 2022 Matrix Origin
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

package metric

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
	prom "github.com/prometheus/client_golang/prometheus"
)

type MetricExporter interface {
	// ExportMetricFamily can be used by a metric to push data. this method must be thread safe
	ExportMetricFamily(context.Context, *pb.MetricFamily) error
	Start() bool
	Stop(bool) (<-chan struct{}, bool)
}

type metricExporter struct {
	localCollector MetricCollector
	nodeid         int32
	role           string
	gather         prom.Gatherer
	isRunning      int32
	cancel         context.CancelFunc
	stopWg         sync.WaitGroup
	sync.Mutex
	histFamilies []*pb.MetricFamily
	now          func() int64
}

func newMetricExporter(gather prom.Gatherer, collector MetricCollector, node int32, role string) MetricExporter {
	m := &metricExporter{
		localCollector: collector,
		nodeid:         node,
		role:           role,
		gather:         gather,
		now:            func() int64 { return int64(types.Now()) },
	}
	return m
}

func (e *metricExporter) ExportMetricFamily(ctx context.Context, mf *pb.MetricFamily) error {
	// already batched RawHist metric will be send immediately
	if isFullBatchRawHist(mf) {
		mfs := []*pb.MetricFamily{mf}
		mfs = e.prepareSend(mfs)
		e.send(mfs)
	} else {
		e.Lock()
		defer e.Unlock()
		e.histFamilies = append(e.histFamilies, mf)
	}
	return nil
}

func (e *metricExporter) Stop(_ bool) (<-chan struct{}, bool) {
	if atomic.SwapInt32(&e.isRunning, 0) == 0 {
		return nil, false
	}
	e.cancel()
	stopCh := make(chan struct{})
	go func() { e.stopWg.Wait(); close(stopCh) }()
	return stopCh, true
}

func (e *metricExporter) Start() bool {
	if atomic.SwapInt32(&e.isRunning, 1) == 1 {
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.stopWg.Add(1)
	go func() {
		defer e.stopWg.Done()
		ticker := time.NewTicker(getGatherInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				e.gatherAndSend()
			case <-ctx.Done():
				return
			}
		}
	}()
	return true
}

func (e *metricExporter) send(mfs []*pb.MetricFamily) {
	if e.localCollector == nil {
		panic("[Metric] Only a local MetricCollector")
	}
	err := e.localCollector.SendMetrics(context.TODO(), mfs)
	if err != nil {
		logutil.Errorf("[Metric] exporter send err: %v", err)
	}
}

func (e *metricExporter) addCommonInfo(mfs []*pb.MetricFamily) {
	now := e.now()
	for _, mf := range mfs {
		mf.Role = e.role
		mf.Node = e.nodeid
		for _, m := range mf.Metric {
			m.Collecttime = now
		}
	}
}

func (e *metricExporter) prepareSend(mfs []*pb.MetricFamily) []*pb.MetricFamily {
	e.Lock()
	mfs = append(mfs, e.histFamilies...)
	e.histFamilies = e.histFamilies[:0]
	e.Unlock()
	e.addCommonInfo(mfs)
	return mfs
}

func (e *metricExporter) gatherAndSend() {
	prommfs, err := e.gather.Gather()
	if err != nil {
		logutil.Errorf("[Metric] gather error: %v", err)
	}
	mfs := pb.P2MMetricFamilies(prommfs)
	mfs = e.prepareSend(mfs)
	e.send(mfs)
}
