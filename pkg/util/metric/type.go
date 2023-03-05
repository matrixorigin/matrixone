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
	"fmt"
	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
	prom "github.com/prometheus/client_golang/prometheus"
	"strings"
	"sync/atomic"
)

const MetricDBConst = "system_metrics"

var (
	LblNodeConst  = "node"
	LblRoleConst  = "role"
	LblValueConst = "value"
	LblTimeConst  = "collecttime"
	occupiedLbls  = map[string]struct{}{LblTimeConst: {}, LblValueConst: {}, LblNodeConst: {}, LblRoleConst: {}}
)

type Collector interface {
	prom.Collector
	// cancelToProm remove the cost introduced by being compatible with prometheus
	CancelToProm()
	// collectorForProm returns a collector used in prometheus scrape registry
	CollectorToProm() prom.Collector
}

type MetricExporter interface {
	// ExportMetricFamily can be used by a metric to push data. this method must be thread safe
	ExportMetricFamily(context.Context, *pb.MetricFamily) error
	Start(context.Context) bool
	Stop(bool) (<-chan struct{}, bool)
}

type selfAsPromCollector struct {
	self prom.Collector
}

func (s *selfAsPromCollector) init(self prom.Collector)        { s.self = self }
func (s *selfAsPromCollector) CancelToProm()                   {}
func (s *selfAsPromCollector) CollectorToProm() prom.Collector { return s.self }

func mustValidLbls(name string, consts prom.Labels, vars []string) {
	mustNotOccupied := func(lblName string) {
		if _, ok := occupiedLbls[strings.ToLower(lblName)]; ok {
			panic(fmt.Sprintf("%s contains a occupied label: %s", name, lblName))
		}
	}
	for k := range consts {
		mustNotOccupied(k)
	}
	for _, v := range vars {
		mustNotOccupied(v)
	}
}

type ExporterHolder struct {
	atomic.Value
}

func NewExportHolder(e MetricExporter) *ExporterHolder {
	h := &ExporterHolder{}
	h.Store(e)
	return h
}

func (h *ExporterHolder) Get() MetricExporter {
	if h != nil {
		return h.Load().(MetricExporter)
	}
	return nil
}

// exporterHolder cooperate with RawHist
var exporterHolder ExporterHolder

func SetMetricExporter(e MetricExporter) {
	exporterHolder.Store(e)
}
