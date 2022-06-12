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
	"fmt"

	dto "github.com/prometheus/client_model/go"
)

// P2M means prometheus to matrixone

func P2MMetricFamilies(mfs []*dto.MetricFamily) []*MetricFamily {
	moMfs := make([]*MetricFamily, 0, len(mfs))
	for _, mf := range mfs {
		if mf.GetType() == dto.MetricType_UNTYPED {
			// Untyped is used as a placeholder, it has no data
			continue
		}
		moMf := &MetricFamily{
			Name: mf.GetName(),
			Help: mf.GetHelp(),
		}
		switch mf.GetType() {
		case dto.MetricType_COUNTER:
			moMf.Type = MetricType_COUNTER
			moMf.Metric = P2MCounters(mf.Metric)
		case dto.MetricType_GAUGE:
			moMf.Type = MetricType_GAUGE
			moMf.Metric = P2MGauges(mf.Metric)
		default:
			panic(fmt.Sprintf("unsupported metric type in mo %v", mf.GetType()))
		}
		moMfs = append(moMfs, moMf)
	}
	return moMfs
}

func P2MCounters(ms []*dto.Metric) []*Metric {
	moMetrics := make([]*Metric, len(ms))
	for i, m := range ms {
		moMetrics[i] = &Metric{
			Label:   P2MLabelPairs(m.Label),
			Counter: &Counter{Value: m.Counter.GetValue()},
		}
	}
	return moMetrics
}

func P2MGauges(ms []*dto.Metric) []*Metric {
	moMetrics := make([]*Metric, len(ms))
	for i, m := range ms {
		moMetrics[i] = &Metric{
			Label: P2MLabelPairs(m.Label),
			Gauge: &Gauge{Value: m.Gauge.GetValue()},
		}
	}
	return moMetrics
}

func P2MLabelPairs(lbls []*dto.LabelPair) []*LabelPair {
	moLbls := make([]*LabelPair, len(lbls))
	for i, lbl := range lbls {
		moLbls[i] = &LabelPair{
			Name:  lbl.GetName(),
			Value: lbl.GetValue(),
		}
	}
	return moLbls
}
