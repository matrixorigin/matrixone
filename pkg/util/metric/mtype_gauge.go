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
	prom "github.com/prometheus/client_golang/prometheus"
)

type Gauge interface {
	prom.Gauge
}

type GaugeOpts = prom.GaugeOpts

func NewGauge(opts prom.GaugeOpts) *gauge {
	mustValidLbls(opts.Name, opts.ConstLabels, nil)
	g := &gauge{Gauge: prom.NewGauge(opts)}
	g.init(g)
	return g
}

func NewGaugeVec(opts prom.GaugeOpts, lvs []string) *gaugeVec {
	mustValidLbls(opts.Name, opts.ConstLabels, lvs)
	gv := &gaugeVec{GaugeVec: prom.NewGaugeVec(opts, lvs)}
	gv.init(gv)
	return gv
}

type gauge struct {
	selfAsPromCollector
	prom.Gauge
}

type gaugeVec struct {
	selfAsPromCollector
	*prom.GaugeVec
}
