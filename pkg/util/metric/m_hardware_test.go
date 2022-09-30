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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	. "github.com/smartystreets/goconvey/convey"
)

func TestHardwareCPU(t *testing.T) {
	Convey("collect cpu succ", t, func() {
		reg := prom.NewRegistry()
		reg.MustRegister(newBatchStatsCollector(cpuPercent{}, cpuTotal{}))

		mf, err := reg.Gather()
		So(err, ShouldBeNil)
		So(len(mf), ShouldEqual, 2)
		// order by metric name
		So(mf[0].GetType(), ShouldEqual, dto.MetricType_GAUGE)
		So(mf[1].GetType(), ShouldEqual, dto.MetricType_COUNTER)

		time.Sleep(time.Second)
		mf2, err := reg.Gather()
		So(err, ShouldBeNil)
		So(len(mf2), ShouldEqual, 2)

		deltaBusy := mf2[1].Metric[0].Counter.GetValue() - mf[1].Metric[0].Counter.GetValue()
		deltaPercent := mf2[0].Metric[0].Gauge.GetValue()

		So(deltaBusy*100, ShouldAlmostEqual, deltaPercent, 40 /* 40% diff will be ok anyway */)
	})
}

func TestHardwareMem(t *testing.T) {
	Convey("collect mem succ", t, func() {
		reg := prom.NewRegistry()
		reg.MustRegister(newBatchStatsCollector(memAvail{}, memUsed{}))

		mf, err := reg.Gather()
		So(err, ShouldBeNil)
		So(len(mf), ShouldEqual, 2)
	})
}

type errorMetric struct{}

func (c errorMetric) Desc() *prom.Desc {
	return prom.NewDesc(
		"test_error_metric",
		"a metric returning errors",
		nil, nil,
	)
}

func (c errorMetric) Metric(_ *statCaches) (prom.Metric, error) {
	return nil, moerr.NewInternalError("Something went wrong")
}

func TestHardwareError(t *testing.T) {
	Convey("collect no error metric", t, func() {
		reg := prom.NewRegistry()
		reg.MustRegister(newBatchStatsCollector(errorMetric{}))

		mf, err := reg.Gather()
		So(err, ShouldBeNil)
		So(len(mf), ShouldEqual, 0)
	})
}
