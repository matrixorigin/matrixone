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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/shirou/gopsutil/v3/cpu"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCPUBusyTimeUsesMonotonicBusyFields(t *testing.T) {
	Convey("busy CPU time excludes idle, iowait, and double-counted guest time", t, func() {
		stats := cpu.TimesStat{
			User:      1,
			System:    2,
			Nice:      3,
			Irq:       4,
			Softirq:   5,
			Steal:     6,
			Idle:      1 << 53,
			Iowait:    100,
			Guest:     100,
			GuestNice: 100,
		}
		So(cpuBusyTime(stats), ShouldEqual, float64(21))
		So(CPUTotalTime(stats)-stats.Idle, ShouldNotEqual, cpuBusyTime(stats))
	})

	Convey("a decreasing iowait sample cannot make busy CPU time decrease", t, func() {
		before := cpu.TimesStat{User: 100, Iowait: 10}
		after := cpu.TimesStat{User: 101, Iowait: 0}

		So(cpuBusyTime(after), ShouldBeGreaterThanOrEqualTo, cpuBusyTime(before))
		So(CPUTotalTime(after)-after.Idle, ShouldBeLessThan, CPUTotalTime(before)-before.Idle)
	})
}

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
		So(mf[0].GetName(), ShouldEqual, "sys_cpu_combined_percent")
		So(mf[1].GetName(), ShouldEqual, "sys_cpu_seconds_total")

		percent := mf[0].Metric[0].Gauge.GetValue()
		firstTotal := mf[1].Metric[0].Counter.GetValue()
		So(percent, ShouldBeGreaterThanOrEqualTo, 0)
		So(percent, ShouldBeLessThanOrEqualTo, 100)
		So(firstTotal, ShouldBeGreaterThanOrEqualTo, 0)

		mf2, err := reg.Gather()
		So(err, ShouldBeNil)
		So(len(mf2), ShouldEqual, 2)
		So(mf2[1].Metric[0].Counter.GetValue(), ShouldBeGreaterThanOrEqualTo, firstTotal)
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

func (c errorMetric) Metric(ctx context.Context, _ *statCaches) (prom.Metric, error) {
	return nil, moerr.NewInternalError(ctx, "Something went wrong")
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
