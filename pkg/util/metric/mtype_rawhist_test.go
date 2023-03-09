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
	"testing"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

type dummyExp struct {
	dummySwitch
	sync.Mutex
	sync.WaitGroup
	mfs []*pb.MetricFamily
}

func (e *dummyExp) ExportMetricFamily(ctx context.Context, mf *pb.MetricFamily) error {
	e.Lock()
	defer e.Unlock()
	e.mfs = append(e.mfs, mf)
	e.Done()
	return nil
}

func TestRawHistCancel(t *testing.T) {
	rawFactory := NewRawHistVec(prom.HistogramOpts{
		Subsystem: "test",
		Name:      "test_raw",
		Help:      "test raw hist metric",
	}, []string{"d1"})
	x := rawFactory.WithLabelValues("d1-x").(*rawHist)
	y := rawFactory.WithLabelValues("d1-y").(*rawHist)
	z := rawFactory.WithLabelValues("d1-z").(*rawHist)
	assert.NotNil(t, x.compat_inner)
	assert.NotNil(t, y.compat_inner)
	assert.NotNil(t, z.compat_inner)
	rawFactory.CancelToProm()
	assert.Nil(t, x.compat_inner)
	assert.Nil(t, y.compat_inner)
	assert.Nil(t, z.compat_inner)
}

func TestRawHistVec(t *testing.T) {
	rawFactory := NewRawHistVec(prom.HistogramOpts{
		Subsystem: "test",
		Name:      "test_raw",
		Help:      "test raw hist metric",
		// leave Bucket as default, we don't care
	}, []string{"d1"})
	rawObserver := rawFactory.WithLabelValues("d1-x").(*rawHist)

	dummyExp := &dummyExp{}
	var iexp MetricExporter = dummyExp
	rawObserver.exporter = NewExportHolder(iexp)
	rawObserver.now = func() int64 { return 10000 }
	exportCnt := 2
	dummyExp.Add(exportCnt)

	withModifiedConfig(func() {
		defer SetRawHistBufLimit(SetRawHistBufLimit(10))
		// Add enough samples to trigger twice Export
		for i := 0; i < exportCnt*10; i++ {
			rawObserver.Observe(float64(i))
		}

		// wait Export complete
		if err := waitWgTimeout(&dummyExp.WaitGroup, time.Second); err != nil {
			t.Errorf("rawHist send timeout")
		}
	})

	// check labels and values
	for _, mf := range dummyExp.mfs {
		if got := mf.GetName(); got != "test_test_raw" {
			t.Errorf("mismatched mf name, got %s", got)
			return
		}
		for _, metric := range mf.Metric {
			if len(metric.Label) != 1 ||
				metric.Label[0].GetName() != "d1" ||
				metric.Label[0].GetValue() != "d1-x" {
				t.Errorf("mismatched metric label: got %v", metric.Label)
				return
			}
			for i, sample := range metric.GetRawHist().Samples {
				var want float64
				if sample.GetValue() >= 10 {
					want = float64(10 + i)
				} else {
					want = float64(i)
				}
				if sample.GetValue() != want || sample.GetDatetime() != 10000 {
					t.Errorf("wrong sample data, got %s, want {10000, %f}", sample.String(), want)
				}
			}
		}
	}

	// try Export when buffer is not full
	dummyExp.Add(1)
	// this Write won't descrease wg because of empty buffer now
	_ = rawObserver.Write(&dto.Metric{})

	// add some samples and then Write, we will find a Export
	values := []float64{1.2, 0.13, 1.35}
	for _, v := range values {
		rawObserver.Observe(v)
	}
	out := &dto.Metric{}
	_ = rawObserver.Write(out)
	if len(out.Label) != 1 {
		t.Errorf("rawHist write wrong labels")
		return
	}
	if out.GetUntyped() == nil {
		t.Errorf("rawHist write wrong metric type")
		return
	}

	if err := waitWgTimeout(&dummyExp.WaitGroup, time.Second); err != nil {
		t.Errorf("rawHist send timeout")
		return
	}
	for i, sample := range dummyExp.mfs[2].Metric[0].GetRawHist().Samples {
		if sample.GetValue() != values[i] || sample.GetDatetime() != 10000 {
			t.Errorf("wrong sample data, got %s, want {10000, %f}", sample.String(), values[i])
		}
	}
}

func TestRawHistVecPanic(t *testing.T) {
	// contains occuppied lables
	assert.Panics(t, func() {
		NewRawHistVec(HistogramOpts{ConstLabels: prom.Labels{"node": "77"}}, nil)
	})
	rawFactory := NewRawHistVec(prom.HistogramOpts{}, []string{"d1", "d2"})

	// wrong number of labels
	for _, lvs := range [][]string{
		{},
		{"1"},
		{"1", "2", "3"},
	} {
		_, err := rawFactory.GetMetricWithLabelValues(lvs...)
		assert.NotNil(t, err, "mismatched labels are accepted")
		assert.Panics(t, func() { rawFactory.WithLabelValues(lvs...) })
	}
}

func TestRawHistGather(t *testing.T) {
	dumExp := &dummyExp{}
	var iexp MetricExporter = dumExp
	raw1 := NewRawHist(prom.HistogramOpts{
		Subsystem: "test",
		Name:      "test_raw",
		Help:      "test raw hist metric",
	})
	raw1.exporter = NewExportHolder(iexp)

	raw2 := NewRawHist(prom.HistogramOpts{
		Subsystem: "stats",
		Name:      "test_stats",
		Help:      "test stats metric",
	})
	raw2.exporter = NewExportHolder(iexp)

	reg := prom.NewRegistry()
	reg.MustRegister(raw1)
	reg.MustRegister(raw2)

	values := []float64{1.2, 0.13, 1.35}
	for _, v := range values {
		raw1.Observe(v)
	}

	// add 1 because gather will skip raw2 due to empty buffer
	dumExp.Add(1)
	mfs, err := reg.Gather()
	if err != nil {
		t.Errorf("rawHist Gather err: %v", err)
	}
	if err := waitWgTimeout(&dumExp.WaitGroup, time.Second); err != nil {
		t.Errorf("rawHist send timeout")
		return
	}

	moMfs := pb.P2MMetricFamilies(mfs)

	if len(moMfs) != 0 {
		t.Error("Gather() on rawHist produces MO metricFamily")
	}
}
