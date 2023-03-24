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
	"sync/atomic"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeDummyNow() func() time.Time {
	tick := int64(0)
	now := time.Now()
	return func() time.Time {
		// 2s gap
		elapse := time.Duration(atomic.AddInt64(&tick, 2)) * time.Second
		return now.Add(elapse)
	}
}

func TestRateCounter(t *testing.T) {
	c := newRateCounter(CounterOpts{
		Subsystem: "test",
		Name:      "rate_counter",
	})
	c.now = makeDummyNow()

	inReg := prom.NewRegistry()
	outReg := prom.NewRegistry()
	inReg.MustRegister(c)
	outReg.MustRegister(c.CollectorToProm())

	c.Inc()
	c.Inc()

	mf, err := inReg.Gather()
	assert.NoError(t, err)
	assert.Equal(t, len(mf), 1)
	assert.InDelta(t, 0.0, mf[0].Metric[0].Counter.GetValue(), 0.05)

	mfout, err := outReg.Gather()
	assert.NoError(t, err)
	assert.Equal(t, len(mfout), 1)
	assert.InDelta(t, 2.0, mfout[0].Metric[0].Counter.GetValue(), 0.05)

	c.Add(12.0)
	c.Add(2.6)

	mf, err = inReg.Gather()
	assert.NoError(t, err)
	assert.InDelta(t, 7.3, mf[0].Metric[0].Counter.GetValue(), 0.05)

	mfout, err = outReg.Gather()
	assert.NoError(t, err)
	assert.InDelta(t, 16.6, mfout[0].Metric[0].Counter.GetValue(), 0.05)
}

func TestRateCounterVec(t *testing.T) {
	factory := newRateCounterVec(CounterOpts{
		Subsystem: "test",
		Name:      "rate_counter",
	}, []string{"zzz", "aaa"}, true)

	require.Panics(t, func() { factory.WithLabelValues("12") })

	c1 := factory.WithLabelValues("1", "2").(*ratecounter)
	c2 := factory.WithLabelValues("10", "20").(*ratecounter)

	c1.now = makeDummyNow()
	c2.now = makeDummyNow()

	inReg := prom.NewRegistry()
	outReg := prom.NewRegistry()
	inReg.MustRegister(factory)
	outReg.MustRegister(factory.CollectorToProm())

	getC1C2Value := func(mf *dto.MetricFamily) (c1 float64, c2 float64) {
		aaaLable := mf.Metric[0].Label[0].GetValue()
		if aaaLable == "2" {
			c1, c2 = *mf.Metric[0].Counter.Value, *mf.Metric[1].Counter.Value
		} else if aaaLable == "20" {
			c1, c2 = *mf.Metric[1].Counter.Value, *mf.Metric[0].Counter.Value
		} else {
			require.Fail(t, "bad label order")
		}
		return
	}

	c1.Add(12.0)
	c2.Add(6.0)

	mf, err := inReg.Gather()
	assert.NoError(t, err)
	assert.Equal(t, len(mf), 1)
	assert.Equal(t, len(mf[0].Metric), 2)
	assert.InDelta(t, 0.0, mf[0].Metric[0].Counter.GetValue(), 0.05)
	assert.InDelta(t, 0.0, mf[0].Metric[1].Counter.GetValue(), 0.05)

	mfout, err := outReg.Gather()
	assert.NoError(t, err)
	assert.Equal(t, len(mfout), 1)
	assert.Equal(t, len(mfout[0].Metric), 2)
	c1v, c2v := getC1C2Value(mfout[0])
	assert.InDelta(t, 12.0, c1v, 0.05)
	assert.InDelta(t, 6.0, c2v, 0.05)

	c1.Add(10.0)
	c2.Add(2.6)

	mf, err = inReg.Gather()
	assert.NoError(t, err)
	c1v, c2v = getC1C2Value(mf[0])
	assert.InDelta(t, 5.0, c1v, 0.05)
	assert.InDelta(t, 1.3, c2v, 0.05)
	mfout, err = outReg.Gather()
	assert.NoError(t, err)
	c1v, c2v = getC1C2Value(mfout[0])
	assert.InDelta(t, 22.0, c1v, 0.05)
	assert.InDelta(t, 8.6, c2v, 0.05)
}

func TestRateCounterVec_notDoAvg(t *testing.T) {
	factory := newRateCounterVec(CounterOpts{
		Subsystem: "test",
		Name:      "rate_counter",
	}, []string{"zzz", "aaa"}, false)

	require.Panics(t, func() { factory.WithLabelValues("12") })

	inReg := prom.NewRegistry()
	inReg.MustRegister(factory)

	factory.WithLabelValues("1", "2").Inc()

	mf, err := inReg.Gather()
	assert.NoError(t, err)
	assert.Equal(t, len(mf), 1)
	assert.Equal(t, len(mf[0].Metric), 1)
	// output the raw data.
	assert.InDelta(t, 1.0, mf[0].Metric[0].Counter.GetValue(), 0.005)
}

func TestOrigCounter(t *testing.T) {
	opts := CounterOpts{
		Subsystem: "test",
		Name:      "test_orig",
	}
	c1 := newCounter(opts)
	assert.Equal(t, c1, c1.CollectorToProm())
	factory := newCounterVec(opts, []string{"labelX"})
	assert.Equal(t, factory, factory.CollectorToProm())
}
