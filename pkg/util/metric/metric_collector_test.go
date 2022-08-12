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
	"regexp"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

type dummySqlExecutor struct {
	opts ie.SessionOverrideOptions
	ch   chan<- string
}

func (e *dummySqlExecutor) ApplySessionOverride(opts ie.SessionOverrideOptions) {
	e.opts = opts
}

func (e *dummySqlExecutor) Exec(sql string, opts ie.SessionOverrideOptions) error {
	select {
	case e.ch <- sql:
	default:
	}
	return nil
}

func (e *dummySqlExecutor) Query(sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
	return nil
}

func newExecutorFactory(sqlch chan string) func() ie.InternalExecutor {
	return func() ie.InternalExecutor {
		return &dummySqlExecutor{
			opts: ie.NewOptsBuilder().Finish(),
			ch:   sqlch,
		}
	}
}

func TestCollectorOpts(t *testing.T) {
	c := newMetricCollector(
		nil, // this nil pointer won't be touched when SqlWorkerNum is set to 0
		WithFlushInterval(time.Second),
		WithMetricThreshold(3),
		WithSampleThreshold(10),
		WithSqlWorkerNum(0),
	).(*metricCollector)
	o := c.opts
	if o.flushInterval != time.Second || o.metricThreshold != 3 || o.sampleThreshold != 10 {
		t.Errorf("collectorOpts doesn't apply correctly")
	}
}

func TestCollector(t *testing.T) {
	sqlch := make(chan string, 100)
	factory := newExecutorFactory(sqlch)
	collector := newMetricCollector(factory, WithFlushInterval(200*time.Millisecond), WithMetricThreshold(2))
	collector.Start()
	defer collector.Stop(false)
	names := []string{"m1", "m2"}
	nodes := []int32{1, 2}
	roles := []string{"ping", "pong"}
	ts := int64(types.Now())
	go func() {
		_ = collector.SendMetrics(context.TODO(), []*pb.MetricFamily{
			{Name: names[0], Type: pb.MetricType_COUNTER, Node: nodes[0], Role: roles[0], Metric: []*pb.Metric{
				{
					Counter: &pb.Counter{Value: 12.0}, Collecttime: ts,
				},
			}},
			{Name: names[1], Type: pb.MetricType_RAWHIST, Metric: []*pb.Metric{
				{
					Label:   []*pb.LabelPair{{Name: "sqltype", Value: "select"}, {Name: "internal", Value: "false"}},
					RawHist: &pb.RawHist{Samples: []*pb.Sample{{Datetime: ts, Value: 12.0}, {Datetime: ts, Value: 12.0}}},
				},
			}},
		})

		_ = collector.SendMetrics(context.TODO(), []*pb.MetricFamily{
			{Name: names[0], Type: pb.MetricType_COUNTER, Node: nodes[1], Role: roles[1], Metric: []*pb.Metric{
				{
					Counter: &pb.Counter{Value: 21.0}, Collecttime: ts,
				},
				{
					Counter: &pb.Counter{Value: 66.0}, Collecttime: ts,
				},
			}},
		})
	}()
	instant := time.Now()
	valuesRe := regexp.MustCompile(`\([^)]*\),?\s?`) // find pattern like (1,2,3)
	nameRe := regexp.MustCompile(`\.(\w+)\svalues`)  // find table name
	nameAndValueCnt := func(s string) (name string, cnt int) {
		cnt = len(valuesRe.FindAllString(s, -1))
		matches := nameRe.FindStringSubmatch(s)
		if len(matches) > 1 {
			name = matches[1]
		} else {
			name = "<nil>"
		}
		return name, cnt
	}

	name, cnt := nameAndValueCnt(<-sqlch)
	if name != names[0] || cnt != 3 {
		t.Errorf("m1 metric should be flushed first with 3 rows, got %s with %d rows", name, cnt)
	}

	sql := <-sqlch
	if time.Since(instant) < 200*time.Millisecond {
		t.Errorf("m2 should be flushed after a period")
	}
	name, cnt = nameAndValueCnt(sql)
	if name != names[1] || cnt != 2 {
		t.Errorf("m2 metric should be flushed first with 2 rows, got %s with %d rows", name, cnt)
	}
}
