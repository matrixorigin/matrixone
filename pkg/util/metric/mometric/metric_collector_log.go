package mometric

import (
	"context"
	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

type metricLogCollector struct {
	*bp.BaseBatchPipe[*pb.MetricFamily, string]
	opts collectorOpts
}

var _ MetricCollector = new(metricLogCollector)
var _ bp.PipeImpl[*pb.MetricFamily, string] = new(metricLogCollector)

func newMetricLogCollector(opts ...collectorOpt) MetricCollector {
	return &metricLogCollector{}
}

func (m *metricLogCollector) SendMetrics(ctx context.Context, mfs []*pb.MetricFamily) error {
	for _, mf := range mfs {
		if err := m.SendItem(ctx, mf); err != nil {
			return err
		}
	}
	return nil
}

func (m *metricLogCollector) NewItemBuffer(name string) bp.ItemBuffer[*pb.MetricFamily, string] {
	//TODO implement me
	panic("implement me")
}

func (m *metricLogCollector) NewItemBatchHandler(ctx context.Context) func(batch string) {
	//TODO implement me
	panic("implement me")
}
