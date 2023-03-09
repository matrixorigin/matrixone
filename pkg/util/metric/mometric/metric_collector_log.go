package mometric

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

// TODO: Once this class is mature, it will be moved inside metric_collector.go
type metricLogCollector struct {
	*bp.BaseBatchPipe[*pb.MetricFamily, string]
	opts collectorOpts
}

var _ MetricCollector = new(metricLogCollector)
var _ bp.PipeImpl[*pb.MetricFamily, string] = new(metricLogCollector)

func newMetricLogCollector(opts ...collectorOpt) MetricCollector {
	initOpts := defaultCollectorOpts()
	for _, o := range opts {
		o.ApplyTo(&initOpts)
	}
	c := &metricLogCollector{
		opts: initOpts,
	}
	pipeOpts := []bp.BaseBatchPipeOpt{bp.PipeWithBatchWorkerNum(c.opts.sqlWorkerNum), bp.PipeWithBufferWorkerNum(1)}
	base := bp.NewBaseBatchPipe[*pb.MetricFamily, string](c, pipeOpts...)
	c.BaseBatchPipe = base
	return c

}

func (m *metricLogCollector) SendMetrics(ctx context.Context, mfs []*pb.MetricFamily) error {
	for _, mf := range mfs {
		if err := m.SendItem(ctx, mf); err != nil {
			return err
		}
	}
	return nil
}

func (m *metricLogCollector) NewItemBuffer(_ string) bp.ItemBuffer[*pb.MetricFamily, string] {
	//TODO: Check if I can reuse mfset by modifying some parts of it.
	// I believe mfset is used only metricCollector (deprecated) and not in metricFsCollector.
	return &mfset{
		Reminder:        bp.NewConstantClock(m.opts.flushInterval),
		metricThreshold: m.opts.metricThreshold,
		sampleThreshold: m.opts.sampleThreshold,
	}
}

func (m *metricLogCollector) NewItemBatchHandler(ctx context.Context) func(batch string) {
	return func(batch string) {
		logutil.Info(batch)
	}
}
