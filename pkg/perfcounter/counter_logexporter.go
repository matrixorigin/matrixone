package perfcounter

import (
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"go.uber.org/zap"
)

type CounterLogExporter struct {
	counter *Counter
}

func NewCounterLogExporter(counter *Counter) metric.LogExporter {
	return &CounterLogExporter{
		counter: counter,
	}
}

// Export returns the fields and its values in loggable format.
func (c *CounterLogExporter) Export() []zap.Field {
	var fields []zap.Field

	reads := c.counter.Cache.Read.LoadC()
	hits := c.counter.Cache.Hit.LoadC()
	memReads := c.counter.Cache.MemRead.LoadC()
	memHits := c.counter.Cache.MemHit.LoadC()
	diskReads := c.counter.Cache.DiskRead.LoadC()
	diskHits := c.counter.Cache.DiskHit.LoadC()

	fields = append(fields, zap.Any("reads", reads))
	fields = append(fields, zap.Any("hits", hits))
	fields = append(fields, zap.Any("hit rate", float64(hits)/float64(reads)))
	fields = append(fields, zap.Any("mem reads", memReads))
	fields = append(fields, zap.Any("mem hits", memHits))
	fields = append(fields, zap.Any("mem hit rate", float64(memHits)/float64(memReads)))

	fields = append(fields, zap.Any("disk reads", diskReads))
	fields = append(fields, zap.Any("disk hits", diskHits))

	fields = append(fields, zap.Any("disk hit rate", float64(diskHits)/float64(diskReads)))

	return fields
}
