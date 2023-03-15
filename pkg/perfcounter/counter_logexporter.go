// Copyright 2023 Matrix Origin
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

package perfcounter

import (
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	"go.uber.org/zap"
)

type CounterLogExporter struct {
	counter *CounterSet
}

func NewCounterLogExporter(counter *CounterSet) stats.LogExporter {
	return &CounterLogExporter{
		counter: counter,
	}
}

// Export returns the fields and its values in loggable format.
func (c *CounterLogExporter) Export() []zap.Field {
	var fields []zap.Field

	reads := c.counter.Cache.Read.SwapW(0)
	hits := c.counter.Cache.Hit.SwapW(0)
	memReads := c.counter.Cache.MemRead.SwapW(0)
	memHits := c.counter.Cache.MemHit.SwapW(0)
	diskReads := c.counter.Cache.DiskRead.SwapW(0)
	diskHits := c.counter.Cache.DiskHit.SwapW(0)

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
