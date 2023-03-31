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
	"strings"

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

	fields = append(fields, zap.Any("FileService Cache Hit Rate",
		float64(c.counter.FileService.Cache.Hit.LoadW())/
			float64(c.counter.FileService.Cache.Read.LoadW())))
	fields = append(fields, zap.Any("FileService Cache Memory Hit Rate",
		float64(c.counter.FileService.Cache.Memory.Hit.LoadW())/
			float64(c.counter.FileService.Cache.Memory.Read.LoadW())))
	fields = append(fields, zap.Any("FileService Cache Disk Hit Rate",
		float64(c.counter.FileService.Cache.Disk.Hit.LoadW())/
			float64(c.counter.FileService.Cache.Disk.Read.LoadW())))

	// all fields in CounterSet
	_ = c.counter.IterFields(func(path []string, counter *stats.Counter) error {
		fields = append(fields, zap.Any(strings.Join(path, "."), counter.SwapW(0)))
		return nil
	})

	return fields
}
