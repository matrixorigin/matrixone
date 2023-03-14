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

package mometric

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
	"time"
)

// MockService for testing stats Registry
type MockService struct {
	stats *MockStats
}

type MockStats struct {
	reads stats.Counter
	hits  stats.Counter
}

func NewMockService() *MockService {
	return &MockService{
		stats: &MockStats{},
	}
}

func (d *MockService) Do() {
	d.stats.reads.Add(2)
	d.stats.hits.Add(1)
}

func (d *MockService) Stats() *MockStats {
	return d.stats
}

// LogExporter for the Mock Service declared above
type MockServiceLogExporter struct {
	service *MockService
}

func NewMockServiceLogExporter(service *MockService) stats.LogExporter {
	return &MockServiceLogExporter{
		service: service,
	}
}

func (c *MockServiceLogExporter) Export() []zap.Field {
	var fields []zap.Field

	stats := c.service.Stats()

	reads := stats.reads.Swap()
	hits := stats.hits.Swap()

	fields = append(fields, zap.Any("reads", reads))
	fields = append(fields, zap.Any("hits", hits))

	return fields
}

func TestStatsLogWriter(t *testing.T) {
	// 1. Register Dev Stats
	service := NewMockService()
	serviceLogExporter := NewMockServiceLogExporter(service)
	stats.Register("MockServiceStats", stats.WithLogExporter(&serviceLogExporter))

	// 2. Start LogWriter
	c := newStatsLogWriter(&stats.DefaultRegistry, 2*time.Second)
	serviceCtx := context.Background()
	assert.True(t, c.Start(serviceCtx))

	// 3. Perform operations on Dev Stats
	service.Do()

	// 4. Wait for log to print in console.
	time.Sleep(6 * time.Second)

	// 5. Read from the console. Should print
	// 2023/03/14 11:39:30.579403 -0500 INFO mometric/stats_log_writer.go:83 MockServiceStats window values  {"reads": 2, "hits": 1}
	// 2023/03/14 11:39:32.579816 -0500 INFO mometric/stats_log_writer.go:83 MockServiceStats window values  {"reads": 0, "hits": 0}
	// 2023/03/14 11:39:34.583647 -0500 INFO mometric/stats_log_writer.go:83 MockServiceStats window values  {"reads": 0, "hits": 0}

	if ch, effect := c.Stop(true); effect {
		<-ch
	}
	println("StatsLogWriter has stopped gracefully.")
}
