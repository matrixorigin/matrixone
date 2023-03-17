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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

	reads := stats.reads.SwapW(0)
	hits := stats.hits.SwapW(0)

	fields = append(fields, zap.Any("reads", reads))
	fields = append(fields, zap.Any("hits", hits))

	return fields
}

func TestStatsLogWriter(t *testing.T) {
	// 1. Register Dev Stats
	service := NewMockService()
	serviceLogExporter := NewMockServiceLogExporter(service)
	stats.Register("MockServiceStats", stats.WithLogExporter(serviceLogExporter))

	//2.1 Setup a Runtime
	runtime.SetupProcessLevelRuntime(runtime.NewRuntime(metadata.ServiceType_CN, "test", logutil.GetGlobalLogger()))

	//2.2 Create custom Hook logger
	var writtenLogs []zapcore.Entry
	customLogger := runtime.ProcessLevelRuntime().Logger().WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		writtenLogs = append(writtenLogs, entry)
		return nil
	}))

	// 2.3 Start the LogWriter
	c := newStatsLogWriter(&stats.DefaultRegistry, customLogger, 2*time.Second)
	serviceCtx := context.Background()
	assert.True(t, c.Start(serviceCtx))

	// 3. Perform operations on Dev Stats
	service.Do()

	// 4. Wait for log to print in console.
	time.Sleep(6 * time.Second)

	// 5. Stop the LogWriter
	if ch, effect := c.Stop(true); effect {
		<-ch
	}
	println("StatsLogWriter has stopped gracefully.")

	//6. Validate the log printed.
	assert.Equal(t, 3, len(writtenLogs))
	for _, log := range writtenLogs {
		assert.Contains(t, log.Message, "stats ")
	}

	// 7. (Optional) Read from the console and validate the log. Example log:
	// 2023/03/15 02:37:31.767463 -0500 INFO cn-service mometric/stats_log_writer.go:86 MockServiceStats stats  {"uuid": "test", "reads": 2, "hits": 1}
	// 2023/03/15 02:37:33.767659 -0500 INFO cn-service mometric/stats_log_writer.go:86 MockServiceStats stats  {"uuid": "test", "reads": 0, "hits": 0}
	// 2023/03/15 02:37:35.767608 -0500 INFO cn-service mometric/stats_log_writer.go:86 MockServiceStats stats  {"uuid": "test", "reads": 0, "hits": 0}
	// StatsLogWriter has stopped gracefully.

}
