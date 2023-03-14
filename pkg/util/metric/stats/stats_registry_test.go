package stats

import (
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
)

// MockService for testing stats Registry
type MockService struct {
	stats *MockStats
}

type MockStats struct {
	reads Counter
	hits  Counter
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

func NewMockServiceLogExporter(service *MockService) LogExporter {
	return &MockServiceLogExporter{
		service: service,
	}
}

func (c *MockServiceLogExporter) Export() []zap.Field {
	var fields []zap.Field

	stats := c.service.Stats()

	reads := stats.reads.LoadC()
	hits := stats.hits.LoadC()

	fields = append(fields, zap.Any("reads", reads))
	fields = append(fields, zap.Any("hits", hits))

	return fields
}

func TestRegister(t *testing.T) {
	// 1. Initialize service
	service := NewMockService()

	// 2. Initialize LogExporter for the service
	serviceLogExporter := NewMockServiceLogExporter(service)

	// 3. Register LogExporter to the default stats registry
	Register("MockServiceStats", WithLogExporter(&serviceLogExporter))

	assert.Equal(t, 1, len(DefaultRegistry))
	assert.Equal(t, &serviceLogExporter, DefaultRegistry["MockServiceStats"].logExporter)
}

func TestExportLog(t *testing.T) {
	// 1. Initialize service
	service := NewMockService()

	// 2. Initialize LogExporter for the service
	serviceLogExporter := NewMockServiceLogExporter(service)

	// 3. Register LogExporter to the default stats registry
	Register("MockServiceStats", WithLogExporter(&serviceLogExporter))

	// 4. Let the service perform some operations
	service.Do()
	service.Do()
	service.Do()

	//5. Call ExportLog for exporting the snapshots of registered stats.
	result := DefaultRegistry.ExportLog()

	assert.Equal(t, 2, len(result["MockServiceStats"]))
	assert.Equal(t, zap.Any("reads", 6), result["MockServiceStats"][0])
	assert.Equal(t, zap.Any("hits", 3), result["MockServiceStats"][1])
}
