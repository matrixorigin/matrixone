package stats

import "go.uber.org/zap"

type LogExporter interface {
	Export() []zap.Field
}

// Family contains attributed related to a DevStats Family.
// Currently, it only has LogExporter
type Family struct {
	logExporter *LogExporter
}
