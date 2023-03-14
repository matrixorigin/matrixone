package stats

import "go.uber.org/zap"

type LogExporter interface {
	Export() []zap.Field
}
