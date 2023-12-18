package fileservice

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[tracePoint](
		newTracePoint,
		resetTracePoint,
		reuse.DefaultOptions[tracePoint]().
			WithEnableChecker())
}
