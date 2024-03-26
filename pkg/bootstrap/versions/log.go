package versions

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
)

var (
	logger *log.MOLogger
	once   sync.Once
)

func getLogger() *log.MOLogger {
	once.Do(initLogger)
	return logger
}

func initLogger() {
	rt := runtime.ProcessLevelRuntime()
	if rt == nil {
		rt = runtime.DefaultRuntime()
	}
	logger = rt.Logger()
}
