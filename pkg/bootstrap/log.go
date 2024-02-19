// Copyright 2022 Matrix Origin
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

package bootstrap

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"go.uber.org/zap"
)

var (
	logger        *log.MOLogger
	upgradeLogger *log.MOLogger
	once          sync.Once
)

func getLogger() *log.MOLogger {
	once.Do(initLogger)
	return logger
}

func getUpgradeLogger() *log.MOLogger {
	once.Do(initLogger)
	return upgradeLogger
}

func initLogger() {
	rt := runtime.ProcessLevelRuntime()
	if rt == nil {
		rt = runtime.DefaultRuntime()
	}
	logger = rt.Logger()
	upgradeLogger = logger.With(zap.String("module", "upgrade-framework"))
}
