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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

var Named sync.Map

func NameForNode(uuid string) string {
	return "node:" + uuid
}

func LogNodeCacheStats(uuid string) {
	v, ok := Named.Load(NameForNode(uuid))
	if !ok {
		return
	}
	counter := v.(*CounterSet)
	logutil.Debug("cache stats",
		zap.Any("node", uuid),
		zap.Any("type", "memory"),
		zap.Any("used", counter.FileService.Cache.Memory.Used.Load()),
		zap.Any("free", counter.FileService.Cache.Memory.Available.Load()),
		zap.Any("hit ratio", float64(counter.FileService.Cache.Memory.Hit.Load())/
			float64(counter.FileService.Cache.Memory.Read.Load())),
	)
	logutil.Debug("cache stats",
		zap.Any("node", uuid),
		zap.Any("type", "disk"),
		zap.Any("hit ratio", float64(counter.FileService.Cache.Disk.Hit.Load())/
			float64(counter.FileService.Cache.Disk.Read.Load())),
	)
}
