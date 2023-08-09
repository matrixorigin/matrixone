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

package main

import (
	"runtime/debug"
	"runtime/metrics"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

func init() {
	// free os memory periodically
	go func() {
		samples := []metrics.Sample{
			{
				Name: "/memory/classes/heap/free:bytes",
			},
			{
				Name: "/memory/classes/total:bytes",
			},
		}

		ticker := time.NewTicker(time.Millisecond * 200)
		var peak uint64
		for range ticker.C {
			metrics.Read(samples)
			if bs := samples[1].Value.Uint64(); bs > peak {
				peak = bs
			}
			if bs := samples[0].Value.Uint64(); bs > 1<<30 {
				logutil.Info("free os memory", zap.Any("free", bs), zap.Any("peak", peak))
				debug.FreeOSMemory()
			}
		}

	}()
}
