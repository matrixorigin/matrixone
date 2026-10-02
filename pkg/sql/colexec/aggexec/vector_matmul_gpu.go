//go:build gpu

// Copyright 2026 Matrix Origin
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

package aggexec

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/cuvs"
)

func init() {
	devices := sync.OnceValue(cuvs.BlockScaledMatmulDeviceCount)
	vectorMatmulGPU = &vectorMatmulGPUHooks{
		available: func() bool { return devices() > 0 },
		hostBytes: cuvs.BlockScaledMatmulHostBytes,
		create: func(format, dim, nq int, queryCells []byte, cellBytes, maxRows, topk int) (vectorMatmulEngine, error) {
			return cuvs.NewBlockScaledMatmul(format, dim, nq, queryCells, cellBytes, maxRows, topk)
		},
	}
}
