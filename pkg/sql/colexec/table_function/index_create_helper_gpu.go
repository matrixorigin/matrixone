//go:build gpu

// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// quantizationBytes is the on-device element size of one vector component for a storage
// quantization. It sizes the per-row cost used to bound a build against VRAM.
//
// Only the GPU create paths size a build this way, so this lives behind the gpu tag
// with them rather than in the shared (untagged) helper.
func quantizationBytes(qt metric.QuantizationType) uint64 {
	switch qt {
	case metric.Quantization_F16:
		return 2
	case metric.Quantization_INT8, metric.Quantization_UINT8:
		return 1
	default: // Quantization_F32
		return 4
	}
}
