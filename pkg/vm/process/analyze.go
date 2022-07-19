// Copyright 2021 Matrix Origin
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

package process

import (
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

func NewAnalyzeInfo(nodeId int32) *AnalyzeInfo {
	return &AnalyzeInfo{
		NodeId:       nodeId,
		InputRows:    0,
		OutputRows:   0,
		TimeConsumed: 0,
		InputSize:    0,
		OutputSize:   0,
		MemorySize:   0,
	}
}

func (a *analyze) Start() {
	a.start = time.Now()
}

func (a *analyze) Stop() {
	if a.analInfo != nil {
		atomic.AddInt64(&a.analInfo.TimeConsumed, int64(time.Since(a.start)/time.Millisecond))
	}
}

func (a *analyze) Alloc(size int64) {
	if a.analInfo != nil {
		atomic.AddInt64(&a.analInfo.MemorySize, size)
	}
}

func (a *analyze) Input(bat *batch.Batch) {
	if a.analInfo != nil {
		atomic.AddInt64(&a.analInfo.InputSize, int64(bat.Size()))
		atomic.AddInt64(&a.analInfo.InputRows, int64(bat.Length()))
	}
}

func (a *analyze) Output(bat *batch.Batch) {
	if a.analInfo != nil {
		atomic.AddInt64(&a.analInfo.OutputSize, int64(bat.Size()))
		atomic.AddInt64(&a.analInfo.OutputRows, int64(bat.Length()))
	}
}
