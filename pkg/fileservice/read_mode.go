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

package fileservice

import (
	"math"
	"sync/atomic"

	"github.com/KimMachineGun/automemlimit/memlimit"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	defaultFullFilePreloadMaxObjectBytes = int64(32 << 20)
	defaultFullFilePreloadMaxInflight    = int64(16)
	defaultFullFilePreloadUnknownBytes   = int64(16 << 20)
	minFullFilePreloadBudgetBytes        = int64(512 << 20)
	maxFullFilePreloadBudgetBytes        = int64(2 << 30)
)

const (
	fullFilePreloadReasonCandidate   = "candidate"
	fullFilePreloadReasonPolicy      = "policy"
	fullFilePreloadReasonStreaming   = "streaming-read"
	fullFilePreloadReasonUnknownSize = "unknown-size"
	fullFilePreloadReasonLargeObject = "large-object"
	fullFilePreloadReasonPressure    = "pressure"
	fullFilePreloadReasonBudget      = "budget"
	fullFilePreloadReasonCountBudget = "count-budget"
	fullFilePreloadReasonBytesBudget = "bytes-budget"
)

var (
	fullFilePreloadMaxObjectBytes atomic.Int64
	fullFilePreloadMaxInflight    atomic.Int64
	fullFilePreloadBudgetBytes    atomic.Int64
	fullFilePreloadInflight       atomic.Int64
	fullFilePreloadInflightBytes  atomic.Int64
)

type fullFilePreloadToken struct {
	estimatedBytes int64
	released       atomic.Bool
}

func init() {
	fullFilePreloadMaxObjectBytes.Store(defaultFullFilePreloadMaxObjectBytes)
	fullFilePreloadMaxInflight.Store(defaultFullFilePreloadMaxInflight)
	fullFilePreloadBudgetBytes.Store(defaultFullFilePreloadBudget())
}

func defaultFullFilePreloadBudget() int64 {
	limit, err := memlimit.FromCgroup()
	if err == nil && limit > 0 && limit <= math.MaxInt64 {
		budget := int64(limit) * 4 / 100
		if budget < minFullFilePreloadBudgetBytes {
			return minFullFilePreloadBudgetBytes
		}
		if budget > maxFullFilePreloadBudgetBytes {
			return maxFullFilePreloadBudgetBytes
		}
		return budget
	}
	return maxFullFilePreloadBudgetBytes
}

func (i *IOVector) resolveReadMode() {
	if i.readModeResolved {
		return
	}
	i.readModeResolved = true
	i.readFullObject = false
	i.readModeReason = fullFilePreloadReasonCandidate

	if !i.Policy.CacheFullFile() || i.Policy.Any(SkipDiskCache) {
		i.readModeReason = fullFilePreloadReasonPolicy
		return
	}
	if i.hasStreamingReadEntry() {
		i.readModeReason = fullFilePreloadReasonStreaming
		return
	}

	i.readFullObject = true
}

func (i *IOVector) resolveS3ReadMode() {
	if i.s3ReadModeReady {
		return
	}
	i.resolveReadMode()
	i.s3ReadModeReady = true

	metric.ObserveFSFullFilePreloadAdmission("attempt", fullFilePreloadReasonCandidate)
	if !i.readFullObject {
		metric.ObserveFSFullFilePreloadAdmission("downgrade", i.readModeReason)
		return
	}

	estimatedBytes, knownSize := i.fullFilePreloadEstimatedBytes()
	if !knownSize {
		i.disableFullFilePreload(fullFilePreloadReasonUnknownSize)
		metric.ObserveFSFullFilePreloadAdmission("downgrade", fullFilePreloadReasonUnknownSize)
		return
	}
	if estimatedBytes > fullFilePreloadMaxObjectBytes.Load() {
		i.disableFullFilePreload(fullFilePreloadReasonLargeObject)
		metric.ObserveFSFullFilePreloadAdmission("downgrade", fullFilePreloadReasonLargeObject)
		return
	}
	if fullFilePreloadUnderPressure() {
		i.disableFullFilePreload(fullFilePreloadReasonPressure)
		metric.ObserveFSFullFilePreloadAdmission("downgrade", fullFilePreloadReasonPressure)
		return
	}
}

func (i *IOVector) acquireFullFilePreloadForS3() bool {
	if !i.readFullObject || i.preloadToken != nil {
		return true
	}
	estimatedBytes, knownSize := i.fullFilePreloadEstimatedBytes()
	if !knownSize {
		i.disableFullFilePreload(fullFilePreloadReasonUnknownSize)
		metric.ObserveFSFullFilePreloadAdmission("downgrade", fullFilePreloadReasonUnknownSize)
		return false
	}
	token, ok, reason := acquireFullFilePreloadToken(estimatedBytes)
	if !ok {
		i.disableFullFilePreload(reason)
		metric.ObserveFSFullFilePreloadAdmission("downgrade", reason)
		return false
	}
	i.preloadToken = token
	metric.ObserveFSFullFilePreloadAdmission("admit", fullFilePreloadReasonBudget)
	return true
}

func (i *IOVector) disableFullFilePreload(reason string) {
	i.readModeResolved = true
	i.s3ReadModeReady = true
	i.readFullObject = false
	i.readModeReason = reason
	i.preloadToken = nil
}

func (i *IOVector) resetReadMode() {
	i.readModeResolved = false
	i.s3ReadModeReady = false
	i.readFullObject = false
	i.readModeReason = ""
	i.preloadToken = nil
}

func (i *IOVector) hasStreamingReadEntry() bool {
	for idx := range i.Entries {
		if i.Entries[idx].WriterForRead != nil ||
			i.Entries[idx].ReadCloserForRead != nil {
			return true
		}
	}
	return false
}

func (i *IOVector) fullFilePreloadEstimatedBytes() (int64, bool) {
	if i.FullFileSizeHint > 0 {
		return i.FullFileSizeHint, true
	}
	return defaultFullFilePreloadUnknownBytes, false
}

func fullFilePreloadUnderPressure() bool {
	_, ok := memoryCachePressureTarget(100)
	return ok
}

func acquireFullFilePreloadToken(estimatedBytes int64) (*fullFilePreloadToken, bool, string) {
	if estimatedBytes <= 0 {
		estimatedBytes = defaultFullFilePreloadUnknownBytes
	}

	maxInflight := fullFilePreloadMaxInflight.Load()
	if maxInflight <= 0 {
		return nil, false, "count-budget"
	}
	for {
		current := fullFilePreloadInflight.Load()
		if current >= maxInflight {
			return nil, false, "count-budget"
		}
		if fullFilePreloadInflight.CompareAndSwap(current, current+1) {
			break
		}
	}

	budget := fullFilePreloadBudgetBytes.Load()
	for {
		current := fullFilePreloadInflightBytes.Load()
		if budget > 0 && current+estimatedBytes > budget {
			fullFilePreloadInflight.Add(-1)
			updateFullFilePreloadInflightMetrics()
			return nil, false, "bytes-budget"
		}
		if fullFilePreloadInflightBytes.CompareAndSwap(current, current+estimatedBytes) {
			break
		}
	}

	updateFullFilePreloadInflightMetrics()
	return &fullFilePreloadToken{estimatedBytes: estimatedBytes}, true, ""
}

func releaseFullFilePreloadToken(token *fullFilePreloadToken) {
	if token == nil || !token.released.CompareAndSwap(false, true) {
		return
	}
	fullFilePreloadInflight.Add(-1)
	fullFilePreloadInflightBytes.Add(-token.estimatedBytes)
	updateFullFilePreloadInflightMetrics()
}

func recordFullFilePreloadReadBytes(token *fullFilePreloadToken, actualBytes int64) {
	if token != nil && actualBytes > 0 {
		metric.AddFSFullFilePreloadReadBytes(actualBytes)
	}
}

func updateFullFilePreloadInflightMetrics() {
	metric.SetFSFullFilePreloadInflight(
		fullFilePreloadInflight.Load(),
		fullFilePreloadInflightBytes.Load(),
	)
}

func resetFullFilePreloadForTest(maxObjectBytes, maxInflight, budgetBytes int64) func() {
	oldMaxObjectBytes := fullFilePreloadMaxObjectBytes.Load()
	oldMaxInflight := fullFilePreloadMaxInflight.Load()
	oldBudgetBytes := fullFilePreloadBudgetBytes.Load()
	oldInflight := fullFilePreloadInflight.Load()
	oldInflightBytes := fullFilePreloadInflightBytes.Load()

	fullFilePreloadMaxObjectBytes.Store(maxObjectBytes)
	fullFilePreloadMaxInflight.Store(maxInflight)
	fullFilePreloadBudgetBytes.Store(budgetBytes)
	fullFilePreloadInflight.Store(0)
	fullFilePreloadInflightBytes.Store(0)
	updateFullFilePreloadInflightMetrics()

	return func() {
		fullFilePreloadMaxObjectBytes.Store(oldMaxObjectBytes)
		fullFilePreloadMaxInflight.Store(oldMaxInflight)
		fullFilePreloadBudgetBytes.Store(oldBudgetBytes)
		fullFilePreloadInflight.Store(oldInflight)
		fullFilePreloadInflightBytes.Store(oldInflightBytes)
		updateFullFilePreloadInflightMetrics()
		clearMemoryCachePressureTargetForTest()
	}
}
