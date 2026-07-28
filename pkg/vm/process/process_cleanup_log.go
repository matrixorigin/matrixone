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

package process

import (
	"sync"
	"time"

	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	pipelineCleanupWarnInterval       = 10 * time.Second
	pipelineCleanupWarnBurstCount     = int64(3)
	pipelineCleanupWarnSampleInterval = int64(100)
)

var pipelineCleanupWarnLimiter = newCleanupWarnLimiter()

type cleanupWarnLimiter struct {
	mu     sync.Mutex
	states map[string]*cleanupWarnState
}

type cleanupWarnState struct {
	count      int64
	suppressed int64
	lastLog    time.Time
}

func newCleanupWarnLimiter() *cleanupWarnLimiter {
	return &cleanupWarnLimiter{
		states: make(map[string]*cleanupWarnState),
	}
}

func (l *cleanupWarnLimiter) allow(key string) (bool, int64, int64) {
	if key == "" {
		key = "pipeline_cleanup"
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	state, ok := l.states[key]
	if !ok {
		state = &cleanupWarnState{}
		l.states[key] = state
	}

	state.count++
	count := state.count
	now := time.Now()

	shouldLog := count <= pipelineCleanupWarnBurstCount ||
		count%pipelineCleanupWarnSampleInterval == 0 ||
		now.Sub(state.lastLog) >= pipelineCleanupWarnInterval
	if !shouldLog {
		state.suppressed++
		return false, count, 0
	}

	suppressed := state.suppressed
	state.suppressed = 0
	state.lastLog = now
	return true, count, suppressed
}

func WarnPipelineCleanupf(proc *Process, key string, format string, args ...any) {
	if key == "" {
		key = "pipeline_cleanup"
	}
	metricv2.PipelineCleanupEventCounter.WithLabelValues(key).Inc()

	if proc == nil {
		return
	}

	allowed, occurrence, suppressed := pipelineCleanupWarnLimiter.allow(key)
	if !allowed {
		return
	}

	format += " occurrence=%d"
	args = append(args, occurrence)
	if suppressed > 0 {
		format += " suppressed=%d"
		args = append(args, suppressed)
	}

	proc.Warnf(proc.Ctx, format, args...)
}
