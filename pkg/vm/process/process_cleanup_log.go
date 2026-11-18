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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap/zapcore"
)

func WarnPipelineCleanupf(proc *Process, key string, format string, args ...any) {
	logPipelineCleanupf(proc, zapcore.WarnLevel, key, format, args...)
}

func DebugPipelineCleanupf(proc *Process, key string, format string, args ...any) {
	logPipelineCleanupf(proc, zapcore.DebugLevel, key, format, args...)
}

func logPipelineCleanupf(proc *Process, level zapcore.Level, key string, format string, args ...any) {
	if key == "" {
		key = "pipeline_cleanup"
	}
	metricv2.PipelineCleanupEventCounter.WithLabelValues(key).Inc()
	if proc == nil || proc.Base == nil || proc.Base.logger == nil ||
		!proc.Base.SessionInfo.LogLevel.Enabled(level) || !proc.Base.logger.Enabled(level) {
		return
	}

	decision, allowed := logutil.AllowEvent("pipeline.cleanup."+level.String()+"."+key, logutil.DefaultRateLimitConfig)
	if !allowed {
		return
	}
	format += " occurrence=%d"
	args = append(args, decision.Occurrence)
	if decision.Suppressed > 0 {
		format += " suppressed=%d"
		args = append(args, decision.Suppressed)
	}
	proc.logf(proc.Ctx, level, format, args...)
}

// LogPipelineTerminalNotEnqueued reports a false terminal send without
// interpreting it as a timeout. An initialized edge can deliver its durable
// terminal state through Done even if a terminal signal could not enter Ch2.
func LogPipelineTerminalNotEnqueued(proc *Process, key string, reg *WaitRegister, signal PipelineSignal) {
	if reg == nil || reg.Ch2 == nil {
		WarnPipelineCleanupf(proc, key, "%s: terminal %s has no receiver channel: err=%v",
			key, signal.EventType, signal.TerminalErr())
		return
	}
	chLen, chCap := WaitRegisterChannelState(reg)
	select {
	case <-reg.Done():
		DebugPipelineCleanupf(proc, key, "%s: terminal %s not enqueued on a terminal edge: channel_len=%d channel_cap=%d err=%v",
			key, signal.EventType, chLen, chCap, signal.TerminalErr())
	default:
		WarnPipelineCleanupf(proc, key, "%s: terminal %s not enqueued and receiver is still active: channel_len=%d channel_cap=%d err=%v",
			key, signal.EventType, chLen, chCap, signal.TerminalErr())
	}
}
