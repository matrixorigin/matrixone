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

package logutil

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Event is the logging contract for one operational outcome. Name and Message
// must be constants: Name is lowercase, dot-separated, and identifies exactly
// one outcome and one rate-limit population. Define an Event once per package
// and reuse it rather than assembling names at call sites.
//
// Event APIs coexist with the legacy global logging helpers. New retry, loop,
// control-plane, and invariant diagnostics should use Event.
type Event struct {
	Name    string
	Message string
}

// EventFieldBuilder creates fields only after the level is enabled and the
// event has passed its output budget. It is the required form when fields need
// formatting, hashing, String/Error calls, iteration, or serialization.
type EventFieldBuilder func() []zap.Field

// Debug emits a bounded debug event. Use DebugLazy for any derived fields.
func (e Event) Debug(fields ...zap.Field) bool { return e.log(zap.DebugLevel, fields) }

// Info emits a bounded audit or lifecycle event. Use InfoLazy for any derived fields.
func (e Event) Info(fields ...zap.Field) bool { return e.log(zap.InfoLevel, fields) }

// Warn emits a bounded recoverable-abnormality event. Use WarnLazy for any derived fields.
func (e Event) Warn(fields ...zap.Field) bool { return e.log(zap.WarnLevel, fields) }

// Error emits a bounded operator-actionable failure event. Use ErrorLazy for any derived fields.
func (e Event) Error(fields ...zap.Field) bool { return e.log(zap.ErrorLevel, fields) }

func (e Event) DebugLazy(build EventFieldBuilder) bool { return e.logLazy(zap.DebugLevel, build) }
func (e Event) InfoLazy(build EventFieldBuilder) bool  { return e.logLazy(zap.InfoLevel, build) }
func (e Event) WarnLazy(build EventFieldBuilder) bool  { return e.logLazy(zap.WarnLevel, build) }
func (e Event) ErrorLazy(build EventFieldBuilder) bool { return e.logLazy(zap.ErrorLevel, build) }

func (e Event) logLazy(level zapcore.Level, build EventFieldBuilder) bool {
	logger := GetGlobalLogger()
	if !logger.Core().Enabled(level) {
		return false
	}
	decision, ok := AllowEvent(e.Name, DefaultRateLimitConfig)
	if !ok {
		return false
	}
	var fields []zap.Field
	if build != nil {
		fields = build()
	}
	return e.write(logger, level, decision, fields)
}

func (e Event) log(level zapcore.Level, fields []zap.Field) bool {
	logger := GetGlobalLogger()
	if !logger.Core().Enabled(level) {
		return false
	}
	decision, ok := AllowEvent(e.Name, DefaultRateLimitConfig)
	if !ok {
		return false
	}
	return e.write(logger, level, decision, fields)
}

func (e Event) write(logger *zap.Logger, level zapcore.Level, decision RateLimitDecision, fields []zap.Field) bool {
	all := EventFieldsWithDecision(fields, decision)
	if ce := logger.WithOptions(zap.AddCallerSkip(3)).Check(level, e.Message); ce != nil {
		ce.Write(all...)
		return true
	}
	return false
}

// AllowEvent acquires the process-wide budget used by all Event APIs,
// including MOLogger Event methods. It intentionally ignores SampleInterval:
// an Event budget must not be bypassed by a count-only sample.
func AllowEvent(name string, config RateLimitConfig) (RateLimitDecision, bool) {
	return globalEventBudget.Allow(name, config)
}

// globalEventBudget has bounded state so a programming error that uses a
// dynamic name cannot itself create an unbounded memory leak. Excess names
// share one budget; valid Event names are low-cardinality constants.
var globalEventBudget = NewEventRateLimiter(RateLimitedLoggerConfig{MaxKeys: 1024})
