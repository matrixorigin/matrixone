// Copyright 2021 - 2022 Matrix Origin
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
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	overflowEvent = "event-budget-overflow"
	// maxEventNameBytes keeps the retained limiter key space bounded even when
	// a caller mistakenly derives an Event name from untrusted input.
	maxEventNameBytes = 128

	FieldEvent             = "event"
	FieldOccurrence        = "occurrence"
	FieldSuppressed        = "suppressed"
	FieldRateLimitOverflow = "event-budget-overflow"
)

// RateLimitConfig governs a single stable event population. Event APIs use a
// strict time budget; SampleInterval is honored only by the legacy
// RateLimitedLogger adapter for compatibility with its existing callers.
type RateLimitConfig struct {
	Interval       time.Duration
	BurstCount     int
	SampleInterval int
}

var DefaultRateLimitConfig = RateLimitConfig{
	Interval:       10 * time.Second,
	BurstCount:     3,
	SampleInterval: 100,
}

// RateLimitedLoggerConfig bounds retained event state. Overflow events share
// one state, so misuse of dynamic keys cannot grow process memory.
type RateLimitedLoggerConfig struct {
	MaxKeys int
}

var DefaultRateLimitedLoggerConfig = RateLimitedLoggerConfig{MaxKeys: 128}

type rateLimitState struct {
	count      int64
	suppressed int64
	lastLog    time.Time
}

// EventRateLimiter is shared by related loggers (With, Named, WithContext)
// so derived loggers cannot evade the same event's storm budget.
type EventRateLimiter struct {
	mu struct {
		sync.Mutex
		states   map[string]*rateLimitState
		overflow rateLimitState
	}
	maxKeys     int
	maxKeyBytes int
	now         func() time.Time
}

type RateLimitDecision struct {
	Event      string
	Occurrence int64
	Suppressed int64
	Overflow   bool
}

func NewEventRateLimiter(config RateLimitedLoggerConfig) *EventRateLimiter {
	if config.MaxKeys <= 0 {
		config.MaxKeys = DefaultRateLimitedLoggerConfig.MaxKeys
	}
	limiter := &EventRateLimiter{
		maxKeys:     config.MaxKeys,
		maxKeyBytes: maxEventNameBytes,
		now:         time.Now,
	}
	limiter.mu.states = make(map[string]*rateLimitState)
	return limiter
}

func (l *EventRateLimiter) Allow(key string, config RateLimitConfig) (RateLimitDecision, bool) {
	return l.allow(key, config, false)
}

func (l *EventRateLimiter) allowLegacy(key string, config RateLimitConfig) (RateLimitDecision, bool) {
	return l.allow(key, config, true)
}

func (l *EventRateLimiter) allow(key string, config RateLimitConfig, allowSampleInterval bool) (RateLimitDecision, bool) {
	config = config.normalized()
	if key == "" {
		key = "unknown"
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	state, overflow := l.stateLocked(key)
	state.count++
	now := l.now()
	shouldLog := state.count <= int64(config.BurstCount) || now.Sub(state.lastLog) >= config.Interval
	if allowSampleInterval && config.SampleInterval > 0 && state.count%int64(config.SampleInterval) == 0 {
		shouldLog = true
	}
	if !shouldLog {
		state.suppressed++
		return RateLimitDecision{}, false
	}
	decision := RateLimitDecision{
		Event:      key,
		Occurrence: state.count,
		Suppressed: state.suppressed,
		Overflow:   overflow,
	}
	if overflow {
		decision.Event = overflowEvent
	}
	state.suppressed = 0
	state.lastLog = now
	return decision, true
}

func (l *EventRateLimiter) stateLocked(key string) (*rateLimitState, bool) {
	if len(key) > l.maxKeyBytes {
		return &l.mu.overflow, true
	}
	if state := l.mu.states[key]; state != nil {
		return state, false
	}
	if len(l.mu.states) >= l.maxKeys {
		return &l.mu.overflow, true
	}
	state := &rateLimitState{}
	// Copy before retaining. A short substring can otherwise keep a much
	// larger caller-owned backing buffer alive for the lifetime of this state.
	l.mu.states[strings.Clone(key)] = state
	return state, false
}

func (l *EventRateLimiter) Reset(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.mu.states, key)
}

func (l *EventRateLimiter) ResetAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.states = make(map[string]*rateLimitState)
	l.mu.overflow = rateLimitState{}
}

func (l *EventRateLimiter) StateCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.mu.states)
}

func (config RateLimitConfig) normalized() RateLimitConfig {
	if config.Interval <= 0 {
		config.Interval = DefaultRateLimitConfig.Interval
	}
	if config.BurstCount <= 0 {
		config.BurstCount = DefaultRateLimitConfig.BurstCount
	}
	return config
}

// RateLimitedLogger is the zap adapter for callers that do not have an
// MOLogger. New repeated call sites should prefer Event or MOLogger Event APIs.
type RateLimitedLogger struct {
	logger           *zap.Logger
	callerSkipLogger *zap.Logger
	limiter          *EventRateLimiter
}

func NewRateLimitedLogger(logger *zap.Logger) *RateLimitedLogger {
	return NewRateLimitedLoggerWithConfig(logger, DefaultRateLimitedLoggerConfig)
}

func NewRateLimitedLoggerWithConfig(logger *zap.Logger, config RateLimitedLoggerConfig) *RateLimitedLogger {
	if logger == nil {
		panic("rate limited logger is nil")
	}
	return &RateLimitedLogger{
		logger:           logger,
		callerSkipLogger: logger.WithOptions(zap.AddCallerSkip(2)),
		limiter:          NewEventRateLimiter(config),
	}
}

func (l *RateLimitedLogger) Error(key, msg string, fields ...zap.Field) {
	l.log(key, zap.ErrorLevel, msg, DefaultRateLimitConfig, fields...)
}
func (l *RateLimitedLogger) ErrorWithConfig(key, msg string, config RateLimitConfig, fields ...zap.Field) {
	l.log(key, zap.ErrorLevel, msg, config, fields...)
}
func (l *RateLimitedLogger) Warn(key, msg string, fields ...zap.Field) {
	l.log(key, zap.WarnLevel, msg, DefaultRateLimitConfig, fields...)
}
func (l *RateLimitedLogger) WarnWithConfig(key, msg string, config RateLimitConfig, fields ...zap.Field) {
	l.log(key, zap.WarnLevel, msg, config, fields...)
}
func (l *RateLimitedLogger) Info(key, msg string, fields ...zap.Field) {
	l.log(key, zap.InfoLevel, msg, DefaultRateLimitConfig, fields...)
}
func (l *RateLimitedLogger) Debug(key, msg string, fields ...zap.Field) {
	l.log(key, zap.DebugLevel, msg, DefaultRateLimitConfig, fields...)
}

func (l *RateLimitedLogger) log(key string, level zapcore.Level, msg string, config RateLimitConfig, fields ...zap.Field) {
	if !l.callerSkipLogger.Core().Enabled(level) {
		return
	}
	decision, ok := l.limiter.allowLegacy(key, config)
	if !ok {
		return
	}
	out := EventFieldsWithDecision(fields, decision)
	if ce := l.callerSkipLogger.Check(level, msg); ce != nil {
		ce.Write(out...)
	}
}

// EventFieldsWithDecision appends the standard event, occurrence, suppression,
// and overflow fields without mutating the caller's slice. It is used by
// global Event, MOLogger, and RateLimitedLogger so their record shape matches.
func EventFieldsWithDecision(fields []zap.Field, decision RateLimitDecision) []zap.Field {
	out := append([]zap.Field(nil), fields...)
	out = append(out, zap.String(FieldEvent, decision.Event), zap.Int64(FieldOccurrence, decision.Occurrence))
	if decision.Suppressed > 0 {
		out = append(out, zap.Int64(FieldSuppressed, decision.Suppressed))
	}
	if decision.Overflow {
		out = append(out, zap.Bool(FieldRateLimitOverflow, true))
	}
	return out
}

func (l *RateLimitedLogger) Reset(key string)    { l.limiter.Reset(key) }
func (l *RateLimitedLogger) ResetAll()           { l.limiter.ResetAll() }
func (l *RateLimitedLogger) StateCount() int     { return l.limiter.StateCount() }
func (l *RateLimitedLogger) Logger() *zap.Logger { return l.logger }
