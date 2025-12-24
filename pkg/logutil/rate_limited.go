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
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// RateLimitedLogger wraps a zap.Logger with rate limiting to prevent log storms.
// It limits log output by both count and time interval.
type RateLimitedLogger struct {
	logger *zap.Logger
	// callerSkipLogger is used for logging with correct caller frame.
	// It has CallerSkip(2) to account for the wrapper methods.
	callerSkipLogger *zap.Logger

	mu struct {
		sync.Mutex
		states map[string]*rateLimitState
	}
}

// rateLimitState tracks the state of a particular log message for rate limiting.
type rateLimitState struct {
	count      int64     // total occurrences since last log
	suppressed int64     // count of suppressed logs
	lastLog    time.Time // last time this message was logged
}

// RateLimitConfig configures rate limiting behavior.
type RateLimitConfig struct {
	// Interval is the minimum time between log outputs for the same message.
	// Default: 10 seconds
	Interval time.Duration
	// BurstCount is the number of logs allowed in burst before rate limiting kicks in.
	// First N occurrences are always logged.
	// Default: 3
	BurstCount int
	// SampleInterval is the count interval at which to log even when rate limited.
	// e.g., 100 means log every 100th occurrence.
	// Default: 100
	SampleInterval int
}

var (
	// DefaultRateLimitConfig is the default configuration for rate limiting.
	DefaultRateLimitConfig = RateLimitConfig{
		Interval:       10 * time.Second,
		BurstCount:     3,
		SampleInterval: 100,
	}
)

// NewRateLimitedLogger creates a new rate-limited logger wrapper.
// It automatically adds CallerSkip(2) to ensure correct caller information
// is displayed in logs (accounting for Error/Warn -> logWithConfig call chain).
func NewRateLimitedLogger(logger *zap.Logger) *RateLimitedLogger {
	rl := &RateLimitedLogger{
		logger:           logger,
		callerSkipLogger: logger.WithOptions(zap.AddCallerSkip(2)),
	}
	rl.mu.states = make(map[string]*rateLimitState)
	return rl
}

// Error logs at error level with rate limiting.
// The key parameter is used to identify unique log sources for rate limiting.
func (l *RateLimitedLogger) Error(key string, msg string, fields ...zap.Field) {
	l.logWithConfig(key, zap.ErrorLevel, msg, DefaultRateLimitConfig, fields...)
}

// ErrorWithConfig logs at error level with custom rate limit configuration.
func (l *RateLimitedLogger) ErrorWithConfig(key string, msg string, config RateLimitConfig, fields ...zap.Field) {
	l.logWithConfig(key, zap.ErrorLevel, msg, config, fields...)
}

// Warn logs at warn level with rate limiting.
func (l *RateLimitedLogger) Warn(key string, msg string, fields ...zap.Field) {
	l.logWithConfig(key, zap.WarnLevel, msg, DefaultRateLimitConfig, fields...)
}

// WarnWithConfig logs at warn level with custom rate limit configuration.
func (l *RateLimitedLogger) WarnWithConfig(key string, msg string, config RateLimitConfig, fields ...zap.Field) {
	l.logWithConfig(key, zap.WarnLevel, msg, config, fields...)
}

func (l *RateLimitedLogger) logWithConfig(key string, level zapcore.Level, msg string, config RateLimitConfig, fields ...zap.Field) {
	l.mu.Lock()
	state, ok := l.mu.states[key]
	if !ok {
		state = &rateLimitState{}
		l.mu.states[key] = state
	}
	l.mu.Unlock()

	count := atomic.AddInt64(&state.count, 1)
	now := time.Now()

	shouldLog := false

	// Always log the first N occurrences (burst)
	if count <= int64(config.BurstCount) {
		shouldLog = true
	} else if count%int64(config.SampleInterval) == 0 {
		// Log every Nth occurrence when rate limited
		shouldLog = true
	} else {
		// Check time-based rate limiting
		l.mu.Lock()
		if now.Sub(state.lastLog) >= config.Interval {
			shouldLog = true
		}
		l.mu.Unlock()
	}

	if shouldLog {
		l.mu.Lock()
		suppressed := atomic.SwapInt64(&state.suppressed, 0)
		state.lastLog = now
		l.mu.Unlock()

		if suppressed > 0 {
			fields = append(fields, zap.Int64("suppressed", suppressed))
		}
		fields = append(fields, zap.Int64("occurrence", count))

		if ce := l.callerSkipLogger.Check(level, msg); ce != nil {
			ce.Write(fields...)
		}
	} else {
		atomic.AddInt64(&state.suppressed, 1)
	}
}

// Reset resets the rate limiting state for a specific key.
func (l *RateLimitedLogger) Reset(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.mu.states, key)
}

// ResetAll resets all rate limiting states.
func (l *RateLimitedLogger) ResetAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mu.states = make(map[string]*rateLimitState)
}

// Logger returns the underlying zap.Logger.
func (l *RateLimitedLogger) Logger() *zap.Logger {
	return l.logger
}
