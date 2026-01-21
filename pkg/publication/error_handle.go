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

package publication

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ErrorMetadata stores error metadata
type ErrorMetadata struct {
	IsRetryable bool      // Whether this error is retryable
	RetryCount  int       // Number of retry attempts
	FirstSeen   time.Time // When first seen
	LastSeen    time.Time // When last seen
	Message     string    // Error message
}

// Parse parses error metadata from string
// Format:
//   - Retryable: "R:count:firstSeen:lastSeen:message"
//   - Non-retryable: "N:firstSeen:message"
func Parse(errMsg string) *ErrorMetadata {
	if errMsg == "" {
		return nil
	}

	parts := strings.SplitN(errMsg, ":", 5)

	// Retryable format: "R:count:firstSeen:lastSeen:message"
	if len(parts) >= 5 && parts[0] == "R" {
		retryCount, _ := strconv.Atoi(parts[1])
		firstSeen, _ := strconv.ParseInt(parts[2], 10, 64)
		lastSeen, _ := strconv.ParseInt(parts[3], 10, 64)

		return &ErrorMetadata{
			IsRetryable: true,
			RetryCount:  retryCount,
			FirstSeen:   time.Unix(firstSeen, 0),
			LastSeen:    time.Unix(lastSeen, 0),
			Message:     parts[4],
		}
	}

	// Non-retryable format: "N:firstSeen:message"
	if len(parts) >= 3 && parts[0] == "N" {
		firstSeen, _ := strconv.ParseInt(parts[1], 10, 64)
		message := strings.Join(parts[2:], ":")

		return &ErrorMetadata{
			IsRetryable: false,
			RetryCount:  0,
			FirstSeen:   time.Unix(firstSeen, 0),
			LastSeen:    time.Unix(firstSeen, 0),
			Message:     message,
		}
	}
	// Legacy format: just the message (assume non-retryable)
	return &ErrorMetadata{
		IsRetryable: false,
		RetryCount:  0,
		FirstSeen:   time.Now(),
		LastSeen:    time.Now(),
		Message:     errMsg,
	}
}

const (
	// RetryThreshold is the maximum number of retries before stopping
	RetryThreshold = 10
)

// BuildErrorMetadata builds new metadata based on old metadata and new error
// It uses the classifier to determine if the error is retryable
// Returns the error metadata and a boolean indicating if retry should continue
// Retry will stop if retry count exceeds RetryThreshold
func BuildErrorMetadata(old *ErrorMetadata, err error, classifier ErrorClassifier) (*ErrorMetadata, bool) {
	now := time.Now()
	message := err.Error()

	// Determine if error is retryable using classifier
	isRetryable := false
	if classifier != nil {
		isRetryable = classifier.IsRetryable(err)
	}

	// New error (no previous metadata)
	if old == nil {
		retryCount := 1
		shouldRetry := isRetryable && retryCount <= RetryThreshold
		return &ErrorMetadata{
			IsRetryable: isRetryable && shouldRetry,
			RetryCount:  retryCount,
			FirstSeen:   now,
			LastSeen:    now,
			Message:     message,
		}, shouldRetry
	}

	// Check if previous retry count exceeded threshold
	// If so, reset count even if error type is the same
	if old.RetryCount > RetryThreshold {
		retryCount := 1
		shouldRetry := isRetryable && retryCount <= RetryThreshold
		return &ErrorMetadata{
			IsRetryable: isRetryable && shouldRetry,
			RetryCount:  retryCount,
			FirstSeen:   now,
			LastSeen:    now,
			Message:     message,
		}, shouldRetry
	}

	// Same error type, increment retry count
	if old.IsRetryable == isRetryable {
		retryCount := old.RetryCount + 1
		shouldRetry := isRetryable && retryCount <= RetryThreshold
		return &ErrorMetadata{
			IsRetryable: isRetryable && shouldRetry,
			RetryCount:  retryCount,
			FirstSeen:   old.FirstSeen, // Preserve first seen time
			LastSeen:    now,           // Update last seen time
			Message:     message,
		}, shouldRetry
	}

	// Error type changed, reset count
	retryCount := 1
	shouldRetry := isRetryable && retryCount <= RetryThreshold
	return &ErrorMetadata{
		IsRetryable: isRetryable && shouldRetry,
		RetryCount:  retryCount,
		FirstSeen:   now,
		LastSeen:    now,
		Message:     message,
	}, shouldRetry
}

// Format formats error metadata to string
func (m *ErrorMetadata) Format() string {
	if m == nil {
		return ""
	}
	if m.IsRetryable {
		return fmt.Sprintf("R:%d:%d:%d:%s",
			m.RetryCount,
			m.FirstSeen.Unix(),
			m.LastSeen.Unix(),
			m.Message,
		)
	}
	return fmt.Sprintf("N:%d:%s",
		m.FirstSeen.Unix(),
		m.Message,
	)
}

// DefaultClassifier recognises common transient network errors and connection issues.
// It handles basic network retry scenarios like connection timeouts, EOF errors, etc.
type DefaultClassifier struct{}

// IsRetryable implements ErrorClassifier.
func (DefaultClassifier) IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Check for EOF errors
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Check for context deadline exceeded
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
		// Check for temporary network errors
		type temporary interface {
			Temporary() bool
		}
		if tmp, ok := netErr.(temporary); ok && tmp.Temporary() {
			return true
		}
	}

	// Check error message for common retryable network patterns
	errMsg := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"connection reset",
		"connection timed out",
		"connection timeout",
		"dial tcp",
		"i/o timeout",
		"broken pipe",
		"tls handshake timeout",
		"use of closed network connection",
		"temporary failure",
		"service unavailable",
		"timeout",
		"network error",
		"rpc error",
		"backend",
		"unavailable",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

// MySQLErrorClassifier recognises transient MySQL errors that are worth retrying.
type MySQLErrorClassifier struct{}

var mysqlRetryableErrorCodes = map[uint16]struct{}{
	// Lock wait timeout exceeded; try restarting transaction
	1205: {},
	// Deadlock found when trying to get lock; try restarting transaction
	1213: {},
	// Server closed the connection
	2006: {},
	// Lost connection to MySQL server during query
	2013: {},
	// Can't connect to MySQL server on host (network issues)
	2003: {},
	// Not enough privilege or connection handshake issues that can be transient
	1043: {},
}

// IsRetryable implements ErrorClassifier.
func (MySQLErrorClassifier) IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, driver.ErrBadConn) {
		return true
	}

	var mysqlErr *gomysql.MySQLError
	if errors.As(err, &mysqlErr) {
		if _, ok := mysqlRetryableErrorCodes[mysqlErr.Number]; ok {
			return true
		}
	}

	return false
}

// CommitErrorClassifier recognises errors that are retryable during commit operations.
// It checks for RC mode transaction retry errors (ErrTxnNeedRetry, ErrTxnNeedRetryWithDefChanged).
type CommitErrorClassifier struct{}

// IsRetryable implements ErrorClassifier.
func (CommitErrorClassifier) IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Check for RC mode transaction retry errors
	// These errors indicate that the transaction needs to be retried in RC mode
	if moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
		return true
	}

	return false
}

var utInjectionErrors = map[string]struct{}{
	"ut injection: publicationSnapshotFinished": {},
	"ut injection: commit failed retryable": {},
}

// UTInjectionClassifier recognises UT injection errors that are retryable.
type UTInjectionClassifier struct{}

// IsRetryable implements ErrorClassifier.
func (UTInjectionClassifier) IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())
	for pattern := range utInjectionErrors {
		patternLower := strings.ToLower(pattern)
		if strings.Contains(errMsg, patternLower) {
			return true
		}
	}

	return false
}

// DownstreamCommitClassifier is used when committing to downstream.
// It combines default, mysql, commit, and ut injection classifiers.
type DownstreamCommitClassifier struct {
	MultiClassifier
}

// NewDownstreamCommitClassifier creates a new DownstreamCommitClassifier.
func NewDownstreamCommitClassifier() *DownstreamCommitClassifier {
	return &DownstreamCommitClassifier{
		MultiClassifier: MultiClassifier{
			DefaultClassifier{},
			MySQLErrorClassifier{},
			CommitErrorClassifier{},
			UTInjectionClassifier{},
		},
	}
}

// UpstreamConnectionClassifier is used when connecting to upstream.
// It combines default, mysql, and commit classifiers.
type UpstreamConnectionClassifier struct {
	MultiClassifier
}

// NewUpstreamConnectionClassifier creates a new UpstreamConnectionClassifier.
func NewUpstreamConnectionClassifier() *UpstreamConnectionClassifier {
	return &UpstreamConnectionClassifier{
		MultiClassifier: MultiClassifier{
			DefaultClassifier{},
			MySQLErrorClassifier{},
			CommitErrorClassifier{},
		},
	}
}

// DownstreamConnectionClassifier is used when connecting to downstream.
// It combines default and mysql classifiers.
type DownstreamConnectionClassifier struct {
	MultiClassifier
}

// NewDownstreamConnectionClassifier creates a new DownstreamConnectionClassifier.
func NewDownstreamConnectionClassifier() *DownstreamConnectionClassifier {
	return &DownstreamConnectionClassifier{
		MultiClassifier: MultiClassifier{
			DefaultClassifier{},
			MySQLErrorClassifier{},
		},
	}
}

// IsRetryableError determines if an error is retryable
// Returns true if the error is a transient error that may succeed on retry,
// such as network errors, timeouts, or temporary system unavailability
// This function uses DownstreamCommitClassifier for consistency
func IsRetryableError(err error) bool {
	classifier := NewDownstreamCommitClassifier()
	return classifier.IsRetryable(err)
}

// Operation defines the callable that will be executed with retry semantics.
type Operation func() error

// BackoffStrategy calculates the waiting duration before the next retry attempt.
type BackoffStrategy interface {
	// Next returns the wait duration before the given attempt (1-indexed).
	Next(attempt int) time.Duration
}

// ErrorClassifier determines whether a failure is retryable.
type ErrorClassifier interface {
	// IsRetryable returns true if the error is transient and worth retrying.
	IsRetryable(error) bool
}

// Policy controls the retry behaviour for an operation.
type Policy struct {
	// MaxAttempts defines how many times the operation should be attempted in total.
	// Must be >= 1.
	MaxAttempts int

	// Backoff decides how long to wait between attempts. Optional; zero value means no backoff.
	Backoff BackoffStrategy

	// Classifier decides whether an error warrants another attempt. Optional; defaults to never retry.
	Classifier ErrorClassifier
}

// Do executes the given Operation following the retry policy.
//
// Behaviour:
//   - Executes op up to MaxAttempts times.
//   - If op returns nil, Do returns nil immediately.
//   - If op returns non-retryable error, Do returns it without further attempts.
//   - Between retryable failures, waits according to Backoff (if provided).
//   - Context cancellation aborts waiting and returns ctx.Err().
func (p Policy) Do(ctx context.Context, op Operation) error {
	if p.MaxAttempts <= 0 {
		return moerr.NewInvalidArgNoCtx("Policy.MaxAttempts", p.MaxAttempts)
	}

	classifier := p.Classifier
	backoff := p.Backoff

	var lastErr error

	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		lastErr = op()
		if lastErr == nil {
			return nil
		}

		// If this was the last attempt, break immediately.
		if attempt == p.MaxAttempts {
			break
		}

		if errors.Is(lastErr, ErrNonRetryable) {
			break
		}

		// If classifier is absent or error not retryable, stop retrying.
		if classifier == nil || !classifier.IsRetryable(lastErr) {
			break
		}

		if backoff == nil {
			continue
		}

		wait := backoff.Next(attempt)
		if wait <= 0 {
			continue
		}

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			// proceed to next attempt
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		}
	}

	return lastErr
}

// ExponentialBackoff implements BackoffStrategy with exponential growth and optional jitter.
type ExponentialBackoff struct {
	// Base delay before the first retry attempt.
	Base time.Duration
	// Factor to multiply delays by each attempt (>1).
	Factor float64
	// Max caps the computed delay (optional).
	Max time.Duration
	// Jitter adds randomization in range [0, Jitter] (optional).
	Jitter time.Duration

	// randFn allows deterministic testing by injection.
	randFn func(time.Duration) time.Duration
}

// Next implements BackoffStrategy.
func (b ExponentialBackoff) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	base := b.Base
	if base <= 0 {
		base = time.Millisecond * 100
	}

	factor := b.Factor
	if factor <= 1 {
		factor = 2
	}

	delay := float64(base) * math.Pow(factor, float64(attempt-1))
	result := time.Duration(delay)
	if b.Max > 0 && result > b.Max {
		result = b.Max
	}

	if b.Jitter > 0 {
		randFn := b.randFn
		if randFn == nil {
			randFn = func(max time.Duration) time.Duration {
				if max <= 0 {
					return 0
				}
				return time.Duration(time.Now().UnixNano() % int64(max))
			}
		}
		result += randFn(b.Jitter)
	}
	return result
}

// MultiClassifier chains multiple classifiers; returns true if any classifier deems the error retryable.
type MultiClassifier []ErrorClassifier

// IsRetryable implements ErrorClassifier.
func (m MultiClassifier) IsRetryable(err error) bool {
	for _, classifier := range m {
		if classifier != nil && classifier.IsRetryable(err) {
			return true
		}
	}
	return false
}

// ErrNonRetryable indicates the operation should not be retried.
var ErrNonRetryable = moerr.NewInternalErrorNoCtx("non-retryable error")
