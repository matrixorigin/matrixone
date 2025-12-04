// Copyright 2024 Matrix Origin
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

package retry

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

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
		return moerr.NewInvalidArgNoCtx("retry.Policy.MaxAttempts", p.MaxAttempts)
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

		if errors.Is(lastErr, ErrCircuitOpen) || errors.Is(lastErr, ErrNonRetryable) {
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

// NeverRetryClassifier never retries any error.
type NeverRetryClassifier struct{}

// IsRetryable implements ErrorClassifier.
func (NeverRetryClassifier) IsRetryable(error) bool { return false }

// AlwaysRetryClassifier retries any error.
type AlwaysRetryClassifier struct{}

// IsRetryable implements ErrorClassifier.
func (AlwaysRetryClassifier) IsRetryable(error) bool { return true }

// ErrNonRetryable indicates the operation should not be retried.
var ErrNonRetryable = moerr.NewInternalErrorNoCtx("non-retryable error")

// ErrCircuitOpen indicates the retry circuit breaker is open.
var ErrCircuitOpen = moerr.NewInternalErrorNoCtx("retry circuit open")
