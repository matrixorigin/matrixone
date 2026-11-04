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
	"io"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func TestPolicy_Do_SuccessWithoutRetry(t *testing.T) {
	p := Policy{
		MaxAttempts: 3,
		Classifier:  AlwaysRetryClassifier{},
		Backoff:     nil,
	}

	called := int32(0)
	err := p.Do(context.Background(), func() error {
		atomic.AddInt32(&called, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if called != 1 {
		t.Fatalf("expected 1 call, got %d", called)
	}
}

func TestPolicy_Do_RetryAndSucceed(t *testing.T) {
	p := Policy{
		MaxAttempts: 5,
		Classifier:  AlwaysRetryClassifier{},
		Backoff: ExponentialBackoff{
			Base:   time.Millisecond,
			Factor: 1.0,
			Jitter: 0,
			randFn: func(time.Duration) time.Duration { return 0 },
		},
	}

	called := int32(0)
	err := p.Do(context.Background(), func() error {
		count := atomic.AddInt32(&called, 1)
		if count < 3 {
			return moerr.NewInternalErrorNoCtx("temporary error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if called != 3 {
		t.Fatalf("expected 3 attempts, got %d", called)
	}
}

func TestPolicy_Do_NonRetryableStopsImmediately(t *testing.T) {
	p := Policy{
		MaxAttempts: 5,
		Classifier:  NeverRetryClassifier{},
	}

	called := int32(0)
	err := p.Do(context.Background(), func() error {
		atomic.AddInt32(&called, 1)
		return moerr.NewInternalErrorNoCtx("non-retry")
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if called != 1 {
		t.Fatalf("expected 1 attempt, got %d", called)
	}
}

func TestPolicy_Do_ContextCancelDuringBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backoff := ExponentialBackoff{
			Base:   time.Hour, // ensure we block on timer
			Factor: 1.0,
			Jitter: 0,
			randFn: func(time.Duration) time.Duration { return 0 },
		}
		p := Policy{
			MaxAttempts: 3,
			Classifier:  AlwaysRetryClassifier{},
			Backoff:     backoff,
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cancelStarted := make(chan struct{})
		go func() {
			close(cancelStarted)
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		<-cancelStarted
		start := time.Now()
		err := p.Do(ctx, func() error { return moerr.NewInternalErrorNoCtx("retryable") })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context canceled, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("context cancel should abort quickly, took %s", elapsed)
		}
	})
}

func TestDefaultClassifier(t *testing.T) {
	classifier := DefaultClassifier{}

	retryableErrors := []error{
		io.ErrUnexpectedEOF,
		io.EOF,
		context.DeadlineExceeded,
		syscall.ECONNRESET,
		syscall.EPIPE,
		&net.DNSError{IsTimeout: true},
		&net.DNSError{IsTemporary: true},
	}

	for _, err := range retryableErrors {
		if !classifier.IsRetryable(err) {
			t.Fatalf("expected retryable for %T", err)
		}
	}

	nonRetryable := moerr.NewInternalErrorNoCtx("permanent")
	if classifier.IsRetryable(nonRetryable) {
		t.Fatalf("expected non-retryable error to be false")
	}
}

func TestPolicy_Do_CircuitOpenStops(t *testing.T) {
	p := Policy{
		MaxAttempts: 5,
		Classifier:  AlwaysRetryClassifier{},
	}

	var attempts int32
	err := p.Do(context.Background(), func() error {
		if atomic.AddInt32(&attempts, 1) == 1 {
			return ErrCircuitOpen
		}
		return nil
	})

	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestExponentialBackoffDefaultsCapAndJitter(t *testing.T) {
	calls := 0
	b := ExponentialBackoff{
		Base: 10 * time.Millisecond, Factor: 3, Max: 20 * time.Millisecond, Jitter: 5 * time.Millisecond,
		randFn: func(max time.Duration) time.Duration {
			calls++
			if max != 5*time.Millisecond {
				t.Fatalf("jitter bound = %s", max)
			}
			return 4 * time.Millisecond
		},
	}
	if got := b.Next(0); got != 14*time.Millisecond {
		t.Fatalf("first delay = %s", got)
	}
	if got := b.Next(3); got != 24*time.Millisecond {
		t.Fatalf("capped delay with jitter = %s", got)
	}
	if calls != 2 {
		t.Fatalf("jitter calls = %d", calls)
	}
	if got := (ExponentialBackoff{}).Next(2); got != 200*time.Millisecond {
		t.Fatalf("default delay = %s", got)
	}
}
