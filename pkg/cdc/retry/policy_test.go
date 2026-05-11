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

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := p.Do(ctx, func() error { return moerr.NewInternalErrorNoCtx("retryable") })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("context cancel should abort quickly")
	}
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
