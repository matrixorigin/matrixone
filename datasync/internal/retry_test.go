package datasync

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoRetriesUntilSuccess(t *testing.T) {
	var gotAttempts []int
	err := Do(context.Background(), RetryPolicy{MaxAttempts: 3, Backoff: time.Nanosecond}, func(_ context.Context, attempt int) error {
		gotAttempts = append(gotAttempts, attempt)
		if attempt < 2 {
			return errors.New("temporary")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}

	wantAttempts := []int{1, 2}
	if len(gotAttempts) != len(wantAttempts) {
		t.Fatalf("attempts = %#v, want %#v", gotAttempts, wantAttempts)
	}
	for i := range wantAttempts {
		if gotAttempts[i] != wantAttempts[i] {
			t.Fatalf("attempts = %#v, want %#v", gotAttempts, wantAttempts)
		}
	}
}

func TestDoReturnsLastError(t *testing.T) {
	errFirst := errors.New("first")
	errLast := errors.New("last")
	errs := []error{errFirst, errLast}

	err := Do(context.Background(), RetryPolicy{MaxAttempts: 2, Backoff: time.Nanosecond}, func(_ context.Context, attempt int) error {
		return errs[attempt-1]
	})
	if !errors.Is(err, errLast) {
		t.Fatalf("Do() error = %v, want last", err)
	}
}

func TestDoTreatsMaxAttemptsLessThanOneAsOne(t *testing.T) {
	attempts := 0
	errBoom := errors.New("boom")

	err := Do(context.Background(), RetryPolicy{MaxAttempts: 0, Backoff: time.Nanosecond}, func(context.Context, int) error {
		attempts++
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("Do() error = %v, want boom", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestDoReturnsContextErrorWhenCanceledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	attempts := 0
	err := Do(ctx, RetryPolicy{MaxAttempts: 2, Backoff: time.Hour}, func(context.Context, int) error {
		attempts++
		cancel()
		return errors.New("temporary")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Do() error = %v, want context canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}
