package retry

import (
	"context"
	"time"
)

type Config struct {
	MaxAttempts int
	Backoff     time.Duration
}

func Do(ctx context.Context, cfg Config, fn func(context.Context, int) error) error {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if err := fn(ctx, attempt); err != nil {
			lastErr = err
		} else {
			return nil
		}
		if attempt == cfg.MaxAttempts {
			break
		}

		timer := time.NewTimer(cfg.Backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}
