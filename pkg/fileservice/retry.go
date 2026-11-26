// Copyright 2023 Matrix Origin
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

package fileservice

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const (
	maxRetryAttemps      = 128
	maxRetryInterval     = time.Second * 1
	initialRetryInterval = time.Millisecond * 10
	retryIntervalFactor  = 1.5
)

func DoWithRetry[T any](
	what string,
	fn func() (T, error),
	maxAttemps int,
	isRetryable func(error) bool,
) (res T, err error) {
	return DoWithRetryContext(context.Background(), what, fn, maxAttemps, isRetryable)
}

func DoWithRetryContext[T any](
	ctx context.Context,
	what string,
	fn func() (T, error),
	maxAttemps int,
	isRetryable func(error) bool,
) (res T, err error) {
	defer catch(&err)

	numRetries := 0
	sleep := initialRetryInterval

	for {
		if err = ctx.Err(); err != nil {
			return
		}
		res, err = fn()
		if err == nil || !isRetryable(err) {
			return
		}
		maxAttemps--
		if maxAttemps <= 0 {
			return
		}
		if sleep < maxRetryInterval {
			sleep = time.Duration(float64(sleep) * retryIntervalFactor)
		}
		if err = waitRetry(ctx, sleep); err != nil {
			return
		}

		numRetries++
		if numRetries%5 == 0 {
			logutil.Info("file service retry",
				zap.Any("times", numRetries),
				zap.Any("what", what),
			)
		}
	}
}

// waitRetry waits for d before a retry, freeing the room of the request ctx runs under
// until it may make the request again.
func waitRetry(ctx context.Context, d time.Duration) error {
	r := pauseRequest(ctx)
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	return r.resume(ctx)
}
