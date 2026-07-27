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

package morpc

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	internalTimeout = time.Second * 10
	oneWayTimeout   = time.Second * 600
)

// Timeout return true if the message is timeout
func (m RPCMessage) Timeout() bool {
	select {
	case <-m.Ctx.Done():
		return true
	default:
		return false
	}
}

// GetTimeoutFromContext returns the timeout duration from context.
func (m RPCMessage) GetTimeoutFromContext() (time.Duration, error) {
	if m.internal {
		if deadline, ok := m.Ctx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return 0, m.Ctx.Err()
			}
			if remaining < internalTimeout {
				return remaining, nil
			}
		}
		return internalTimeout, nil
	}
	if m.oneWay {
		return oneWayTimeout, nil
	}

	d, ok := m.Ctx.Deadline()
	if !ok {
		return 0, moerr.NewInvalidInputNoCtx("timeout deadline not set")
	}
	now := time.Now()
	if now.After(d) {
		return 0, moerr.NewInvalidInputNoCtx("timeout has invalid deadline")
	}
	return d.Sub(now), nil
}

// earliestDeadline returns the earliest non-zero deadline in a batch.
func earliestDeadline(current, candidate time.Time) time.Time {
	if candidate.IsZero() {
		return current
	}
	if current.IsZero() || candidate.Before(current) {
		return candidate
	}
	return current
}

// remainingDeadlineTimeout converts an absolute deadline into the positive
// duration expected by goetty Flush. An expired deadline must not become zero,
// because zero is commonly interpreted as "no timeout".
func remainingDeadlineTimeout(deadline, now time.Time) time.Duration {
	timeout := deadline.Sub(now)
	if timeout <= 0 {
		return time.Nanosecond
	}
	return timeout
}
