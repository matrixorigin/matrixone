// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backoff

import (
	"math"
	"time"
)

// Exponential computes the delay for a one-based attempt. Non-positive initial
// delays default to 100ms, and multipliers at or below one default to two.
// A positive maximum caps the delay before jitter is added. When random is nil,
// jitter uses the current time; callers may inject it for deterministic tests.
func Exponential(attempt int, initial time.Duration, multiplier float64, maximum, jitter time.Duration, random func(time.Duration) time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	base := initial
	if base <= 0 {
		base = time.Millisecond * 100
	}

	factor := multiplier
	if factor <= 1 {
		factor = 2
	}

	delay := float64(base) * math.Pow(factor, float64(attempt-1))
	result := time.Duration(delay)
	if maximum > 0 && result > maximum {
		result = maximum
	}

	if jitter > 0 {
		randFn := random
		if randFn == nil {
			randFn = func(max time.Duration) time.Duration {
				if max <= 0 {
					return 0
				}
				return time.Duration(time.Now().UnixNano() % int64(max))
			}
		}
		result += randFn(jitter)
	}
	return result
}
