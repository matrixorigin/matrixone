// Copyright 2021 - 2024 Matrix Origin
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
	"context"
	"time"
)

func getTimeout(
	ctx context.Context,
	timeout time.Duration,
) time.Duration {
	if timeout == 0 {
		v, ok := getTimeoutFromContext(ctx)
		if !ok {
			panic("invalid deadline")
		}
		timeout = v
	}
	return timeout
}

func getTimeoutFromContext(ctx context.Context) (time.Duration, bool) {
	d, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	now := time.Now()
	if now.After(d) {
		return 0, true
	}
	return d.Sub(now), true
}
