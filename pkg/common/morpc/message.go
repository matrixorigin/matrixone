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
