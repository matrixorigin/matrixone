// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// odkuAttemptResult outlives operator Reset, but is never reused by a retry.
// Only the coordinator action owner publishes, after its terminal EOF. Compile
// consumes it after all statement consumers succeed, including writer sinks.
type odkuAttemptResult struct {
	state atomic.Uint32
	rows  uint64
}

func (r *odkuAttemptResult) publish(rows uint64) error {
	if !r.state.CompareAndSwap(0, 1) {
		return moerr.NewInternalErrorNoCtx("ODKU affected rows published more than once")
	}
	r.rows = rows
	r.state.Store(2)
	return nil
}

func (r *odkuAttemptResult) result() (uint64, error) {
	if r.state.Load() != 2 {
		return 0, moerr.NewInternalErrorNoCtx("ODKU action owner did not reach EOF")
	}
	return r.rows, nil
}
