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

package logutil

import (
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Slow logs if operation not done in timeout duration
func Slow(timeout time.Duration, msg string, fields ...zap.Field) (doneFunc func() bool) {
	state := new(atomic.Int32) // 0: pending, 1: logged, 2: do not log
	timer := time.AfterFunc(timeout, func() {
		if state.CompareAndSwap(0, 1) {
			GetGlobalLogger().Info(msg, fields...)
		}
	})
	doneFunc = func() (logged bool) {
		if timer.Stop() {
			// timer stopped
			return false
		}
		if state.CompareAndSwap(0, 2) {
			// will not log
			return false
		}
		// logged or logging
		return true
	}
	return
}
