// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package motrace

import (
	"time"

	"go.uber.org/zap/zapcore"
)

func newDummyLog() *MOZapLog {
	log := newMOZap()
	log.LoggerName = "logger_name"
	log.Level = zapcore.InfoLevel
	log.SpanContext = DefaultSpanContext()
	log.Timestamp = time.Now()
	log.Caller = "motrace/common_test.go"
	log.Message = "dummy message"
	log.Extra = "dummy extra"
	log.Stack = "dummy stack"
	return log
}
