// Copyright 2022 Matrix Origin
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

package trace

import (
	"context"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export"

	"go.uber.org/zap/zapcore"
)

var _ batchpipe.HasName = &MOLog{}
var _ IBuffer2SqlItem = &MOLog{}

type MOLog struct {
	StatementId TraceID       `json:"statement_id"`
	SpanId      SpanID        `json:"span_id"`
	Timestamp   util.TimeNano `json:"Timestamp"`
	Level       zapcore.Level `json:"Level"`
	CodeLine    util.Frame    `json:"code_line"` // like "util/trace/trace.go:666"
	Message     string        `json:"Message"`
}

func newMOLog() *MOLog {
	return &MOLog{}
}

func (MOLog) GetName() string {
	return MOLogType
}

func (l MOLog) Size() int64 {
	return int64(unsafe.Sizeof(l)) + int64(len(l.Message))
}

func (l MOLog) Free() {}

var logLevelEnabler atomic.Value

func SetLogLevel(l zapcore.LevelEnabler) {
	logLevelEnabler.Store(l)

}

func ReportLog(ctx context.Context, level zapcore.Level, depth int, formatter string, args ...any) {
	if !logLevelEnabler.Load().(zapcore.LevelEnabler).Enabled(level) {
		return
	}
	if !gTracerProvider.enableTracer {
		return
	}
	_, newSpan := Start(DefaultContext(), "ReportLog")
	defer newSpan.End()

	span := SpanFromContext(ctx)
	sc := span.SpanContext()
	if sc.IsEmpty() {
		span = SpanFromContext(DefaultContext())
		sc = span.SpanContext()
	}
	log := newMOLog()
	log.StatementId = sc.TraceID
	log.SpanId = sc.SpanID
	log.Timestamp = util.NowNS()
	log.Level = level
	log.CodeLine = util.Caller(depth)
	log.Message = fmt.Sprintf(formatter, args...)
	export.GetGlobalBatchProcessor().Collect(DefaultContext(), log)
}
