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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/export"

	"github.com/cockroachdb/errors/errbase"
	"go.uber.org/zap"
)

var _ IBuffer2SqlItem = (*MOErrorHolder)(nil)
var _ CsvFields = (*MOErrorHolder)(nil)

type MOErrorHolder struct {
	Error     error         `json:"error"`
	Timestamp util.TimeNano `json:"timestamp"`
}

func (h MOErrorHolder) GetName() string {
	return MOErrorType
}

func (h MOErrorHolder) Size() int64 {
	return int64(32*8) + int64(unsafe.Sizeof(h))
}
func (h MOErrorHolder) Free() {}

func (h MOErrorHolder) CsvOptions() *CsvOptions {
	return CommonCsvOptions
}

func (h MOErrorHolder) CsvFields() []string {
	var span Span
	if ct := errutil.GetContextTracer(h.Error); ct != nil && ct.Context() != nil {
		span = SpanFromContext(ct.Context())
	} else {
		span = SpanFromContext(DefaultContext())
	}
	var result []string
	result = append(result, span.SpanContext().TraceID.String())
	result = append(result, span.SpanContext().SpanID.String())
	result = append(result, GetNodeResource().NodeUuid)
	result = append(result, GetNodeResource().NodeType)
	result = append(result, h.Error.Error())
	result = append(result, fmt.Sprintf(errorFormatter.Load().(string), h.Error))
	result = append(result, nanoSec2DatetimeString(h.Timestamp))
	return result
}

func (h *MOErrorHolder) Format(s fmt.State, verb rune) { errbase.FormatError(h.Error, s, verb) }

// ReportError send to BatchProcessor
func ReportError(ctx context.Context, err error, depth int) {
	msg := fmt.Sprintf("error: %v", err)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(depth+1)).Info(msg, ContextField(ctx))
	if !GetTracerProvider().IsEnable() {
		return
	}
	if ctx == nil {
		ctx = DefaultContext()
	}
	e := &MOErrorHolder{Error: err, Timestamp: util.NowNS()}
	export.GetGlobalBatchProcessor().Collect(ctx, e)
}
