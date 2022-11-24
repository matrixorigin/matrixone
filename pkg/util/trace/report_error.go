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
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"time"
	"unsafe"

	"github.com/cockroachdb/errors/errbase"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"go.uber.org/zap"
)

// MOErrorHolder implement export.IBuffer2SqlItem and export.CsvFields
type MOErrorHolder struct {
	Error     error     `json:"error"`
	Timestamp time.Time `json:"timestamp"`
}

func (h *MOErrorHolder) GetName() string {
	return errorView.OriginTable.GetName()
}

func (h *MOErrorHolder) Size() int64 {
	return int64(32*8) + int64(unsafe.Sizeof(h))
}
func (h *MOErrorHolder) Free() {
	h.Error = nil
}

func (h *MOErrorHolder) GetRow() *table.Row { return errorView.OriginTable.GetRow() }

func (h *MOErrorHolder) CsvFields(row *table.Row) []string {
	row.Reset()
	row.SetColumnVal(rawItemCol, errorView.Table)
	row.SetColumnVal(timestampCol, Time2DatetimeString(h.Timestamp))
	row.SetColumnVal(nodeUUIDCol, GetNodeResource().NodeUuid)
	row.SetColumnVal(nodeTypeCol, GetNodeResource().NodeType)
	row.SetColumnVal(errorCol, h.Error.Error())
	row.SetColumnVal(stackCol, fmt.Sprintf(errorFormatter.Load().(string), h.Error))
	var moError *moerr.Error
	if errors.As(h.Error, &moError) {
		row.SetColumnVal(errCodeCol, fmt.Sprintf("%d", moError.ErrorCode()))
	}
	if ct := errutil.GetContextTracer(h.Error); ct != nil && ct.Context() != nil {
		span := SpanFromContext(ct.Context())
		row.SetColumnVal(traceIDCol, span.SpanContext().TraceID.String())
		row.SetColumnVal(spanIDCol, span.SpanContext().SpanID.String())
		row.SetColumnVal(spanKindCol, span.SpanContext().Kind.String())
	}
	return row.ToStrings()
}

func (h *MOErrorHolder) Format(s fmt.State, verb rune) { errbase.FormatError(h.Error, s, verb) }

// ReportError send to BatchProcessor
func ReportError(ctx context.Context, err error, depth int) {
	msg := fmt.Sprintf("error: %v", err)
	sc := SpanFromContext(ctx).SpanContext()
	if sc.IsEmpty() {
		logutil.GetErrorLogger().WithOptions(zap.AddCallerSkip(depth)).Error(msg)
	} else {
		logutil.GetErrorLogger().WithOptions(zap.AddCallerSkip(depth)).Error(msg, ContextField(ctx))
	}
	if !GetTracerProvider().IsEnable() {
		return
	}
	if ctx == nil {
		ctx = DefaultContext()
	}
	e := &MOErrorHolder{Error: err, Timestamp: time.Now()}
	GetGlobalBatchProcessor().Collect(ctx, e)
}
