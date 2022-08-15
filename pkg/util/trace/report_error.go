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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/export"

	"github.com/cockroachdb/errors/errbase"
)

var _ IBuffer2SqlItem = &MOErrorHolder{}

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

func (h *MOErrorHolder) Format(s fmt.State, verb rune) { errbase.FormatError(h.Error, s, verb) }

// ReportError send to BatchProcessor
func ReportError(ctx context.Context, err error) {
	if ctx == nil {
		ctx = DefaultContext()
	}
	e := &MOErrorHolder{Error: err, Timestamp: util.NowNS()}
	export.GetGlobalBatchProcessor().Collect(ctx, e)
}

// HandleError api for pkg/util/errors as errorReporter
func HandleError(ctx context.Context, err error, depth int) {
	if !gTracerProvider.IsEnable() {
		return
	}
	if ctx == nil {
		ctx = DefaultContext()
	}
	ReportError(ctx, err)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(depth+1)).Info("error", ContextField(ctx), zap.Error(err))
}
