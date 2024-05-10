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

package errutil

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors/errbase"

	"github.com/matrixorigin/matrixone/pkg/util/stack"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

// WithContext annotates err with a stack info and a context, which should contain span info.
// At the mean time, it will call ReportError to store error info
// If err is nil, WithContext returns nil.
func WithContext(ctx context.Context, err error) error {
	return WithContextWithDepth(ctx, err, 1)
}

func WithContextWithDepth(ctx context.Context, err error, depth int) error {
	if err == nil {
		return nil
	}
	if NoReportFromContext(ctx) {
		return nil
	}
	if _, ok := err.(StackTracer); !ok {
		err = &withStack{cause: err, Stack: stack.Callers(depth + 1)}
	}
	// check SpanContext
	// if exist NEED a copy for SpanContext, prevent DATA RACE between 'span.SpanContext()' and 'sc.Reset()'
	span := trace.SpanFromContext(ctx)
	if sc := span.SpanContext(); !sc.IsEmpty() {
		newSC := sc.Clone()
		ctx = trace.ContextWithSpanContext(context.Background(), newSC)
	}
	err = &withContext{cause: err, ctx: ctx}
	GetReportErrorFunc()(ctx, err, depth+1)
	return err
}

// ContextTracer retrieves the context.Context
type ContextTracer interface {
	Context() context.Context
}

func GetContextTracer(inErr error) ContextTracer {
	var stacked ContextTracer
	WalkDeep(inErr, func(err error) bool {
		if contextTracer, ok := err.(ContextTracer); ok {
			stacked = contextTracer
			return true
		}
		return false
	})
	return stacked
}

func HasContext(err error) bool {
	return GetContextTracer(err) != nil
}

var _ error = (*withContext)(nil)
var _ Wrapper = (*withContext)(nil)
var _ ContextTracer = (*withContext)(nil)
var _ fmt.Formatter = (*withContext)(nil)

type withContext struct {
	cause error

	ctx context.Context
}

func (w *withContext) Error() string                 { return w.cause.Error() }
func (w *withContext) Cause() error                  { return w.cause }
func (w *withContext) Unwrap() error                 { return w.cause }
func (w *withContext) Context() context.Context      { return w.ctx }
func (w *withContext) Format(s fmt.State, verb rune) { errbase.FormatError(w, s, verb) }
