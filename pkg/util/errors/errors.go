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

package errors

import (
	"context"
	goErrors "errors"
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/util"
)

func init() {
	SetErrorReporter(noopReportError)
}

type Wrapper interface {
	Unwrap() error
}

type WithIs interface {
	Is(error) bool
}

func Unwrap(err error) error {
	return goErrors.Unwrap(err)
}

func Is(err, target error) bool {
	return goErrors.Is(err, target)
}

func As(err error, target any) bool {
	return goErrors.As(err, target)
}

func New(text string) error {
	err := &withStack{goErrors.New(text), util.Callers(1)}
	GetReportErrorFunc()(nil, err, 1)
	return err
}

func NewWithContext(ctx context.Context, text string) (err error) {
	err = &withStack{goErrors.New(text), util.Callers(1)}
	err = &withContext{err, ctx}
	GetReportErrorFunc()(ctx, err, 1)
	return err
}

// Wrap returns an error annotating err with a stack trace
// at the point Wrap is called, and the supplied message.
// If err is nil, Wrap returns nil.
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	err = &withMessage{
		cause: err,
		msg:   message,
	}
	return &withStack{
		err,
		util.Callers(1),
	}
}

// Wrapf returns an error annotating err with a stack trace
// at the point Wrapf is called, and the format specifier.
// If err is nil, Wrapf returns nil.
func Wrapf(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	err = &withMessage{
		cause: err,
		msg:   fmt.Sprintf(format, args...),
	}
	return &withStack{
		err,
		util.Callers(1),
	}
}

// WalkDeep does a depth-first traversal of all errors.
// The visitor function can return true to end the traversal early.
// In that case, WalkDeep will return true, otherwise false.
func WalkDeep(err error, visitor func(err error) bool) bool {
	// Go deep
	unErr := err
	for unErr != nil {
		if done := visitor(unErr); done {
			return true
		}
		unErr = Unwrap(unErr)
	}

	return false
}

func ReportError(ctx context.Context, err error) {
	GetReportErrorFunc()(ctx, err, 1)
}

type reportErrorFunc func(context.Context, error, int)

// errorReporter should be trace.HandleError
var errorReporter atomic.Value

func noopReportError(context.Context, error, int) {}

func SetErrorReporter(f reportErrorFunc) {
	errorReporter.Store(f)
}

func GetReportErrorFunc() reportErrorFunc {
	return errorReporter.Load().(reportErrorFunc)
}
