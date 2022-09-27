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
	"errors"
	"sync/atomic"

	pkgErr "github.com/pkg/errors"
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

func Wrap(err error, message string) error {
	return pkgErr.Wrap(err, message)
}

func Wrapf(err error, format string, args ...any) error {
	return pkgErr.Wrapf(err, format, args...)
}

// WalkDeep does a depth-first traversal of all errors.
// The visitor function can return true to end the traversal early, otherwise false.
func WalkDeep(err error, visitor func(err error) bool) bool {
	// Go deep
	unErr := err
	for unErr != nil {
		if done := visitor(unErr); done {
			return true
		}
		unErr = errors.Unwrap(unErr)
	}

	return false
}

// ReportError used to handle non-moerr Error
func ReportError(ctx context.Context, err error) {
	GetReportErrorFunc()(ctx, err, 1)
}

type reportErrorFunc func(context.Context, error, int)

// errorReporter should be trace.ReportError
var errorReporter atomic.Value

func noopReportError(context.Context, error, int) {}

func SetErrorReporter(f reportErrorFunc) {
	errorReporter.Store(f)
}

func GetReportErrorFunc() reportErrorFunc {
	return errorReporter.Load().(reportErrorFunc)
}
