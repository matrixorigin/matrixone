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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/util"

	"github.com/cockroachdb/errors/errbase"
)

// This file mirrors the WithStack functionality from
// github.com/pkg/errors. We would prefer to reuse the withStack
// struct from that package directly (the library recognizes it well)
// unfortunately github.com/pkg/errors does not enable client code to
// customize the depth at which the stack trace is captured.

// WithStack annotates err with a stack trace at the point WithStack was called.
func WithStack(err error) error {
	// Skip the frame of WithStack itself in caller stack.
	// this mirrors the behavior of WithStack() in github.com/pkg/errors.
	return WithStackDepth(err, 1)
}

// WithStackDepth annotates err with a stack trace starting from the given call depth.
// The value zero identifies the caller of WithStackDepth itself.
// See the documentation of WithStack() for more details.
func WithStackDepth(err error, depth int) error {
	if err == nil {
		return nil
	}
	return &withStack{cause: err, Stack: util.Callers(depth + 1)}
}

// StackTracer retrieves the StackTrace
// Generally you would want to use the GetStackTracer function to do that.
type StackTracer interface {
	StackTrace() util.StackTrace
}

func GetStackTracer(inErr error) StackTracer {
	var stacked StackTracer
	WalkDeep(inErr, func(err error) bool {
		if stackTracer, ok := err.(StackTracer); ok {
			stacked = stackTracer
			return true
		}
		return false
	})
	return stacked
}

func HasStack(err error) bool {
	return GetStackTracer(err) != nil
}

type withStack struct {
	cause error

	*util.Stack
}

var _ error = (*withStack)(nil)
var _ Wrapper = (*withStack)(nil)
var _ fmt.Formatter = (*withStack)(nil)
var _ StackTracer = (*withStack)(nil)

func (w *withStack) Error() string { return w.cause.Error() }
func (w *withStack) Cause() error  { return w.cause }
func (w *withStack) Unwrap() error { return w.cause }

// Format implements the fmt.Formatter interface.
func (w *withStack) Format(s fmt.State, verb rune) { errbase.FormatError(w, s, verb) }

func (w *withStack) HasStack() bool {
	return true
}
