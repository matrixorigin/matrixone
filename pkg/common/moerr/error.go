// Copyright 2021 - 2022 Matrix Origin
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

package moerr

import (
	"fmt"
	"runtime/debug"
)

const (
	// 0 is OK.
	SUCCESS = iota
	INFO
	WARN

	// Group 1: Internal errors
	ERROR_START = 1000 + iota
	INTERNAL_ERROR
	NYI

	// Group 2: numeric
	DIVISION_BY_ZERO = 2000 + iota
	OUT_OF_RANGE
)

type Error struct {
	Code    int32
	Message string
}

func (e *Error) Ok() bool {
	return e.Code < ERROR_START
}

func (e *Error) Error() string {
	return e.Message
}

//
// Most of the times should not call this.  Just use nil
// func NewSUCCESS() *Error {
// 	return &Error{SUCCESS, ""}
// }
//

func NewInfo(msg string) *Error {
	return &Error{INFO, msg}
}

func NewWarn(msg string) *Error {
	return &Error{WARN, msg}
}

func NewInternalError(msg string, args ...interface{}) *Error {
	var err = Error{Code: INTERNAL_ERROR}
	err.Message = fmt.Sprintf("Internal error: "+msg, args...)
	return &err
}

// Convert a runtime panic to internal error.
func NewPanicError(v interface{}) *Error {
	if e, ok := v.(*Error); ok {
		return e
	}
	return NewInternalError("panic %v: %s", v, debug.Stack())
}

func NewError(code int32, msg string) *Error {
	return &Error{code, msg}
}
