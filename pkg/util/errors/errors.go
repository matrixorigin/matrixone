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
	_ "unsafe"

	goErr "errors"
)

type Wrapper interface {
	Unwrap() error
}

type isError interface {
	Is(error) bool
}

var _ Wrapper = &MOError{}

type MOError struct {
	code MOErrorCode
	msg  string
	args []any
}

func NewMOError(code MOErrorCode, msg string) error {
	err := &MOError{
		code: code,
		msg:  msg,
	}
	return err
}

func (e *MOError) Mark(args ...any) error {
	return &MOError{
		code: e.code,
		msg:  e.msg,
		args: args[:],
	}
}

func (e *MOError) Code() string { return e.code }
func (e *MOError) Error() string {
	return fmt.Sprintf(e.msg, e.args...)
}

// Format implements the fmt.Formatter interface.
func (e *MOError) Format(s fmt.State, verb rune) {
	fmt.Fprintf(s, e.Error())
}

// Unwrap implements the Wrapper interface.
func (e *MOError) Unwrap() error {
	return nil
}

func Unwrap(err error) error {
	return goErr.Unwrap(err)
}

func Is(err, target error) bool {
	return goErr.Is(err, target)
}

func As(err error, target any) bool {
	return goErr.As(err, target)
}

func New(text string) error {
	return &MOError{code: InnerErrCode, msg: text}
}
