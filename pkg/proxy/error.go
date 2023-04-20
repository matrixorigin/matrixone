// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"fmt"
	"github.com/cockroachdb/errors"
)

// errorCode indicates the errors.
type errorCode int

const (
	codeNone errorCode = iota
	codeAuthFailed
	codeClientDisconnect
	codeServerDisconnect
)

func (c errorCode) String() string {
	switch c {
	case codeNone:
		return "None"
	case codeAuthFailed:
		return "Auth failed"
	case codeClientDisconnect:
		return "Client disconnect"
	case codeServerDisconnect:
		return "Server disconnect"
	}
	return ""
}

type errWithCode struct {
	code  errorCode
	cause error
}

var _ error = (*errWithCode)(nil)
var _ fmt.Formatter = (*errWithCode)(nil)

func (e *errWithCode) Error() string {
	if e.code == 0 {
		return e.cause.Error()
	}
	return fmt.Sprintf("%s: %v", e.code, e.cause)
}

func (e *errWithCode) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

func withCode(err error, code errorCode) error {
	if err == nil {
		return nil
	}
	return &errWithCode{cause: err, code: code}
}

func getErrorCode(err error) errorCode {
	if e := (*errWithCode)(nil); errors.As(err, &e) {
		return e.code
	}
	return codeNone
}
