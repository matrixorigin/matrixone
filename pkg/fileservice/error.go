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

package fileservice

import (
	"errors"
	"io"
	"strings"
)

func IsRetryableError(err error) bool {
	// Is error
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	str := err.Error()
	// match exact string
	switch str {
	case "connection reset by peer",
		"connection timed out":
		return true
	}
	// match sub-string
	if strings.Contains(str, "unexpected EOF") {
		return true
	}
	if strings.Contains(str, "connection reset by peer") {
		return true
	}
	if strings.Contains(str, "connection timed out") {
		return true
	}
	if strings.Contains(str, "dial tcp: lookup") {
		return true
	}
	return false
}

type errorStr string

func (e errorStr) Error() string {
	return string(e)
}

type throwError struct {
	err error
}

func throw(err error) {
	panic(throwError{
		err: err,
	})
}

func catch(ptr *error) {
	p := recover()
	if p == nil {
		return
	}
	e, ok := p.(throwError)
	if !ok {
		panic(p)
	} else {
		*ptr = e.err
	}
}
