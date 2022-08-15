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

package util

import (
	"fmt"
	"io"
	"runtime"

	pkgErr "github.com/pkg/errors"
)

type StackTrace = pkgErr.StackTrace

type Frame = pkgErr.Frame

type Stack []uintptr

// Callers mirrors the code in github.com/pkg/errors,
// but makes the depth customizable.
func Callers(depth int) *Stack {
	const numFrames = 32
	var pcs [numFrames]uintptr
	n := runtime.Callers(2+depth, pcs[:])
	var st Stack = pcs[0:n]
	return &st
}

// Caller return only one Frame
func Caller(depth int) Frame {
	const numFrames = 1
	var pcs [numFrames]uintptr
	_ = runtime.Callers(2+depth, pcs[:])
	return Frame(pcs[0])
}

// Format mirrors the code in github.com/pkg/errors.
func (s *Stack) Format(st fmt.State, verb rune) {
	for _, pc := range *s {
		f := Frame(pc)
		io.WriteString(st, "\n")
		f.Format(st, verb)
	}
}

// StackTrace mirrors the code in github.com/pkg/errors.
func (s *Stack) StackTrace() pkgErr.StackTrace {
	f := make([]pkgErr.Frame, len(*s))
	for i := 0; i < len(f); i++ {
		f[i] = pkgErr.Frame((*s)[i])
	}
	return f
}
