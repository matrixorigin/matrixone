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
	"bytes"
	"fmt"
	"io"
	"runtime"

	"github.com/cockroachdb/errors/withstack"
	pkgErr "github.com/pkg/errors"
)

// This file mirrors the WithStack functionality from
// github.com/pkg/errors. We would prefer to reuse the withStack
// struct from that package directly (the library recognizes it well)
// unfortunately github.com/pkg/errors does not enable client code to
// customize the depth at which the stack trace is captured.

// WithStack like cockroach/errors/withstack
func WithStack(err error) error {
	return withstack.WithStackDepth(err, 1)
}

// WithStackDepth call cockroach/errors/withstack
func WithStackDepth(err error, depth int) error {
	return withstack.WithStackDepth(err, depth+1)
}

type StackTrace = pkgErr.StackTrace

type Frame = pkgErr.Frame

type stack []uintptr

// callers mirrors the code in github.com/pkg/errors,
// but makes the depth customizable.
func callers(depth int) *stack {
	const numFrames = 32
	var pcs [numFrames]uintptr
	n := runtime.Callers(2+depth, pcs[:])
	var st stack = pcs[0:n]
	return &st
}

// Format mirrors the code in github.com/pkg/errors.
func (s *stack) Format(st fmt.State, verb rune) {
	for _, pc := range *s {
		f := Frame(pc)
		io.WriteString(st, "\n")
		f.Format(st, verb)
	}
}

// StackTrace mirrors the code in github.com/pkg/errors.
func (s *stack) StackTrace() pkgErr.StackTrace {
	f := make([]pkgErr.Frame, len(*s))
	for i := 0; i < len(f); i++ {
		f[i] = pkgErr.Frame((*s)[i])
	}
	return f
}

// rawStack usage example of runtime.Callers
// return caller's codeLine, and stack info
func rawStack(skip int) (string, string) {
	rpc := make([]uintptr, MaxCodeStackLen)
	n := runtime.Callers(skip, rpc)
	frames := runtime.CallersFrames(rpc)
	buf := new(bytes.Buffer)
	var codeLine string
	for i := 0; i < n; i++ {
		frame, _ := frames.Next()
		pc, file, lineNo, fun := rpc[i], frame.File, frame.Line, frame.Function
		if i == 0 {
			codeLine = fmt.Sprintf("%s:%d", file, lineNo)
		}
		fmt.Fprintf(buf, "%s:%d (0x%x)\n", file, lineNo, pc)
		fmt.Fprintf(buf, "  %s\n", fun)
	}
	return codeLine, buf.String()
}
