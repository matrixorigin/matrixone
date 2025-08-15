// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashtable

import "io"

// errorAfterNWriter returns error after N bytes was written
type errorAfterNWriter struct {
	N int
}

var _ io.Writer = new(errorAfterNWriter)

func (e *errorAfterNWriter) Write(p []byte) (n int, err error) {
	if e.N < len(p) {
		if e.N < 0 {
			return 0, io.ErrUnexpectedEOF
		}
		n = e.N
		e.N = -1
		err = io.ErrShortWrite
		return
	}
	e.N -= len(p)
	return len(p), nil
}
