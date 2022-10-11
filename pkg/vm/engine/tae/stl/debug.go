// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stl

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
)

var (
	_callersPool = sync.Pool{
		New: func() interface{} {
			return newCallers(32)
		},
	}
)

func GetCalllers(skip int) *Callers {
	c := _callersPool.Get()
	cc := c.(*Callers)
	var n int
	for {
		n = runtime.Callers(skip+2, cc.storage)
		if n < len(cc.storage) {
			break
		}
		size := len(cc.storage)
		cc.Close()
		cc = newCallers(size * 2)
	}
	cc.num = n
	return cc
}

type Callers struct {
	storage []uintptr
	num     int
}

func newCallers(size int) *Callers {
	return &Callers{
		storage: make([]uintptr, size),
	}
}

func (c *Callers) Close() {
	c.num = 0
	_callersPool.Put(c)
}

func (c *Callers) String() string {
	var buffer bytes.Buffer
	i := 0
	frames := runtime.CallersFrames(c.storage[:c.num])
	for frame, more := frames.Next(); more; frame, more = frames.Next() {
		if i != 0 {
			buffer.WriteByte('\n')
		}
		i++
		// buffer.WriteByte('[')
		// buffer.WriteString(frame.Function)
		// buffer.WriteByte(']')
		// buffer.WriteByte('|')
		buffer.WriteString(frame.File)
		buffer.WriteByte(':')
		buffer.WriteString(strconv.Itoa(frame.Line))
	}
	return buffer.String()
}
