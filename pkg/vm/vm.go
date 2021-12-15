// Copyright 2021 Matrix Origin
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

package vm

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(ins Instructions, buf *bytes.Buffer) {
	for i, in := range ins {
		if i > 0 {
			buf.WriteString(" -> ")
		}
		formatFunc[in.Code](in.Arg, buf)
	}
}

func Clean(_ Instructions, _ *process.Process) {
}

// Prepare initialization
func Prepare(ins Instructions, proc *process.Process) error {
	for _, in := range ins {
		if err := prepareFunc[in.Code](proc, in.Arg); err != nil {
			return err
		}
	}
	return nil
}

func Run(ins Instructions, proc *process.Process) (bool, error) {
	var ok bool
	var end bool
	var err error

	for _, in := range ins {
		if ok, err = execFunc[in.Code](proc, in.Arg); err != nil {
			return ok || end, err
		}
		if ok {
			end = true
		}
	}
	return end, nil
}
