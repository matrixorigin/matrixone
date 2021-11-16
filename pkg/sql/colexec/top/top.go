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

package top

import (
	"bytes"
	"fmt"
	"matrixone/pkg/compare"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("τ([")
	for i, f := range n.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString(fmt.Sprintf("], %v)", n.Limit))
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	ctr := &n.Ctr
	{
		ctr.attrs = make([]string, len(n.Fs))
		for i, f := range n.Fs {
			ctr.attrs[i] = f.Attr
		}
	}
	ctr.n = len(n.Fs)
	ctr.sels = make([]int64, n.Limit)
	ctr.cmps = make([]compare.Compare, len(n.Fs))
	return nil
}

func Call(_ *process.Process, _ interface{}) (bool, error) {
	return false, nil
}
