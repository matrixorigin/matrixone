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

package limit

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("limit(%v)", n.Limit))
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

// Call returning only the first n tuples from its input
func Call(_ int, proc *process.Process, arg interface{}) (bool, error) {
	bat := proc.Reg.InputBatch
	if bat == nil {
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	n := arg.(*Argument)
	if n.Seen >= n.Limit {
		proc.Reg.InputBatch = nil
		bat.Clean(proc.Mp)
		return true, nil
	}
	length := len(bat.Zs)
	newSeen := n.Seen + uint64(length)
	if newSeen >= n.Limit { // limit - seen
		batch.SetLength(bat, int(n.Limit-n.Seen))
		n.Seen = newSeen
		return true, nil
	}
	n.Seen = newSeen
	return false, nil
}
