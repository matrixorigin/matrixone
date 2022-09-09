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

package dispatch

import (
	"bytes"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("dispatch")
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	return nil
}

func Call(_ int, proc *process.Process, arg any) (bool, error) {
	ap := arg.(*Argument)
	bat := proc.InputBatch()
	if bat == nil {
		for _, reg := range ap.Regs {
			select {
			case <-reg.Ctx.Done():
			case reg.Ch <- nil:
			}
		}
		return true, nil
	}
	vecs := ap.vecs[:0]
	for i := range bat.Vecs {
		if bat.Vecs[i].IsOriginal() {
			vec, err := vector.Dup(bat.Vecs[i], proc.Mp())
			if err != nil {
				return false, err
			}
			vecs = append(vecs, vec)
		}
	}
	for i := range bat.Vecs {
		if bat.Vecs[i].IsOriginal() {
			bat.Vecs[i] = vecs[0]
			vecs = vecs[1:]
		}
	}
	if ap.All {
		atomic.AddInt64(&bat.Cnt, int64(len(ap.Regs))-1)
		if bat.Ht != nil {
			jm, ok := bat.Ht.(*hashmap.JoinMap)
			if ok {
				jm.IncRef(int64(len(ap.Regs)) - 1)
			}
		}
		for _, reg := range ap.Regs {
			select {
			case <-reg.Ctx.Done():
			case reg.Ch <- bat:
			}
		}
		return false, nil
	}
	for len(ap.Regs) > 0 {
		reg := ap.Regs[ap.ctr.i]
		select {
		case <-reg.Ctx.Done():
			ap.Regs = append(ap.Regs[:ap.ctr.i], ap.Regs[ap.ctr.i+1:]...)
			if ap.ctr.i >= len(ap.Regs) {
				ap.ctr.i = 0
			}
		case reg.Ch <- bat:
			if ap.ctr.i = ap.ctr.i + 1; ap.ctr.i >= len(ap.Regs) {
				ap.ctr.i = 0
			}
			return false, nil
		}
	}
	return true, nil
}
