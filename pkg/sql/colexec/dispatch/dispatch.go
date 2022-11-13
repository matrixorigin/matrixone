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
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"sync/atomic"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("dispatch")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	return nil
}

func Call(idx int, proc *process.Process, arg any) (bool, error) {
	ap := arg.(*Argument)
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}

	// source vectors should be cloned and instead before it was sent.
	for i, vec := range bat.Vecs {
		if vec.IsOriginal() {
			cloneVec, err := vector.Dup(vec, proc.Mp())
			if err != nil {
				bat.Clean(proc.Mp())
				return false, err
			}
			bat.Vecs[i] = cloneVec
		}
	}

	// send to each one
	if ap.All {
		refCountAdd := int64(len(ap.Regs) - 1)
		atomic.AddInt64(&bat.Cnt, refCountAdd)
		if jm, ok := bat.Ht.(*hashmap.JoinMap); ok {
			jm.IncRef(refCountAdd)
		}

		for _, reg := range ap.Regs {
			select {
			case <-reg.Ctx.Done():
				return false, moerr.NewInternalError("pipeline context has done.")
			case reg.Ch <- bat:
			}
		}
		return false, nil
	}
	// send to any one
	for len(ap.Regs) > 0 {
		if ap.ctr.i == len(ap.Regs) {
			ap.ctr.i = 0
		}
		reg := ap.Regs[ap.ctr.i]
		select {
		case <-reg.Ctx.Done():
			// XXX is that suitable ? should we return err
			return false, moerr.NewInternalError("pipeline context has done.")
		case reg.Ch <- bat:
			ap.ctr.i++
			return false, nil
		}
	}
	logutil.Warnf("no pipeline to receive the batch from dispatch. but still get batch need to send.")
	return true, nil
}
