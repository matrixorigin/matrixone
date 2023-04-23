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

package restrict

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("filter(%s)", ap.E))
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)

	ap.ctr.executors, err = colexec.NewExpressionExecutor(proc, ap.E)
	return err
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	anal.Input(bat, isFirst)

	vec, err := ap.ctr.executors.Eval(proc, []*batch.Batch{bat})
	if err != nil {
		bat.Clean(proc.Mp())
		return false, err
	}

	if proc.OperatorOutofMemory(int64(vec.Size())) {
		return false, moerr.NewOOM(proc.Ctx)
	}
	anal.Alloc(int64(vec.Size()))
	if !vec.GetType().IsBoolean() {
		return false, moerr.NewInvalidInput(proc.Ctx, "filter condition is not boolean")
	}
	bs := vector.MustFixedCol[bool](vec)
	if vec.IsConst() {
		if vec.IsConstNull() || !bs[0] {
			bat.Shrink(nil)
		}
	} else {
		sels := proc.Mp().GetSels()
		for i, b := range bs {
			if b && !vec.GetNulls().Contains(uint64(i)) {
				sels = append(sels, int64(i))
			}
		}
		bat.Shrink(sels)
		proc.Mp().PutSels(sels)
	}
	anal.Output(bat, isLast)
	proc.SetInputBatch(bat)
	return false, nil
}
