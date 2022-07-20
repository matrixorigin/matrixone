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

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("σ(%s)", n.E))
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(idx int, proc *process.Process, arg interface{}) (bool, error) {
	bat := proc.Reg.InputBatch
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
	anal.Input(bat)
	vec, err := colexec.EvalExpr(bat, proc, ap.E)
	if err != nil {
		bat.Clean(proc.Mp)
		return false, err
	}
	defer vector.Clean(vec, proc.Mp)
	bs, ok := vec.Col.([]bool)
	if !ok {
		return false, errors.New("", "Only bool expression can be used as filter condition.")
	}
	if vec.IsScalar() {
		if !bs[0] {
			bat.Shrink(nil)
		}
	} else {
		sels := proc.GetSels()
		for i, b := range bs {
			if b {
				sels = append(sels, int64(i))
			}
		}
		bat.Shrink(sels)
		proc.PutSels(sels)
	}
	anal.Output(bat)
	proc.Reg.InputBatch = bat
	return false, nil
}
