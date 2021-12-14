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

package splice

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Argument struct{
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	var err error

	bat := proc.Reg.InputBatch
	b := proc.TempBatch

	if bat == nil { // last batch
		proc.Reg.InputBatch = b
		b = nil
		return true, nil
	}

	if len(bat.Zs) == 0 { // empty batch
		return false, nil
	}

	proc.TempBatch, err = b.Append(proc.Mp, bat)
	if err != nil {
		return false, err
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return false, nil
}