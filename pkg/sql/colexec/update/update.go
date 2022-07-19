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

package update

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	buf.WriteString("update rows")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(_ int, proc *process.Process, arg interface{}) (bool, error) {
	p := arg.(*Argument)
	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Zs) == 0 {
		return false, nil
	}
	defer bat.Clean(proc.Mp)
	affectedRows := uint64(batch.Length(bat))
	// Fill vector for constant value
	for i := range bat.Vecs {
		if i == 0 {
			continue
		}
		bat.Vecs[i] = bat.Vecs[i].ConstExpand(proc.Mp)
	}

	if p.PriKeyIdx != -1 {
		// Delete old data because update primary key
		err := p.TableSource.Delete(p.Ts, bat.GetVector(p.PriKeyIdx), p.PriKey, proc.Snapshot)
		if err != nil {
			return false, err
		}

		// Reduce batch for update column
		vector.Clean(bat.Vecs[0], proc.Mp)
		bat.Vecs = bat.Vecs[1:]
		bat.Attrs = append(bat.Attrs, p.UpdateAttrs...)
		bat.Attrs = append(bat.Attrs, p.OtherAttrs...)

		// Write new data after update
		err = p.TableSource.Write(p.Ts, bat, proc.Snapshot)
		if err != nil {
			return false, err
		}
	} else {
		// Assign attribute name for batch
		bat.Attrs = append(bat.Attrs, p.HideKey)
		bat.Attrs = append(bat.Attrs, p.UpdateAttrs...)

		// Write new data after update
		err := p.TableSource.Update(p.Ts, bat, proc.Snapshot)
		if err != nil {
			return false, err
		}
	}

	p.M.Lock()
	p.AffectedRows += affectedRows
	p.M.Unlock()

	return false, nil
}
