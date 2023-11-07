// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mergeblock

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(" MergeS3BlocksMetaLoc ")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	ap := arg
	ap.container = new(Container)
	ap.container.mp = make(map[int]*batch.Batch)
	ap.container.mp2 = make(map[int][]*batch.Batch)
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	var err error
	ap := arg
	result, err := arg.children[0].Call(proc)
	if err != nil {
		return result, err
	}
	if result.Batch == nil {
		result.Status = vm.ExecStop
		return result, nil
	}
	if result.Batch.IsEmpty() {
		result.Batch = batch.EmptyBatch
		return result, nil
	}
	bat := result.Batch
	if err := ap.Split(proc, bat); err != nil {
		return result, err
	}

	// If the target is a partition table
	if len(ap.PartitionSources) > 0 {
		// 'i' aligns with partition number
		for i := range ap.PartitionSources {
			if ap.container.mp[i].RowCount() > 0 {
				// batches in mp will be deeply copied into txn's workspace.
				if err = ap.PartitionSources[i].Write(proc.Ctx, ap.container.mp[i]); err != nil {
					return result, err
				}
			}

			for _, bat := range ap.container.mp2[i] {
				// batches in mp2 will be deeply copied into txn's workspace.
				if err = ap.PartitionSources[i].Write(proc.Ctx, bat); err != nil {
					return result, err
				}

			}
			ap.container.mp2[i] = ap.container.mp2[i][:0]
		}
	} else {
		// handle origin/main table.
		if ap.container.mp[0].RowCount() > 0 {
			//batches in mp will be deeply copied into txn's workspace.
			if err = ap.Tbl.Write(proc.Ctx, ap.container.mp[0]); err != nil {
				return result, err
			}
		}

		for _, bat := range ap.container.mp2[0] {
			//batches in mp2 will be deeply copied into txn's workspace.
			if err = ap.Tbl.Write(proc.Ctx, bat); err != nil {
				return result, err
			}
		}
		ap.container.mp2[0] = ap.container.mp2[0][:0]
	}

	return result, nil
}
