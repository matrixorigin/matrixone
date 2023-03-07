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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Container struct {
	// mp is used to store the metaLoc Batch
	mp map[int]*batch.Batch
	// mp2 is used to store the normal data batches
	mp2 map[int][]*batch.Batch
}

type Argument struct {
	// 1. main table
	Tbl engine.Relation
	// 2. unique index tables
	Unique_tbls  []engine.Relation
	AffectedRows uint64
	// 3. used for ut_test, otherwise the batch will free,
	// and we can't get the result to check
	notFreeBatch bool
	container    *Container
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	for k := range arg.container.mp {
		arg.container.mp[k].Clean(proc.GetMPool())
		arg.container.mp[k] = nil
	}
}

func (arg *Argument) GetMetaLocBat(name string) {
	bat := batch.New(true, []string{name})
	bat.Cnt = 1
	bat.Vecs[0] = vector.New(types.New(types.T_text,
		0, 0))
	arg.container.mp[0] = bat
	for i := range arg.Unique_tbls {
		bat := batch.New(true, []string{name})
		bat.Cnt = 1
		bat.Vecs[0] = vector.New(types.New(types.T_text,
			0, 0))
		arg.container.mp[i+1] = bat
	}
}

func (arg *Argument) Split(proc *process.Process, bat *batch.Batch) error {
	arg.GetMetaLocBat(bat.Attrs[1])
	tblIdx := vector.MustTCols[int16](bat.GetVector(0))
	metaLocs := vector.MustStrCols(bat.GetVector(1))
	for i := range tblIdx {
		if tblIdx[i] >= 0 {
			if tblIdx[i] == 0 {
				val, err := strconv.ParseUint(strings.Split(metaLocs[i], ":")[2], 0, 64)
				if err != nil {
					return err
				}
				arg.AffectedRows += val
			}
			arg.container.mp[int(tblIdx[i])].Vecs[0].Append([]byte(metaLocs[i]), false, proc.GetMPool())
		} else {
			idx := int(-(tblIdx[i] + 1))
			bat := &batch.Batch{}
			if err := bat.UnmarshalBinary([]byte(metaLocs[i])); err != nil {
				return err
			}
			if idx == 0 {
				arg.AffectedRows += uint64(bat.Length())
			}
			arg.container.mp2[idx] = append(arg.container.mp2[idx], bat)
		}
	}
	for _, bat := range arg.container.mp {
		bat.SetZs(bat.Vecs[0].Length(), proc.GetMPool())
	}
	return nil
}
