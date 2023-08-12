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

package projection

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("projection(")
	for i, e := range n.Es {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(e.String())
	}
	buf.WriteString(")")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.projExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, ap.Es)

	return err
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	bat := proc.InputBatch()
	ap := arg.(*Argument)

	if bat == nil {
		if ap.IsLog {
			logutil.Infof("Table[%s] projection operator input batch is NULL, ap ptr[%p]", ap.TableName, ap)
		}
		proc.SetInputBatch(nil)
		return process.ExecStop, nil
	}
	if bat.Last() {
		if ap.IsLog {
			logutil.Infof("Table[%s] projection operator input batch is last, ap ptr[%p], bat ptr[%p], bat cnt: %d", ap.TableName, ap, bat, bat.Cnt)
		}
		proc.SetInputBatch(bat)
		return process.ExecNext, nil
	}
	if bat.IsEmpty() {
		if ap.IsLog {
			logutil.Infof("Table[%s] projection operator input batch is empty, ap ptr[%p], bat ptr[%p], bat cnt: %d", ap.TableName, ap, bat, bat.Cnt)
		}
		bat.Clean(proc.Mp())
		proc.SetInputBatch(batch.EmptyBatch)
		return process.ExecNext, nil
	}

	if ap.IsLog {
		logutil.Infof("Table[%s] projection operator input batch: %s, ap ptr[%p], bat ptr[%p], bat cnt: %d", ap.TableName, bat.PrintBatch(), ap, bat, bat.Cnt)
	}

	anal.Input(bat, isFirst)
	//ap := arg.(*Argument)
	rbat := batch.NewWithSize(len(ap.Es))

	// do projection.
	for i := range ap.ctr.projExecutors {
		vec, err := ap.ctr.projExecutors[i].Eval(proc, []*batch.Batch{bat})
		if err != nil {
			if ap.IsLog {
				logutil.Infof("Table[%s] projection operator do projection err1: %s, ap ptr[%p], bat ptr[%p], bat cnt: %d", ap.TableName, err.Error(), ap, bat, bat.Cnt)
			}
			return process.ExecNext, err
		}
		rbat.Vecs[i] = vec
	}

	newAlloc, err := colexec.FixProjectionResult(proc, ap.ctr.projExecutors, rbat, bat)
	if err != nil {
		if ap.IsLog {
			logutil.Infof("Table[%s] projection operator do projection err2: %s, ap ptr[%p]", ap.TableName, err.Error(), ap)
		}
		bat.Clean(proc.Mp())
		return process.ExecNext, err
	}
	anal.Alloc(int64(newAlloc))

	rbat.SetRowCount(bat.RowCount())

	proc.PutBatch(bat)
	anal.Output(rbat, isLast)
	proc.SetInputBatch(rbat)
	if ap.IsLog {
		rbat.TableName = ap.TableName
		logutil.Infof("Table[%s], process ptr[%s] projection operator output batch: %s, ap ptr[%p], rbat ptr[%p], rbat cnt: %d", proc, ap.TableName, rbat.PrintBatch(), ap, rbat, rbat.Cnt)
	}
	return process.ExecNext, nil
}
