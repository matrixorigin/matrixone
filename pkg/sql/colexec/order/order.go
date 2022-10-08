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

package order

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/partition"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString("τ([")
	for i, f := range ap.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(Container)
	{
		ap.ctr.ds = make([]bool, len(ap.Fs))
		ap.ctr.vecs = make([]evalVector, len(ap.Fs))
		for i, f := range ap.Fs {
			ap.ctr.ds[i] = f.Type == colexec.Descending
		}
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	bat := proc.Reg.InputBatch
	if bat == nil {
		return true, nil
	}
	if bat.Length() == 0 {
		return false, nil
	}
	ap := arg.(*Argument)
	return ap.ctr.process(ap, bat, proc)
}

func (ctr *Container) process(ap *Argument, bat *batch.Batch, proc *process.Process) (bool, error) {
	for i := 0; i < bat.VectorCount(); i++ {
		vec := bat.GetVector(int32(i))
		if vec.IsOriginal() {
			nvec, err := vector.Dup(bat.Vecs[i], proc.Mp())
			if err != nil {
				return false, err
			}
			bat.SetVector(int32(i), nvec)

		}
	}
	for i, f := range ap.Fs {
		vec, err := colexec.EvalExpr(bat, proc, f.E)
		if err != nil {
			for j := 0; j < i; j++ {
				if ctr.vecs[j].needFree {
					vector.Clean(ctr.vecs[j].vec, proc.Mp())
				}
			}
			return false, err
		}
		ctr.vecs[i].vec = vec
		ctr.vecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.vecs[i].needFree = false
				break
			}
		}
	}
	defer func() {
		for i := range ctr.vecs {
			if ctr.vecs[i].needFree {
				vector.Clean(ctr.vecs[i].vec, proc.Mp())
			}
		}
	}()
	ovec := ctr.vecs[0].vec
	n := len(bat.Zs)
	sels := make([]int64, n)
	for i := range sels {
		sels[i] = int64(i)
	}
	sort.Sort(ctr.ds[0], sels, ovec)
	if len(ctr.vecs) == 1 {
		if err := bat.Shuffle(sels, proc.Mp()); err != nil {
			panic(err)
		}
		return false, nil
	}
	ps := make([]int64, 0, 16)
	ds := make([]bool, len(sels))
	for i, j := 1, len(ctr.vecs); i < j; i++ {
		desc := ctr.ds[i]
		ps = partition.Partition(sels, ds, ps, ovec)
		vec := ctr.vecs[i].vec
		for i, j := 0, len(ps); i < j; i++ {
			if i == j-1 {
				sort.Sort(desc, sels[ps[i]:], vec)
			} else {
				sort.Sort(desc, sels[ps[i]:ps[i+1]], vec)
			}
		}
		ovec = vec
	}
	if err := bat.Shuffle(sels, proc.Mp()); err != nil {
		panic(err)
	}
	return false, nil
}
