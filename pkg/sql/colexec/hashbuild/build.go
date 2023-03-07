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

package hashbuild

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/index"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" hash build ")
}

func Prepare(proc *process.Process, arg any) error {
	var err error

	ap := arg.(*Argument)
	ap.ctr = new(container)
	if ap.NeedHashMap {
		if ap.ctr.mp, err = hashmap.NewStrMap(false, ap.Ibucket, ap.Nbucket, proc.Mp()); err != nil {
			return err
		}
		ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions))
		ap.ctr.evecs = make([]evalVector, len(ap.Conditions))
		ap.ctr.nullSels = make([]int32, 0)
	}
	ap.ctr.bat = batch.NewWithSize(len(ap.Typs))
	ap.ctr.bat.Zs = proc.Mp().GetSels()
	for i, typ := range ap.Typs {
		ap.ctr.bat.Vecs[i] = vector.New(typ)
	}

	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, _ bool) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, anal, isFirst); err != nil {
				ap.Free(proc, true)
				return false, err
			}
			if ap.ctr.mp != nil {
				anal.Alloc(ap.ctr.mp.Size())
			}
			ctr.state = End
		default:
			if ctr.bat != nil {
				if ap.NeedHashMap {
					ctr.bat.Ht = hashmap.NewJoinMap(ctr.sels, ctr.nullSels, nil, ctr.mp, ctr.hasNull, ctr.idx)
				}
				proc.SetInputBatch(ctr.bat)
				ctr.mp = nil
				ctr.bat = nil
				ctr.sels = nil
				ctr.nullSels = nil
			} else {
				proc.SetInputBatch(nil)
			}
			ap.Free(proc, false)
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool) error {
	var err error

	for {
		start := time.Now()
		bat := <-proc.Reg.MergeReceivers[0].Ch
		anal.WaitStop(start)

		if bat == nil {
			break
		}
		if bat.Length() == 0 {
			continue
		}
		anal.Input(bat, isFirst)
		anal.Alloc(int64(bat.Size()))
		if ctr.bat, err = ctr.bat.Append(proc.Ctx, proc.Mp(), bat); err != nil {
			return err
		}
		bat.Clean(proc.Mp())
	}
	if ctr.bat == nil || ctr.bat.Length() == 0 || !ap.NeedHashMap {
		return nil
	}
	ctr.cleanEvalVectors(proc.Mp())
	if err = ctr.evalJoinCondition(ctr.bat, ap.Conditions, proc, anal); err != nil {
		return err
	}

	if ctr.idx != nil {
		return ctr.indexBuild()
	}

	inBuckets := make([]uint8, hashmap.UnitLimit)

	itr := ctr.mp.NewIterator()
	count := ctr.bat.Length()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		rows := ctr.mp.GroupCount()
		vals, zvals, err := itr.Insert(i, n, ctr.vecs)
		if err != nil {
			return err
		}
		for k, v := range vals[:n] {
			if zvals[k] == 0 {
				ctr.hasNull = true
				continue
			}
			if v == 0 {
				continue
			}
			if v > rows {
				ctr.sels = append(ctr.sels, make([]int32, 0))
			}
			ai := int64(v) - 1
			ctr.sels[ai] = append(ctr.sels[ai], int32(i+k))
		}
		if ap.IsRight {
			copy(inBuckets, hashmap.OneUInt8s)
			_, zvals = itr.Find(i, n, ctr.vecs, inBuckets)
			for k := 0; k < n; k++ {
				if inBuckets[k] == 0 {
					continue
				}
				if zvals[k] == 0 {
					ctr.nullSels = append(ctr.nullSels, int32(i+k))
				}
			}
		}

	}

	return nil
}

func (ctr *container) indexBuild() error {
	// e.g. original data = ["a", "b", "a", "c", "b", "c", "a", "a"]
	//      => dictionary = ["a"->1, "b"->2, "c"->3]
	//      => poses = [1, 2, 1, 3, 2, 3, 1, 1]
	// sels = [[0, 2, 6, 7], [1, 4], [3, 5]]
	ctr.sels = make([][]int32, index.MaxLowCardinality)
	poses := vector.MustTCols[uint16](ctr.idx.GetPoses())
	for k, v := range poses {
		if v == 0 {
			continue
		}
		bucket := int(v) - 1
		if len(ctr.sels[bucket]) == 0 {
			ctr.sels[bucket] = make([]int32, 0, 64)
		}
		ctr.sels[bucket] = append(ctr.sels[bucket], int32(k))
	}
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, conds []*plan.Expr, proc *process.Process, analyze process.Analyze) error {
	for i, cond := range conds {
		vec, err := colexec.EvalExpr(bat, proc, cond)
		if err != nil || vec.ConstExpand(false, proc.Mp()) == nil {
			ctr.cleanEvalVectors(proc.Mp())
			return err
		}
		ctr.vecs[i] = vec
		ctr.evecs[i].vec = vec
		ctr.evecs[i].needFree = true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				ctr.evecs[i].needFree = false
				break
			}
		}
		if ctr.evecs[i].needFree && vec != nil {
			analyze.Alloc(int64(vec.Size()))
		}

	}
	return nil
}
