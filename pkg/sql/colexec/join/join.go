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

package join

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/index"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" inner join ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	ap.ctr.evecs = make([]evalVector, len(ap.Conditions[0]))
	ap.ctr.vecs = make([]*vector.Vector, len(ap.Conditions[0]))
	return nil
}

func Call(idx int, proc *process.Process, arg any) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc, anal); err != nil {
				ap.Free(proc, true)
				return false, err
			}
			ctr.state = Probe

		case Probe:
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.Length() == 0 {
				continue
			}
			if ctr.bat == nil || ctr.bat.Length() == 0 {
				bat.Clean(proc.Mp())
				continue
			}
			if err := ctr.probe(bat, ap, proc, anal); err != nil {
				ap.Free(proc, true)
				return false, err
			}
			return false, nil

		default:
			ap.Free(proc, false)
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze) error {
	bat := <-proc.Reg.MergeReceivers[1].Ch
	if bat != nil {
		ctr.bat = bat
		ctr.mp = bat.Ht.(*hashmap.JoinMap).Dup()
	}
	return nil
}

func (ctr *container) probe(bat *batch.Batch, ap *Argument, proc *process.Process, anal process.Analyze) error {
	defer bat.Clean(proc.Mp())
	anal.Input(bat)
	rbat := batch.NewWithSize(len(ap.Result))
	rbat.Zs = proc.Mp().GetSels()
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			rbat.Vecs[i] = vector.New(bat.Vecs[rp.Pos].Typ)
		} else {
			rbat.Vecs[i] = vector.New(ctr.bat.Vecs[rp.Pos].Typ)
		}
	}

	idxFlg := false
	ctr.cleanEvalVectors(proc.Mp())
	if err := ctr.evalJoinCondition(bat, ap.Conditions[0], proc, &idxFlg); err != nil {
		return err
	}

	mSels := ctr.mp.Sels()
	if idxFlg {
		if err := ctr.indexProbe(ap, bat, rbat, mSels, proc); err != nil {
			rbat.Clean(proc.Mp())
			return err
		}
		rbat.ExpandNulls()
		anal.Output(rbat)
		proc.SetInputBatch(rbat)
		return nil
	}

	count := bat.Length()
	itr := ctr.mp.Map().NewIterator()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		copy(ctr.inBuckets, hashmap.OneUInt8s)
		vals, zvals := itr.Find(i, n, ctr.vecs, ctr.inBuckets)
		for k := 0; k < n; k++ {
			if ctr.inBuckets[k] == 0 || zvals[k] == 0 || vals[k] == 0 {
				continue
			}
			sels := mSels[vals[k]-1]
			if ap.Cond != nil {
				for _, sel := range sels {
					vec, err := colexec.JoinFilterEvalExprInBucket(bat, ctr.bat, i+k, int(sel), proc, ap.Cond)
					if err != nil {
						return err
					}
					bs := vec.Col.([]bool)
					if !bs[0] {
						vec.Free(proc.Mp())
						continue
					}
					vec.Free(proc.Mp())
					for j, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := vector.UnionOne(rbat.Vecs[j], bat.Vecs[rp.Pos], int64(i+k), proc.Mp()); err != nil {
								rbat.Clean(proc.Mp())
								return err
							}
						} else {
							if err := vector.UnionOne(rbat.Vecs[j], ctr.bat.Vecs[rp.Pos], sel, proc.Mp()); err != nil {
								rbat.Clean(proc.Mp())
								return err
							}
						}
					}
					rbat.Zs = append(rbat.Zs, ctr.bat.Zs[sel])
				}
			} else {
				for j, rp := range ap.Result {
					if rp.Rel == 0 {
						if err := vector.UnionMulti(rbat.Vecs[j], bat.Vecs[rp.Pos], int64(i+k), len(sels), proc.Mp()); err != nil {
							rbat.Clean(proc.Mp())
							return err
						}
					} else {
						if err := vector.Union(rbat.Vecs[j], ctr.bat.Vecs[rp.Pos], sels, true, proc.Mp()); err != nil {
							rbat.Clean(proc.Mp())
							return err
						}
					}
				}
				for _, sel := range sels {
					rbat.Zs = append(rbat.Zs, ctr.bat.Zs[sel])
				}
			}
		}
	}
	rbat.ExpandNulls()
	anal.Output(rbat)
	proc.SetInputBatch(rbat)
	return nil
}

func (ctr *container) indexProbe(ap *Argument, bat, rbat *batch.Batch, mSels [][]int64, proc *process.Process) error {
	col := vector.MustTCols[uint16](ctr.vecs[0])
	for i, v := range col {
		if v == 0 {
			continue
		}
		sels := mSels[v-1]
		for _, sel := range sels {
			if ap.Cond != nil {
				vec, err := colexec.JoinFilterEvalExprInBucket(bat, ctr.bat, i, int(sel), proc, ap.Cond)
				if err != nil {
					return err
				}
				bs := vector.MustTCols[bool](vec)
				if !bs[0] {
					vec.Free(proc.Mp())
					continue
				}
				vec.Free(proc.Mp())
			}
			for j, rp := range ap.Result {
				if rp.Rel == 0 {
					if err := vector.UnionOne(rbat.Vecs[j], bat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
						return err
					}
					if err := populateIndex(rbat.Vecs[j], bat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
						return err
					}
				} else {
					if err := vector.UnionOne(rbat.Vecs[j], ctr.bat.Vecs[rp.Pos], sel, proc.Mp()); err != nil {
						return err
					}
					if err := populateIndex(rbat.Vecs[j], ctr.bat.Vecs[rp.Pos], sel, proc.Mp()); err != nil {
						return err
					}
				}
			}
			rbat.Zs = append(rbat.Zs, ctr.bat.Zs[sel])
		}
	}
	return nil
}

func (ctr *container) evalJoinCondition(bat *batch.Batch, conds []*plan.Expr, proc *process.Process, flg *bool) error {
	for i, cond := range conds {
		vec, err := colexec.EvalExpr(bat, proc, cond)
		if err != nil || vec.ConstExpand(proc.Mp()) == nil {
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

		if *flg, err = ctr.dictEncoding(proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) dictEncoding(m *mpool.MPool) (bool, error) {
	// find out whether hashbuild is built by low cardinality index
	idx := ctr.mp.Index()
	if idx == nil {
		return false, nil
	}

	vec := ctr.vecs[0]
	encoded := vector.New(types.Type{Oid: types.T_uint16})
	// case 1
	// 1. the join columns of both left table and right table are indexed
	// 2. left condition is not an expression
	if vec.IsLowCardinality() && !ctr.evecs[0].needFree {
		leftIdx := ctr.vecs[0].Index().(*index.LowCardinalityIndex)
		// e.g. idx.dict = ["a"->1, "b"->2, "c"->3]
		//      leftIdx.dict = ["c"->1, "d"->2, "b"->3, "a"->4]
		//      mapping => fixed map = [3, 0, 2, 1]
		fixedMap := idx.GetDict().FindBatch(leftIdx.GetDict().GetUnique())
		poses := vector.MustTCols[uint16](leftIdx.GetPoses())
		col := make([]uint16, len(poses))
		for i, pos := range poses {
			if pos == 0 {
				continue
			}
			col[i] = fixedMap[pos-1]
		}
		if err := vector.AppendFixed(encoded, col, m); err != nil {
			encoded.Free(m)
			return false, err
		}
	} else {
		// case 2
		// 1. only the join column of right table is indexed
		// 2. it does not matter if left is an expression or not
		if err := idx.Encode(encoded, vec); err != nil {
			encoded.Free(m)
			vec.Free(m)
			return false, err
		}
		if ctr.evecs[0].needFree {
			vec.Free(m)
		}
	}
	ctr.vecs[0] = encoded
	ctr.evecs[0].vec = encoded
	ctr.evecs[0].needFree = true
	return true, nil
}

func populateIndex(result, selected *vector.Vector, row int64, m *mpool.MPool) error {
	if !selected.IsLowCardinality() {
		return nil
	}

	idx := selected.Index().(*index.LowCardinalityIndex)
	if result.Index() == nil {
		result.SetIndex(idx.DupEmpty())
	}

	resultIdx := result.Index().(*index.LowCardinalityIndex)
	dst, src := resultIdx.GetPoses(), idx.GetPoses()
	return vector.UnionOne(dst, src, row, m)
}
