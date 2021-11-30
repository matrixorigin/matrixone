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

package times

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString(" ⨯  ")
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.ctr = new(Container)
	transform.Prepare(proc, n.Arg)
	n.ctr.state = Fill
	n.ctr.views = make([]*view, len(n.Ss))
	n.ctr.inserts = make([]uint8, UnitLimit)
	n.ctr.zinserts = make([]uint8, UnitLimit)
	n.ctr.hashs = make([]uint64, UnitLimit)
	n.ctr.values = make([]*uint64, UnitLimit)
	n.ctr.zvalues = make([]*uint64, UnitLimit)
	n.ctr.h8.keys = make([]uint64, UnitLimit)
	for i := 0; i < len(n.Ss); i++ {
		n.ctr.views[i] = &view{
			rn:  n.Ss[i],
			key: n.Svars[i],
			isB: n.SisBares[i],
		}
	}
	n.ctr.constructVars(n)
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	if n.ctr.state == End {
		proc.Reg.InputBatch = nil
		return true, nil
	}
	return false, nil
	/*
		if n.ctr.state == Fill {
			if err := n.ctr.fill(n.Bats, proc); err != nil {
				proc.Reg.InputBatch = nil
				n.ctr.state = End
				return true, err
			}
			n.ctr.state = Probe
		}
		if _, err := transform.Call(proc, n.Arg); err != nil {
			n.ctr.state = End
			proc.Reg.InputBatch = nil
			return false, err
		}
		bat := proc.Reg.InputBatch
		if bat == nil {
			n.ctr.state = End
			proc.Reg.InputBatch = nil
			return true, nil
		}
		if len(bat.Zs) == 0 {
			proc.Reg.InputBatch = &batch.Batch{}
			return false, nil
		}
		if err := n.ctr.probe(n.Arg.Ctr.Is, bat, n, proc); err != nil {
			proc.Reg.InputBatch = nil
			n.ctr.state = End
			return true, err
		}
		proc.Reg.InputBatch = n.ctr.bat
		return false, nil
	*/
}

/*
func (ctr *Container) fill(bats []*batch.Batch, proc *process.Process) error {
	for i := 0; i < len(bats); i++ {
		bat := bats[i]
		if bat == nil {
			continue
		}
		if len(bat.Zs) == 0 {
			i--
			continue
		}
		if err := ctr.fillBatch(ctr.views[i], bat, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) probe(is []int, bat *batch.Batch, arg *Argument, proc *process.Process) error {
	ctr.rows = 0
	defer batch.Clean(bat, proc.Mp)
	ctr.constructBatch(arg.R, arg.VarsMap, bat)
	return ctr.probeBatch(is, arg, bat, proc)
}
*/

/*
func (ctr *Container) probeBatch(is []int, arg *Argument, bat *batch.Batch, proc *process.Process) error {
	var vec *vector.Vector

	if err := ctr.newBatch(is, arg, bat, proc); err != nil {
		return err
	}
	for vi := 1; vi < len(arg.Rvars); vi++ {
		{
			fmt.Printf("********process vi: %v\n", vi)
		}
		if arg.VarsMap[arg.Rvars[vi]] > 1 {
			vec = batch.GetVector(ctr.bat, arg.R+"."+arg.Rvars[vi])
		} else {
			vec = batch.GetVector(ctr.bat, arg.Rvars[vi])
		}
		v := ctr.views[vi]
		switch v.typ.Oid {
		case types.T_uint32:
			vs := vec.Col.([]uint32)
			count := int64(len(bat.Zs))
			if v.isB {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							ctr.bat.Zs[i+int64(k)] = 0 // clear
							continue
						}
						sel := int64(*vp)
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sel]
						{ // vector fill
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sel, proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			} else {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sels := v.sels[*vp]
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sels[0]]
						{
							for i := 1; i < len(sels); i++ {
								z0 += v.bat.Zs[sels[i]]
							}
						}
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								for _, sel := range sels {
									ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								}
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			}
		}
	}
	batch.Reduce(ctr.bat, ctr.vars, proc.Mp)
	{
		fmt.Printf("count: %p - %v, %v\n", ctr.bat, ctr.bat.Attrs, len(ctr.bat.Zs))
		for j, vec := range ctr.bat.Vecs {
			{
				if vec != nil {
					fmt.Printf("\t[%v] %v, = %v\n", bat.Attrs[j], j, vector.Length(vec))
				}
			}
		}
	}
	return nil
}
*/

/*
func (ctr *Container) newBatch(is []int, arg *Argument, bat *batch.Batch, proc *process.Process) error {
	v := ctr.views[0]
	vec := batch.GetVector(bat, arg.Rvars[0])
	switch v.typ.Oid {
	case types.T_uint32:
		vs := vec.Col.([]uint32)
		count := int64(len(bat.Zs))
		if v.isB {
			for i := int64(0); i < count; i += UnitLimit {
				n := int(count - i)
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						ctr.h8.keys[k] = uint64(vs[int(i)+k])
					}
				}
				ctr.hashs[0] = 0
				copy(ctr.values, ctr.zvalues)
				v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
				for k, vp := range ctr.values {
					if vp == nil {
						continue
					}
					sel := int64(*vp)
					z := bat.Zs[i+int64(k)]
					z0 := v.bat.Zs[sel]
					{ // vector fill
						for j, vec := range bat.Vecs {
							{
								if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
									continue
								}
								if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
									continue
								}
							}
							if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
								return err
							}
						}
						for j, vec := range v.bat.Vecs {
							{
								if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
									continue
								}
							}
							if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sel, proc.Mp); err != nil {
								return err
							}
						}
					}
					{ // ring fill
						for j := range bat.Rs {
							if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
								return err
							}
							ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
							ctr.bat.Rs[j].Mul(ctr.rows, z0)
						}
						for j, r := range v.bat.Rs {
							if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
								return err
							}
							ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
							ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
						}
						ctr.rows++
						ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
					}
				}
			}
		} else {
			for i := int64(0); i < count; i += UnitLimit {
				n := int(count - i)
				if n > UnitLimit {
					n = UnitLimit
				}
				{
					for k := 0; k < n; k++ {
						ctr.h8.keys[k] = uint64(vs[int(i)+k])
					}
				}
				ctr.hashs[0] = 0
				copy(ctr.values, ctr.zvalues)
				v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
				for k, vp := range ctr.values {
					if vp == nil {
						continue
					}
					sels := v.sels[*vp]
					z := bat.Zs[i+int64(k)]
					z0 := v.bat.Zs[sels[0]]
					{
						for i := 1; i < len(sels); i++ {
							z0 += v.bat.Zs[sels[i]]
						}
					}
					{ // vector fill
						for j, vec := range bat.Vecs {
							{
								if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
									continue
								}
								if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
									continue
								}
							}
							if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
								return err
							}
						}
						for j, vec := range v.bat.Vecs {
							{
								if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
									continue
								}
							}
							if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
								return err
							}
						}
					}
					{ // ring fill
						for j := range bat.Rs {
							if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
								return err
							}
							ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
							ctr.bat.Rs[j].Mul(ctr.rows, z0)
						}
						for j, r := range v.bat.Rs {
							if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
								return err
							}
							for _, sel := range sels {
								ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
							}
							ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
						}
						ctr.rows++
						ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
					}
				}
			}
		}
	}
	return nil
}
*/

/*
func (ctr *Container) probeBatch(is []int, arg *Argument, bat *batch.Batch, proc *process.Process) error {
	for vi := 0; vi < len(arg.Rvars); vi++ {
		vec := batch.GetVector(bat, arg.Rvars[vi])
		v := ctr.views[vi]
		switch v.typ.Oid {
		case types.T_int8:
			vs := vec.Col.([]int8)
			count := int64(len(bat.Zs))
			if v.isB {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sel := int64(*vp)
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sel]
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sel, proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			} else {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sels := v.sels[*vp]
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sels[0]]
						{
							for i := 1; i < len(sels); i++ {
								z0 += v.bat.Zs[sels[i]]
							}
						}
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								for _, sel := range sels {
									ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								}
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			}
		case types.T_int16:
			vs := vec.Col.([]int16)
			count := int64(len(bat.Zs))
			if v.isB {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sel := int64(*vp)
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sel]
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sel, proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			} else {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sels := v.sels[*vp]
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sels[0]]
						{
							for i := 1; i < len(sels); i++ {
								z0 += v.bat.Zs[sels[i]]
							}
						}
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								for _, sel := range sels {
									ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								}
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			}
		case types.T_int32:
			vs := vec.Col.([]int32)
			count := int64(len(bat.Zs))
			if v.isB {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sel := int64(*vp)
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sel]
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sel, proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			} else {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sels := v.sels[*vp]
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sels[0]]
						{
							for i := 1; i < len(sels); i++ {
								z0 += v.bat.Zs[sels[i]]
							}
						}
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								for _, sel := range sels {
									ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								}
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			}
		case types.T_int64:
			vs := vec.Col.([]int64)
			count := int64(len(bat.Zs))
			if v.isB {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sel := int64(*vp)
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sel]
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sel, proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			} else {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sels := v.sels[*vp]
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sels[0]]
						{
							for i := 1; i < len(sels); i++ {
								z0 += v.bat.Zs[sels[i]]
							}
						}
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								for _, sel := range sels {
									ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								}
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			}
		case types.T_uint8:
			vs := vec.Col.([]uint8)
			count := int64(len(bat.Zs))
			if v.isB {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sel := int64(*vp)
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sel]
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sel, proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			} else {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sels := v.sels[*vp]
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sels[0]]
						{
							for i := 1; i < len(sels); i++ {
								z0 += v.bat.Zs[sels[i]]
							}
						}
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								for _, sel := range sels {
									ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								}
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			}
		case types.T_uint16:
			vs := vec.Col.([]uint16)
			count := int64(len(bat.Zs))
			if v.isB {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sel := int64(*vp)
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sel]
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sel, proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			} else {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sels := v.sels[*vp]
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sels[0]]
						{
							for i := 1; i < len(sels); i++ {
								z0 += v.bat.Zs[sels[i]]
							}
						}
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								for _, sel := range sels {
									ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								}
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			}
		case types.T_uint32:
			vs := vec.Col.([]uint32)
			count := int64(len(bat.Zs))
			if v.isB {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sel := int64(*vp)
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sel]
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sel, proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
				{
					fmt.Printf("join %v: 1 ctr.bat: %p - %v\n", v.key, ctr.bat, ctr.bat.Attrs)
					for i, vec := range ctr.bat.Vecs {
						fmt.Printf("\t[%v] = %p\n", i, vec)
					}
				}
			} else {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sels := v.sels[*vp]
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sels[0]]
						{
							for i := 1; i < len(sels); i++ {
								z0 += v.bat.Zs[sels[i]]
							}
						}
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								for _, sel := range sels {
									ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								}
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			}
		case types.T_uint64:
			vs := vec.Col.([]uint64)
			count := int64(len(bat.Zs))
			if v.isB {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sel := int64(*vp)
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sel]
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sel, proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			} else {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sels := v.sels[*vp]
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sels[0]]
						{
							for i := 1; i < len(sels); i++ {
								z0 += v.bat.Zs[sels[i]]
							}
						}
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								for _, sel := range sels {
									ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								}
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			}
		case types.T_float32:
			vs := vec.Col.([]float32)
			count := int64(len(bat.Zs))
			if v.isB {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sel := int64(*vp)
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sel]
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sel, proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			} else {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sels := v.sels[*vp]
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sels[0]]
						{
							for i := 1; i < len(sels); i++ {
								z0 += v.bat.Zs[sels[i]]
							}
						}
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								for _, sel := range sels {
									ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								}
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			}
		case types.T_float64:
			vs := vec.Col.([]float64)
			count := int64(len(bat.Zs))
			if v.isB {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sel := int64(*vp)
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sel]
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sel, proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			} else {
				for i := int64(0); i < count; i += UnitLimit {
					n := int(count - i)
					if n > UnitLimit {
						n = UnitLimit
					}
					{
						for k := 0; k < n; k++ {
							ctr.h8.keys[k] = uint64(vs[int(i)+k])
						}
					}
					ctr.hashs[0] = 0
					copy(ctr.values, ctr.zvalues)
					v.h8.ht.FindBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values[:n])
					for k, vp := range ctr.values {
						if vp == nil {
							continue
						}
						sels := v.sels[*vp]
						z := bat.Zs[i+int64(k)]
						z0 := v.bat.Zs[sels[0]]
						{
							for i := 1; i < len(sels); i++ {
								z0 += v.bat.Zs[sels[i]]
							}
						}
						{ // vector fill
							for j, vec := range bat.Vecs {
								{
									if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
										continue
									}
									if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i+int64(k), proc.Mp); err != nil {
									return err
								}
							}
							for j, vec := range v.bat.Vecs {
								{
									if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
										continue
									}
								}
								if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
									return err
								}
							}
						}
						{ // ring fill
							for j := range bat.Rs {
								if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
									return err
								}
								ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
								ctr.bat.Rs[j].Mul(ctr.rows, z0)
							}
							for j, r := range v.bat.Rs {
								if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
									return err
								}
								for _, sel := range sels {
									ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
								}
								ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
							}
							ctr.rows++
							ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
						}
					}
				}
			}
		case types.T_char, types.T_varchar:
			vs := vec.Col.(*types.Bytes)
			count := int64(len(bat.Zs))
			for i := int64(0); i < count; i++ {
				key := vs.Get(i)
				vp := v.hstr.ht.Find(0, hashtable.StringRef{Ptr: &key[0], Len: len(key)})
				if vp == nil {
					continue
				}
				z := bat.Zs[i]
				sels := v.sels[*vp]
				z0 := v.bat.Zs[sels[0]]
				{
					for i := 1; i < len(sels); i++ {
						z0 += v.bat.Zs[sels[i]]
					}
				}
				{ // vector fill
					for j, vec := range bat.Vecs {
						{
							if _, ok := ctr.fvarsMap[bat.Attrs[j]]; !ok {
								continue
							}
							if _, ok := ctr.varsMap[ctr.bat.Attrs[ctr.is[j]]]; ok && vec.Ref == 1 {
								continue
							}
						}
						if err := vector.UnionOne(ctr.bat.Vecs[j], vec, i, proc.Mp); err != nil {
							return err
						}
					}
					for j, vec := range v.bat.Vecs {
						{
							if _, ok := ctr.varsMap[ctr.bat.Attrs[v.vis[j]]]; ok && vec.Ref == 1 {
								continue
							}
						}
						if err := vector.UnionOne(ctr.bat.Vecs[v.vis[j]], vec, sels[0], proc.Mp); err != nil {
							return err
						}
					}
				}
				{ // ring fill
					for j := range bat.Rs {
						if err := ctr.bat.Rs[j].Grow(proc.Mp); err != nil {
							return err
						}
						ctr.bat.Rs[j].Fill(ctr.rows, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[is[j]])
						ctr.bat.Rs[j].Mul(ctr.rows, z0)
					}
					for j, r := range v.bat.Rs {
						if err := ctr.bat.Rs[v.ris[j]].Grow(proc.Mp); err != nil {
							return err
						}
						for _, sel := range sels {
							ctr.bat.Rs[v.ris[j]].Add(r, ctr.rows, sel)
						}
						ctr.bat.Rs[v.ris[j]].Mul(ctr.rows, z)
					}
					ctr.rows++
					ctr.bat.Zs = append(ctr.bat.Zs, z*z0)
				}
			}
		}
	}
	{
		fmt.Printf("count: %p - %v, %v\n", ctr.bat, ctr.bat.Attrs, len(ctr.bat.Zs))
		for j, vec := range ctr.bat.Vecs {
			{
				if vec != nil {
					fmt.Printf("\t[%v] %v, = %v\n", bat.Attrs[j], j, vector.Length(vec))
				}
			}
		}
	}
	batch.Reduce(ctr.bat, ctr.vars, proc.Mp)
	return nil
}
*/

/*
func (ctr *Container) fillBatch(v *view, bat *batch.Batch, proc *process.Process) error {
	v.bat = bat
	vec := batch.GetVector(bat, v.key)
	switch v.typ = vec.Typ; v.typ.Oid {
	case types.T_int8:
		if v.isB && bat.Ht != nil {
			v.h8.ht = bat.Ht.(*hashtable.Int64HashMap)
			return nil
		}
		v.h8.ht = &hashtable.Int64HashMap{}
		v.h8.ht.Init()
		vs := vec.Col.([]int8)
		count := int64(len(bat.Zs))
		for i := int64(0); i < count; i += UnitLimit {
			n := int(count - i)
			if n > UnitLimit {
				n = UnitLimit
			}
			{
				for k := 0; k < n; k++ {
					ctr.h8.keys[k] = uint64(vs[int(i)+k])
				}
			}
			ctr.hashs[0] = 0
			copy(ctr.inserts[:n], ctr.zinserts[:n])
			if err := v.h8.ht.InsertBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values); err != nil {
				batch.Clean(bat, proc.Mp)
				return err
			}
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = v.rows
					v.rows++
					v.sels = append(v.sels, make([]int64, 0, 8))
				}
				ai := int64(*ctr.values[k])
				v.sels[ai] = append(v.sels[ai], i+int64(k))
			}
		}
	case types.T_int16:
		if v.isB && bat.Ht != nil {
			v.h8.ht = bat.Ht.(*hashtable.Int64HashMap)
			return nil
		}
		v.h8.ht = &hashtable.Int64HashMap{}
		v.h8.ht.Init()
		vs := vec.Col.([]int16)
		count := int64(len(bat.Zs))
		for i := int64(0); i < count; i += UnitLimit {
			n := int(count - i)
			if n > UnitLimit {
				n = UnitLimit
			}
			{
				for k := 0; k < n; k++ {
					ctr.h8.keys[k] = uint64(vs[int(i)+k])
				}
			}
			ctr.hashs[0] = 0
			copy(ctr.inserts[:n], ctr.zinserts[:n])
			if err := v.h8.ht.InsertBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values); err != nil {
				batch.Clean(bat, proc.Mp)
				return err
			}
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = v.rows
					v.rows++
					v.sels = append(v.sels, make([]int64, 0, 8))
				}
				ai := int64(*ctr.values[k])
				v.sels[ai] = append(v.sels[ai], i+int64(k))
			}
		}
	case types.T_int32:
		if v.isB && bat.Ht != nil {
			v.h8.ht = bat.Ht.(*hashtable.Int64HashMap)
			return nil
		}
		v.h8.ht = &hashtable.Int64HashMap{}
		v.h8.ht.Init()
		vs := vec.Col.([]int32)
		count := int64(len(bat.Zs))
		for i := int64(0); i < count; i += UnitLimit {
			n := int(count - i)
			if n > UnitLimit {
				n = UnitLimit
			}
			{
				for k := 0; k < n; k++ {
					ctr.h8.keys[k] = uint64(vs[int(i)+k])
				}
			}
			ctr.hashs[0] = 0
			copy(ctr.inserts[:n], ctr.zinserts[:n])
			if err := v.h8.ht.InsertBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values); err != nil {
				batch.Clean(bat, proc.Mp)
				return err
			}
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = v.rows
					v.rows++
					v.sels = append(v.sels, make([]int64, 0, 8))
				}
				ai := int64(*ctr.values[k])
				v.sels[ai] = append(v.sels[ai], i+int64(k))
			}
		}
	case types.T_int64:
		if v.isB && bat.Ht != nil {
			v.h8.ht = bat.Ht.(*hashtable.Int64HashMap)
			return nil
		}
		v.h8.ht = &hashtable.Int64HashMap{}
		v.h8.ht.Init()
		vs := vec.Col.([]int64)
		count := int64(len(bat.Zs))
		for i := int64(0); i < count; i += UnitLimit {
			n := int(count - i)
			if n > UnitLimit {
				n = UnitLimit
			}
			{
				for k := 0; k < n; k++ {
					ctr.h8.keys[k] = uint64(vs[int(i)+k])
				}
			}
			ctr.hashs[0] = 0
			copy(ctr.inserts[:n], ctr.zinserts[:n])
			if err := v.h8.ht.InsertBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values); err != nil {
				batch.Clean(bat, proc.Mp)
				return err
			}
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = v.rows
					v.rows++
					v.sels = append(v.sels, make([]int64, 0, 8))
				}
				ai := int64(*ctr.values[k])
				v.sels[ai] = append(v.sels[ai], i+int64(k))
			}
		}
	case types.T_uint8:
		if v.isB && bat.Ht != nil {
			v.h8.ht = bat.Ht.(*hashtable.Int64HashMap)
			return nil
		}
		v.h8.ht = &hashtable.Int64HashMap{}
		v.h8.ht.Init()
		vs := vec.Col.([]uint8)
		count := int64(len(bat.Zs))
		for i := int64(0); i < count; i += UnitLimit {
			n := int(count - i)
			if n > UnitLimit {
				n = UnitLimit
			}
			{
				for k := 0; k < n; k++ {
					ctr.h8.keys[k] = uint64(vs[int(i)+k])
				}
			}
			ctr.hashs[0] = 0
			copy(ctr.inserts[:n], ctr.zinserts[:n])
			if err := v.h8.ht.InsertBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values); err != nil {
				batch.Clean(bat, proc.Mp)
				return err
			}
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = v.rows
					v.rows++
					v.sels = append(v.sels, make([]int64, 0, 8))
				}
				ai := int64(*ctr.values[k])
				v.sels[ai] = append(v.sels[ai], i+int64(k))
			}
		}
	case types.T_uint16:
		if v.isB && bat.Ht != nil {
			v.h8.ht = bat.Ht.(*hashtable.Int64HashMap)
			return nil
		}
		v.h8.ht = &hashtable.Int64HashMap{}
		v.h8.ht.Init()
		vs := vec.Col.([]uint16)
		count := int64(len(bat.Zs))
		for i := int64(0); i < count; i += UnitLimit {
			n := int(count - i)
			if n > UnitLimit {
				n = UnitLimit
			}
			{
				for k := 0; k < n; k++ {
					ctr.h8.keys[k] = uint64(vs[int(i)+k])
				}
			}
			ctr.hashs[0] = 0
			copy(ctr.inserts[:n], ctr.zinserts[:n])
			if err := v.h8.ht.InsertBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values); err != nil {
				batch.Clean(bat, proc.Mp)
				return err
			}
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = v.rows
					v.rows++
					v.sels = append(v.sels, make([]int64, 0, 8))
				}
				ai := int64(*ctr.values[k])
				v.sels[ai] = append(v.sels[ai], i+int64(k))
			}
		}
	case types.T_uint32:
		if v.isB && bat.Ht != nil {
			v.h8.ht = bat.Ht.(*hashtable.Int64HashMap)
			return nil
		}
		v.h8.ht = &hashtable.Int64HashMap{}
		v.h8.ht.Init()
		vs := vec.Col.([]uint32)
		count := int64(len(bat.Zs))
		for i := int64(0); i < count; i += UnitLimit {
			n := int(count - i)
			if n > UnitLimit {
				n = UnitLimit
			}
			{
				for k := 0; k < n; k++ {
					ctr.h8.keys[k] = uint64(vs[int(i)+k])
				}
			}
			ctr.hashs[0] = 0
			copy(ctr.inserts[:n], ctr.zinserts[:n])
			if err := v.h8.ht.InsertBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values); err != nil {
				batch.Clean(bat, proc.Mp)
				return err
			}
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = v.rows
					v.rows++
					v.sels = append(v.sels, make([]int64, 0, 8))
				}
				ai := int64(*ctr.values[k])
				v.sels[ai] = append(v.sels[ai], i+int64(k))
			}
		}
	case types.T_uint64:
		if v.isB && bat.Ht != nil {
			v.h8.ht = bat.Ht.(*hashtable.Int64HashMap)
			return nil
		}
		v.h8.ht = &hashtable.Int64HashMap{}
		v.h8.ht.Init()
		vs := vec.Col.([]uint64)
		count := int64(len(bat.Zs))
		for i := int64(0); i < count; i += UnitLimit {
			n := int(count - i)
			if n > UnitLimit {
				n = UnitLimit
			}
			{
				for k := 0; k < n; k++ {
					ctr.h8.keys[k] = uint64(vs[int(i)+k])
				}
			}
			ctr.hashs[0] = 0
			copy(ctr.inserts[:n], ctr.zinserts[:n])
			if err := v.h8.ht.InsertBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values); err != nil {
				batch.Clean(bat, proc.Mp)
				return err
			}
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = v.rows
					v.rows++
					v.sels = append(v.sels, make([]int64, 0, 8))
				}
				ai := int64(*ctr.values[k])
				v.sels[ai] = append(v.sels[ai], i+int64(k))
			}
		}
	case types.T_float32:
		if v.isB && bat.Ht != nil {
			v.h8.ht = bat.Ht.(*hashtable.Int64HashMap)
			return nil
		}
		v.h8.ht = &hashtable.Int64HashMap{}
		v.h8.ht.Init()
		vs := vec.Col.([]float32)
		count := int64(len(bat.Zs))
		for i := int64(0); i < count; i += UnitLimit {
			n := int(count - i)
			if n > UnitLimit {
				n = UnitLimit
			}
			{
				for k := 0; k < n; k++ {
					ctr.h8.keys[k] = uint64(vs[int(i)+k])
				}
			}
			ctr.hashs[0] = 0
			copy(ctr.inserts[:n], ctr.zinserts[:n])
			if err := v.h8.ht.InsertBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values); err != nil {
				batch.Clean(bat, proc.Mp)
				return err
			}
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = v.rows
					v.rows++
					v.sels = append(v.sels, make([]int64, 0, 8))
				}
				ai := int64(*ctr.values[k])
				v.sels[ai] = append(v.sels[ai], i+int64(k))
			}
		}
	case types.T_float64:
		if v.isB && bat.Ht != nil {
			v.h8.ht = bat.Ht.(*hashtable.Int64HashMap)
			return nil
		}
		v.h8.ht = &hashtable.Int64HashMap{}
		v.h8.ht.Init()
		vs := vec.Col.([]float64)
		count := int64(len(bat.Zs))
		for i := int64(0); i < count; i += UnitLimit {
			n := int(count - i)
			if n > UnitLimit {
				n = UnitLimit
			}
			{
				for k := 0; k < n; k++ {
					ctr.h8.keys[k] = uint64(vs[int(i)+k])
				}
			}
			ctr.hashs[0] = 0
			copy(ctr.inserts[:n], ctr.zinserts[:n])
			if err := v.h8.ht.InsertBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values); err != nil {
				batch.Clean(bat, proc.Mp)
				return err
			}
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = v.rows
					v.rows++
					v.sels = append(v.sels, make([]int64, 0, 8))
				}
				ai := int64(*ctr.values[k])
				v.sels[ai] = append(v.sels[ai], i+int64(k))
			}
		}
	case types.T_char, types.T_varchar:
		v.hstr.ht = &hashtable.StringHashMap{}
		v.hstr.ht.Init()
		vs := vec.Col.(*types.Bytes)
		count := int64(len(bat.Zs))
		for i := int64(0); i < count; i++ {
			key := vs.Get(i)
			ok, vp, err := v.hstr.ht.Insert(0, hashtable.StringRef{Ptr: &key[0], Len: len(key)})
			if err != nil {
				batch.Clean(bat, proc.Mp)
				return err
			}
			if ok {
				*vp = v.rows
				v.rows++
				v.sels = append(v.sels, make([]int64, 0, 8))
			}
			ai := int64(*vp)
			v.sels[ai] = append(v.sels[ai], i)
		}
	}
	return nil
}
*/

func (ctr *Container) constructBatch(rn string, varsMap map[string]int, bat *batch.Batch) {
	ctr.is = ctr.is[:0]
	ctr.bat = new(batch.Batch)
	if len(ctr.is) == 0 {
		ctr.is = make([]int, len(bat.Attrs))
	} else {
		ctr.is = ctr.is[:len(bat.Attrs)]
	}
	{
		for i, attr := range bat.Attrs {
			if _, ok := ctr.fvarsMap[attr]; !ok {
				continue
			}
			if varsMap[attr] > 1 {
				ctr.bat.Attrs = append(ctr.bat.Attrs, rn+"."+attr)
			} else {
				ctr.bat.Attrs = append(ctr.bat.Attrs, attr)
			}
			ctr.is[i] = len(ctr.bat.Vecs)
			ctr.bat.Vecs = append(ctr.bat.Vecs, vector.New(bat.Vecs[i].Typ))
		}
	}
	for _, v := range ctr.views {
		for i, attr := range v.bat.Attrs {
			if varsMap[attr] > 1 {
				ctr.bat.Attrs = append(ctr.bat.Attrs, v.rn+"."+attr)
			} else {
				ctr.bat.Attrs = append(ctr.bat.Attrs, attr)
			}
			v.vis = append(v.vis, len(ctr.bat.Vecs))
			ctr.bat.Vecs = append(ctr.bat.Vecs, vector.New(v.bat.Vecs[i].Typ))
		}
	}
	for i, r := range bat.Rs {
		ctr.bat.Rs = append(ctr.bat.Rs, r.Dup())
		ctr.bat.As = append(ctr.bat.As, bat.As[i])
		ctr.bat.Refs = append(ctr.bat.Refs, bat.Refs[i])
	}
	for _, v := range ctr.views {
		for i, r := range v.bat.Rs {
			v.ris = append(v.ris, len(ctr.bat.Rs))
			ctr.bat.Rs = append(ctr.bat.Rs, r.Dup())
			ctr.bat.As = append(ctr.bat.As, v.bat.As[i])
			ctr.bat.Refs = append(ctr.bat.Refs, v.bat.Refs[i])
		}
	}
}

func (ctr *Container) constructVars(arg *Argument) {
	ctr.vars = make([]string, 0, 8)
	ctr.fvarsMap = make(map[string]uint8)
	for _, fv := range arg.Arg.FreeVars {
		ctr.fvarsMap[fv] = 0
	}
	for _, rvar := range arg.Rvars {
		if arg.VarsMap[rvar] > 1 {
			ctr.vars = append(ctr.vars, arg.R+"."+rvar)
		} else {
			ctr.vars = append(ctr.vars, rvar)
		}
	}
	for i, s := range arg.Ss {
		if arg.VarsMap[arg.Svars[i]] > 1 {
			ctr.vars = append(ctr.vars, s+"."+arg.Svars[i])
		} else {
			ctr.vars = append(ctr.vars, arg.Svars[i])
		}
	}
	ctr.varsMap = make(map[string]uint8)
	for _, v := range ctr.vars {
		ctr.varsMap[v] = 0
	}
}
