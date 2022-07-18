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

package product

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString(" ⨯ ")
}

func Prepare(proc *process.Process, arg interface{}) error {
	ap := arg.(*Argument)
	ap.ctr = new(Container)
	return nil
}

func Call(_ int, proc *process.Process, arg interface{}) (bool, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(ap, proc); err != nil {
				ctr.state = End
				return true, err
			}
			ctr.state = Probe
		case Probe:
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				ctr.state = End
				if ctr.bat != nil {
					ctr.bat.Clean(proc.Mp)
				}
				continue
			}
			if len(bat.Zs) == 0 {
				continue
			}
			if ctr.bat == nil {
				bat.Clean(proc.Mp)
				continue
			}
			if err := ctr.probe(bat, ap, proc); err != nil {
				ctr.state = End
				bat.Clean(proc.Mp)
				proc.Reg.InputBatch = nil
				return true, err
			}
			return false, nil
		default:
			proc.Reg.InputBatch = nil
			return true, nil
		}
	}
}

func (ctr *Container) build(ap *Argument, proc *process.Process) error {
	var err error

	for {
		bat := <-proc.Reg.MergeReceivers[1].Ch
		if bat == nil {
			break
		}
		if len(bat.Zs) == 0 {
			continue
		}
		if ctr.bat == nil {
			ctr.bat = batch.NewWithSize(len(bat.Vecs))
			for i, vec := range bat.Vecs {
				ctr.bat.Vecs[i] = vector.New(vec.Typ)
			}
		}
		if ctr.bat, err = ctr.bat.Append(proc.Mp, bat); err != nil {
			bat.Clean(proc.Mp)
			ctr.bat.Clean(proc.Mp)
			return err
		}
		bat.Clean(proc.Mp)
	}
	return nil
}

func (ctr *Container) probe(bat *batch.Batch, ap *Argument, proc *process.Process) error {
	defer bat.Clean(proc.Mp)
	rbat := batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			rbat.Vecs[i] = vector.New(bat.Vecs[rp.Pos].Typ)
		} else {
			rbat.Vecs[i] = vector.New(ctr.bat.Vecs[rp.Pos].Typ)
		}
	}
	count := len(bat.Zs)
	for i := 0; i < count; i++ {
		for j := 0; j < len(ctr.bat.Zs); j++ {
			for k, rp := range ap.Result {
				if rp.Rel == 0 {
					if err := vector.UnionOne(rbat.Vecs[k], bat.Vecs[rp.Pos], int64(i), proc.Mp); err != nil {
						rbat.Clean(proc.Mp)
						return err
					}
				} else {
					if err := vector.UnionOne(rbat.Vecs[k], ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp); err != nil {
						rbat.Clean(proc.Mp)
						return err
					}
				}
			}
			rbat.Zs = append(rbat.Zs, ctr.bat.Zs[j])
		}
	}
	rbat.ExpandNulls()
	proc.Reg.InputBatch = rbat
	return nil
}
