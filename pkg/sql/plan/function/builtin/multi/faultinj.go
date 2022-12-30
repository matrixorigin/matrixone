// Copyright 2022 Matrix Origin
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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func EnableFaultInjection(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	fault.Enable()
	return proc.AllocBoolScalarVector(true), nil
}

func DisableFaultInjection(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	fault.Disable()
	return proc.AllocBoolScalarVector(true), nil
}

func AddFaultPoint(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	for i := 0; i < 5; i++ {
		if vecs[i].IsConstNull() || !vecs[i].IsConst() {
			return nil, moerr.NewInvalidArgNoCtx("AddFaultPoint", "not scalar")
		}
	}

	name := vecs[0].String()
	freq := vecs[1].String()
	action := vecs[2].String()
	iarg := vector.MustTCols[int64](vecs[3])[0]
	sarg := vecs[4].String()

	if err := fault.AddFaultPoint(proc.Ctx, name, freq, action, iarg, sarg); err != nil {
		return nil, err
	}

	return proc.AllocBoolScalarVector(true), nil
}

func RemoveFaultPoint(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if vecs[0].IsConstNull() || !vecs[0].IsConst() {
		return nil, moerr.NewInvalidArgNoCtx("RemoveFaultPoint", "not scalar")
	}

	name := vecs[0].String()
	if err := fault.RemoveFaultPoint(proc.Ctx, name); err != nil {
		return nil, err
	}
	return proc.AllocBoolScalarVector(true), nil
}

func TriggerFaultPoint(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if vecs[0].IsConstNull() || !vecs[0].IsConst() {
		return nil, moerr.NewInvalidArgNoCtx("TriggerFaultPoint", "not scalar")
	}

	name := vecs[0].String()
	iv, _, ok := fault.TriggerFault(name)
	if !ok {
		return proc.AllocScalarNullVector(types.T_int64.ToType()), nil
	}
	return proc.AllocInt64ScalarVector(iv), nil
}
