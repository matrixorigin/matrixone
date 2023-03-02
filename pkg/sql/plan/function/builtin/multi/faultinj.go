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

func EnableFaultInjection(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	fault.Enable()
	return vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp()), nil
}

func DisableFaultInjection(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	fault.Disable()
	return vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp()), nil
}

func AddFaultPoint(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	for i := 0; i < 5; i++ {
		if ivecs[i].IsConstNull() || !ivecs[i].IsConst() {
			return nil, moerr.NewInvalidArg(proc.Ctx, "AddFaultPoint", "not scalar")
		}
	}

	name := ivecs[0].GetStringAt(0)
	freq := ivecs[1].GetStringAt(0)
	action := ivecs[2].GetStringAt(0)
	iarg := vector.MustFixedCol[int64](ivecs[3])[0]
	sarg := ivecs[4].GetStringAt(0)

	if err := fault.AddFaultPoint(proc.Ctx, name, freq, action, iarg, sarg); err != nil {
		return nil, err
	}

	return vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp()), nil
}

func RemoveFaultPoint(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if ivecs[0].IsConstNull() || !ivecs[0].IsConst() {
		return nil, moerr.NewInvalidArg(proc.Ctx, "RemoveFaultPoint", "not scalar")
	}

	name := ivecs[0].GetStringAt(0)
	if err := fault.RemoveFaultPoint(proc.Ctx, name); err != nil {
		return nil, err
	}
	return vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp()), nil
}

func TriggerFaultPoint(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if ivecs[0].IsConstNull() || !ivecs[0].IsConst() {
		return nil, moerr.NewInvalidArg(proc.Ctx, "TriggerFaultPoint", "not scalar")
	}

	name := ivecs[0].GetStringAt(0)
	iv, _, ok := fault.TriggerFault(name)
	if !ok {
		return vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp()), nil
	}
	return vector.NewConstFixed(types.T_int64.ToType(), iv, 1, proc.Mp()), nil
}
