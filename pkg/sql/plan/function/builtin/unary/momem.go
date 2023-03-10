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

package unary

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func MoMemUsage(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if len(ivecs) != 1 {
		return nil, moerr.NewInvalidInput(proc.Ctx, "no mpool name")
	}
	inputVector := ivecs[0]
	rtyp := types.T_varchar.ToType()
	ivals := vector.MustStrCol(inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}

		memUsage := mpool.ReportMemUsage(ivals[0])
		return vector.NewConstBytes(rtyp, []byte(memUsage), ivecs[0].Length(), proc.Mp()), nil
	} else {
		panic(moerr.NewInvalidInput(proc.Ctx, "mo mem usage can only take scalar input"))
	}
}

func moMemUsageCmd(cmd string, ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if len(ivecs) != 1 {
		return nil, moerr.NewInvalidInput(proc.Ctx, "no mpool name")
	}
	inputVector := ivecs[0]
	rtyp := types.T_varchar.ToType()
	ivals := vector.MustStrCol(inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}

		ok := mpool.MPoolControl(ivals[0], cmd)
		return vector.NewConstBytes(rtyp, []byte(ok), ivecs[0].Length(), proc.Mp()), nil
	} else {
		panic(moerr.NewInvalidInput(proc.Ctx, "mo mem usage can only take scalar input"))
	}
}

func MoEnableMemUsageDetail(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return moMemUsageCmd("enable_detail", vectors, proc)
}

func MoDisableMemUsageDetail(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return moMemUsageCmd("disable_detail", vectors, proc)
}
