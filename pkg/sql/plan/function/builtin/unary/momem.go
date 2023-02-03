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

func MoMemUsage(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if len(vectors) != 1 {
		return nil, moerr.NewInvalidInput(proc.Ctx, "no mpool name")
	}
	inputVector := vectors[0]
	resultType := types.T_varchar.ToType()
	inputValues := vector.MustStrCols(inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}

		memUsage := mpool.ReportMemUsage(inputValues[0])
		return vector.NewConstString(resultType, inputVector.Length(), memUsage, proc.Mp()), nil
	} else {
		panic(moerr.NewInvalidInput(proc.Ctx, "mo mem usage can only take scalar input"))
	}
}

func moMemUsageCmd(cmd string, vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if len(vectors) != 1 {
		return nil, moerr.NewInvalidInput(proc.Ctx, "no mpool name")
	}
	inputVector := vectors[0]
	resultType := types.T_varchar.ToType()
	inputValues := vector.MustStrCols(inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}

		ok := mpool.MPoolControl(inputValues[0], cmd)
		return vector.NewConstString(resultType, inputVector.Length(), ok, proc.Mp()), nil
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
