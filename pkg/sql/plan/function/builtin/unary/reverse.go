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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/reverse"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Reverse(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_varchar.ToType()
	if ivecs[0].GetType().Oid == types.T_blob {
		rtyp = types.T_blob.ToType()
	}
	ivals := vector.MustStrCol(inputVector)
	if inputVector.IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
	} else if inputVector.IsConst() {
		var rvals [1]string
		reverse.Reverse(ivals, rvals[:])
		return vector.NewConstBytes(rtyp, []byte(rvals[0]), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvals := make([]string, len(ivals))
		reverse.Reverse(ivals, rvals)
		vec := vector.NewVec(rtyp)
		vector.AppendStringList(vec, rvals, nil, proc.Mp())
		nulls.Set(vec.GetNulls(), inputVector.GetNulls())
		return vec, nil
	}
}
