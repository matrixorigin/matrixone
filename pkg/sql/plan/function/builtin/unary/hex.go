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
	"encoding/hex"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func HexString(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_varchar.ToType()
	ivals := vector.MustStrCol(inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		var rvals [1]string
		HexEncodeString(ivals, rvals[:])
		return vector.NewConstBytes(rtyp, []byte(rvals[0]), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvals := make([]string, len(ivals))
		HexEncodeString(ivals, rvals)
		vec := vector.NewVec(rtyp)
		vector.AppendStringList(vec, rvals, nil, proc.Mp())
		return vec, nil
	}
}

func HexInt64(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_varchar.ToType()
	ivals := vector.MustFixedCol[int64](inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		var rvals [1]string
		HexEncodeInt64(ivals, rvals[:])
		return vector.NewConstBytes(rtyp, []byte(rvals[0]), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvals := make([]string, len(ivals))
		HexEncodeInt64(ivals, rvals)
		vec := vector.NewVec(rtyp)
		vector.AppendStringList(vec, rvals, nil, proc.Mp())
		return vec, nil
	}
}

func HexEncodeString(xs []string, rs []string) []string {
	for i, str := range xs {
		dst := hex.EncodeToString([]byte(str))
		rs[i] = dst
	}
	return rs
}

func HexEncodeInt64(xs []int64, rs []string) []string {
	for i, str := range xs {
		rs[i] = fmt.Sprintf("%X", str)
	}
	return rs
}
