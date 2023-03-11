// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// todo(broccoli): revise this, maybe rewrite this? at least clean up the logic
func Concat_ws(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtyp := types.T_varchar.ToType()
	// If any binary type exists return binary type.
	for _, v := range ivecs {
		if v.GetType().Oid == types.T_binary || v.GetType().Oid == types.T_varbinary || v.GetType().Oid == types.T_blob {
			rtyp = types.T_blob.ToType()
			break
		}
	}
	if ivecs[0].IsConst() {
		if ivecs[0].IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		vectorIsConst := make([]bool, 0)
		inputCleaned := make([]*vector.Vector, 0) // no NULL const vectors
		AllConst := true
		for i := 1; i < len(ivecs); i++ {
			if ivecs[i].IsConstNull() {
				continue
			}
			if ivecs[i].IsConst() {
				vectorIsConst = append(vectorIsConst, true)
			} else {
				vectorIsConst = append(vectorIsConst, false)
				AllConst = false
			}
			inputCleaned = append(inputCleaned, ivecs[i])
		}
		separator := ivecs[0].GetStringAt(0)
		if AllConst {
			return concatWsWithConstSeparatorAllConst(rtyp, inputCleaned, separator, proc.Mp())
		}
		return concatWsWithConstSeparator(rtyp, inputCleaned, separator, vectorIsConst, proc)
	} else {
		vectorIsConst := make([]bool, 0)
		inputCleaned := make([]*vector.Vector, 0) // no NULL const vectors
		AllConst := true
		for i := 1; i < len(ivecs); i++ {
			if ivecs[i].IsConstNull() {
				continue
			}
			if ivecs[i].IsConst() {
				vectorIsConst = append(vectorIsConst, true)
			} else {
				vectorIsConst = append(vectorIsConst, false)
				AllConst = false
			}
			inputCleaned = append(inputCleaned, ivecs[i])
		}
		separator := ivecs[0]
		if AllConst {
			return concatWsAllConst(rtyp, inputCleaned, separator, proc)
		} else {
			return concatWs(rtyp, inputCleaned, separator, vectorIsConst, proc)
		}
	}
}

func concatWs(rtyp types.Type, inputCleaned []*vector.Vector, separator *vector.Vector, vectorIsConst []bool, proc *process.Process) (*vector.Vector, error) {
	separators := vector.MustStrCol(separator)
	length := len(separators)
	resultNsp := new(nulls.Nulls)
	rvals := make([]string, length)
	for i := 0; i < length; i++ {
		allNull := true
		for j := range inputCleaned {
			if vectorIsConst[j] {
				allNull = false
				if len(rvals[i]) > 0 {
					rvals[i] += separators[i]
				}
				rvals[i] += inputCleaned[j].GetStringAt(0)
			} else {
				if nulls.Contains(inputCleaned[j].GetNulls(), uint64(i)) {
					continue
				}
				allNull = false
				if len(rvals[i]) > 0 {
					rvals[i] += separators[i]
				}
				rvals[i] += inputCleaned[j].GetStringAt(i)
			}
		}
		if allNull {
			nulls.Add(resultNsp, uint64(i))
		}
	}
	rvec := vector.NewVec(rtyp)
	vector.AppendStringList(rvec, rvals, nil, proc.Mp())
	return rvec, nil
}

func concatWsAllConst(rtyp types.Type, inputCleaned []*vector.Vector, separator *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	separators := vector.MustStrCol(separator)
	length := len(separators)
	resultNsp := new(nulls.Nulls)
	rvals := make([]string, length)
	for i := 0; i < length; i++ {
		allNull := true
		for j := range inputCleaned {
			if len(rvals[i]) > 0 {
				rvals[i] += separators[i]
			}
			allNull = false
			rvals[i] += inputCleaned[j].GetStringAt(0)
		}
		if allNull { // this check is redundant
			nulls.Add(resultNsp, uint64(i))
		}
	}
	rvec := vector.NewVec(rtyp)
	vector.AppendStringList(rvec, rvals, nil, proc.Mp())
	return rvec, nil
}

// the inputs are guaranteed to be scalar non-NULL
func concatWsWithConstSeparatorAllConst(rtyp types.Type, inputCleaned []*vector.Vector, separator string, mp *mpool.MPool) (*vector.Vector, error) {
	res := ""
	for i := range inputCleaned {
		res = res + inputCleaned[i].GetStringAt(0)
		if i+1 == len(inputCleaned) {
			break
		} else {
			res += separator
		}
	}
	vec := vector.NewVec(rtyp)
	vector.AppendBytes(vec, []byte(res), res == "", mp)
	return vec, nil
}

// inputCleaned does not have NULL const
func concatWsWithConstSeparator(rtyp types.Type, inputCleaned []*vector.Vector, separator string, vectorIsConst []bool, proc *process.Process) (*vector.Vector, error) {
	length := 0
	for i := range inputCleaned {
		inputI := vector.MustBytesCol(inputCleaned[i])
		lengthI := len(inputI)
		if lengthI == 0 {
			length = 0 // this means that one column that needs to be concatenated is empty
			break
		}
		if lengthI > length {
			length = lengthI
		}
	}

	resultNsp := new(nulls.Nulls)
	rvals := make([]string, length)

	for i := 0; i < length; i++ {
		allNull := true
		for j := range inputCleaned {
			if vectorIsConst[j] {
				if j > 0 && !nulls.Contains(inputCleaned[j-1].GetNulls(), uint64(i)) {
					rvals[i] += separator
				}
				allNull = false
				rvals[i] += inputCleaned[j].GetStringAt(0)
			} else {
				if nulls.Contains(inputCleaned[j].GetNulls(), uint64(i)) {
					continue
				}
				if j > 0 && !nulls.Contains(inputCleaned[j-1].GetNulls(), uint64(i)) {
					rvals[i] += separator
				}
				allNull = false
				rvals[i] += inputCleaned[j].GetStringAt(i)
			}
		}
		if allNull {
			nulls.Add(resultNsp, uint64(i))
		}
	}
	rvec := vector.NewVec(rtyp)
	vector.AppendStringList(rvec, rvals, nil, proc.Mp())
	return rvec, nil
}
