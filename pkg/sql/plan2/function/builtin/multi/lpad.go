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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const UINT16_MAX = ^uint16(0)

func Lpad(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if vecs[0].IsScalarNull() || vecs[1].IsScalarNull() || vecs[2].IsScalarNull() {
		return proc.AllocScalarNullVector(vecs[0].Typ), nil
	}
	vs := vector.MustBytesCols(vecs[0]) //Get the first arg
	if !vecs[1].IsScalar() && vecs[1].Typ.Oid != types.T_int64 {
		return nil, errors.New("The second argument of the " +
			"lpad function must be an int64 constant")
	}
	if !vecs[2].IsScalar() && vecs[2].Typ.Oid != types.T_varchar {
		return nil, errors.New("The third argument" +
			" of the lpad function must be an string constant")
	}

	if vecs[0].IsScalar() && vecs[1].IsScalar() && vecs[2].IsScalar() {

		return nil, nil
	}

	//characters length not bytes length
	lengths := vector.MustTCols[int64](vecs[1])

	//pad string
	padds := vector.MustBytesCols(vecs[2])

	//TODO : consider constant input

	results := &types.Bytes{}
	resultNUll := new(nulls.Nulls)

	//evaluate bytes space for every long
	rowCount := vector.Length(vecs[0])
	for i := 0; i < rowCount; i++ {
		//1.count characters in source and pad
		var source []byte
		if vecs[0].IsScalar() {
			source = vs.Get(int64(0))
		} else {
			source = vs.Get(int64(i))
		}
		sourceLen := len(source)
		sourceRune := []rune(string(source))
		sourceCharCnt := int64(len(sourceRune))
		pad := padds.Get(int64(i))
		padLen := len(pad)
		padRune := []rune(string(pad))
		padCharCnt := int64(len(padRune))
		targetLength := lengths[i]
		if targetLength < 0 {
			//NULL
			nulls.Add(resultNUll, uint64(i))
			results.AppendOnce([]byte{})
		} else if targetLength == 0 {
			//empty string
			results.AppendOnce([]byte{})
		} else if targetLength < sourceCharCnt {
			//shorten source string
			sourcePart := sourceRune[:targetLength]
			results.AppendOnce([]byte(string(sourcePart)))
		} else if targetLength >= sourceCharCnt {
			//calculate the space count
			r := targetLength - sourceCharCnt
			//complete pad count
			p := r / padCharCnt
			//partial pad count
			m := r % padCharCnt
			padSpaceCount := p*int64(padLen) + int64(len(string(padRune[:m]))) + int64(sourceLen)
			padSpace := make([]byte, padSpaceCount)
			// fill p * padStr
			for j := int64(0); j < p; j++ {
				copy(padSpace[j*int64(padLen):], pad)
			}
			// fill partial padStr
			copy(padSpace[p*int64(padLen):], []byte(string(padRune[:m])))
			// fill source
			copy(padSpace[padSpaceCount-int64(sourceLen):], source)
			results.AppendOnce(padSpace)
		}
	}

	resultVec, err := proc.AllocVector(types.T_varchar.ToType(), 1)
	if err != nil {
		return nil, err
	}

	resultVec.Nsp = resultNUll
	vector.SetCol(resultVec, results)
	return resultVec, nil
}
