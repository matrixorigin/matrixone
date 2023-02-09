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

package ctl

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/multi"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const ZeroDate = "0001-01-01"
const formatMask = "%Y/%m/%d"
const regexpMask = `\d{1,4}/\d{1,2}/\d{1,2}`

// MOLogDate parse 'YYYY/MM/DD' date from input string.
// return '0001-01-01' if input string not container 'YYYY/MM/DD' substr, until DateParse Function support return NULL for invalid date string.
func MOLogDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {

	regexpVec := vector.NewWithStrings(types.T_varchar.ToType(), []string{regexpMask}, nil, proc.Mp())
	regexpInput := append(vectors, regexpVec)
	parsedInput, err := multi.RegularSubstr(regexpInput, proc)
	if err != nil {
		return nil, err
	}

	formatVec := vector.NewConstString(types.T_char.ToType(), len(formatMask), formatMask, proc.Mp())
	strToDateInput := []*vector.Vector{parsedInput, formatVec}
	resultVector, err := binary.StrToDate(strToDateInput, proc)
	if err != nil {
		return nil, err
	}
	return resultVector, err
}
