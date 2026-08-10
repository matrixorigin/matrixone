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

package function

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// MoTupleExpr decodes a tuple bytes representation and returns a human-readable string.
// It uses DecodeTuple to decode the bytes and SQLStrings to format the output.
func MoTupleExpr(params []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	if length == 0 {
		return nil
	}
	p := vector.GenerateFunctionStrParameter(params[0])

	// special case: ignore all rows
	if selectList.IgnoreAllRow() {
		rs.SetNullResult(uint64(length))
		return nil
	}
	if params[0].IsConst() {
		v, null := p.GetStrValue(0)
		if null {
			rs.SetNullResult(uint64(length))
			return nil
		}
		output, ok := formatMoTupleExpr(v)
		if !ok {
			rs.SetNullResult(uint64(length))
			return nil
		}
		return appendRepeatedBytesResultWithSelection(rs, output, length, selectList)
	}

	for i := uint64(0); i < uint64(length); i++ {
		if selectList.Contains(i) {
			if err := rs.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}
		v, null := p.GetStrValue(i)
		if null {
			if err := rs.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}

		output, ok := formatMoTupleExpr(v)
		if !ok {
			if err := rs.AppendMustNullForBytesResult(); err != nil {
				return err
			}
			continue
		}
		if err := rs.AppendMustBytesValue(output); err != nil {
			return err
		}
	}

	return nil
}

func formatMoTupleExpr(value []byte) ([]byte, bool) {
	tuple, _, schema, err := types.DecodeTuple(value)
	if err != nil {
		return nil, false
	}

	// Tuple encoding doesn't contain scale information for decimal types, so
	// scale=0 displays decimals as their internal scaled integer values.
	scales := make([]int32, len(schema))
	sqlStrings := tuple.SQLStrings(scales)
	if len(sqlStrings) == 1 {
		return []byte(sqlStrings[0]), true
	}
	return []byte("(" + strings.Join(sqlStrings, ", ") + ")"), true
}
