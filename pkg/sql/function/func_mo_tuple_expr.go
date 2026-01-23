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
	p := vector.GenerateFunctionStrParameter(params[0])
	rs := vector.MustFunctionResult[types.Varlena](result)

	// special case: ignore all rows
	if selectList.IgnoreAllRow() {
		rs.AddNullRange(0, uint64(length))
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v, null := p.GetStrValue(i)
		if null {
			rs.AppendMustNull()
			continue
		}

		// Decode the tuple bytes
		tuple, _, schema, err := types.DecodeTuple(v)
		if err != nil {
			rs.AppendMustNull()
			continue
		}

		// Generate scales array from schema
		// Note: Tuple encoding doesn't contain scale information for decimal types,
		// so we use scale=0 which displays decimals as their internal scaled integer values.
		// For example, decimal(10,2) value 12.50 will be displayed as 1250.
		scales := make([]int32, len(schema))
		for idx := range scales {
			scales[idx] = 0 // Default scale (displays decimals as scaled integers)
		}

		// Convert tuple to SQL strings
		sqlStrings := tuple.SQLStrings(scales)

		// Format the output
		var output string
		if len(sqlStrings) == 1 {
			output = sqlStrings[0]
		} else {
			output = "(" + strings.Join(sqlStrings, ", ") + ")"
		}

		// Append to result
		if err := rs.AppendMustBytesValue([]byte(output)); err != nil {
			return err
		}
	}

	return nil
}
