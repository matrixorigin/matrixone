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

package function2

import (
	"encoding/hex"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func builtInHexString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {

	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res := HexEncodeString(v)
			if err := rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInHexInt64(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			res := HexEncodeInt64(v)
			if err := rs.AppendBytes(function2Util.QuickStrToBytes(res), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func HexEncodeString(xs []byte) string {
	return hex.EncodeToString(xs)
}

func HexEncodeInt64(xs int64) string {
	return fmt.Sprintf("%X", xs)
}
