// Copyright 2021 Matrix Origin
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

package util

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func getVal(val any) string {
	switch v := val.(type) {
	case float32:
		return fmt.Sprintf("%e", val)
	case float64:
		return fmt.Sprintf("%e", val)
	case []byte:
		return string(v)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", val)
	}
}

func GenVectorByVarValue(proc *process.Process, typ types.Type, val any) (*vector.Vector, error) {
	if val == nil {
		vec := vector.NewConstNull(typ, 1, proc.Mp()) // todo use pool
		return vec, nil
	} else {
		strVal := getVal(val)
		vec := vector.NewConstBytes(typ, []byte(strVal), 1, proc.Mp()) // todo use pool
		return vec, nil
	}
}

func AppendAnyToStringVector(proc *process.Process, val any, vec *vector.Vector) error {
	if val == nil {
		return vector.AppendBytes(vec, []byte{}, true, proc.Mp())
	} else {
		strVal := getVal(val)
		return vector.AppendBytes(vec, []byte(strVal), false, proc.Mp())
	}
}

func SetAnyToStringVector(proc *process.Process, val any, vec *vector.Vector, idx int) error {
	if val == nil {
		vec.GetNulls().Set(uint64(idx))
		return nil
	} else {
		strVal := getVal(val)
		return vector.SetBytesAt(vec, idx, []byte(strVal), proc.Mp())
	}
}
