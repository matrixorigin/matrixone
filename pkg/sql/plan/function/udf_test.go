// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestPythonTypeContractPreservesDeclaredShape(t *testing.T) {
	argument := types.New(types.T_decimal64, 18, 6)
	returnType := types.New(types.T_varchar, 64, 0)
	argumentDescriptor, err := NewPythonTypeDescriptor(argument)
	require.NoError(t, err)
	returnDescriptor, err := NewPythonTypeDescriptor(returnType)
	require.NoError(t, err)
	body, err := json.Marshal(PythonRoutineBody{
		ArgTypes:   []PythonTypeDescriptor{argumentDescriptor},
		ReturnType: &returnDescriptor,
	})
	require.NoError(t, err)

	routine := &Udf{Language: "python", Body: string(body), ArgsType: []types.Type{types.T_decimal64.ToType()}}
	require.NoError(t, routine.LoadPythonTypeContract())
	require.Equal(t, argument, routine.GetArgsType()[0])
	require.Equal(t, returnType, routine.GetRetType())
	require.Equal(t, int32(6), routine.GetArgsPlanType()[0].Scale)
	require.Equal(t, int32(64), routine.GetRetPlanType().Width)
}

func TestPythonTypeContractKeepsLegacyFallback(t *testing.T) {
	routine := &Udf{
		Language: "python",
		Body:     `{"handler":"legacy","source":"def legacy(ctx, x): return x"}`,
		ArgsType: []types.Type{types.T_int32.ToType()},
		RetType:  "int",
	}
	require.NoError(t, routine.LoadPythonTypeContract())
	require.Equal(t, types.T_int32.ToType(), routine.GetArgsType()[0])
	require.Equal(t, types.T_int32.ToType(), routine.GetRetType())
}
