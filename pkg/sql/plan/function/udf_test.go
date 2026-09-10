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
	"context"
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/udf"
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
		Handler:        "identity",
		Source:         "def identity(ctx, x): return x",
		Mode:           "SCALAR",
		NullPolicy:     udf.NullCallHandler,
		ABIContract:    udf.PythonABIContract,
		AdapterVersion: udf.PythonAdapterVersion,
		SDKVersion:     udf.PythonSDKVersion,
		ArgTypes:       []PythonTypeDescriptor{argumentDescriptor},
		ReturnType:     &returnDescriptor,
	})
	require.NoError(t, err)

	routine := &Udf{Language: "python", Body: string(body), ArgsType: []types.Type{types.T_decimal64.ToType()}}
	require.NoError(t, routine.LoadPythonTypeContract())
	routine.Args = []*Arg{{Type: "decimal"}}
	require.NoError(t, routine.ValidatePythonTypeContract())
	require.Equal(t, argument, routine.GetArgsType()[0])
	require.Equal(t, returnType, routine.GetRetType())
	require.Equal(t, int32(6), routine.GetArgsPlanType()[0].Scale)
	require.Equal(t, int32(64), routine.GetRetPlanType().Width)
}

func TestPythonTypeContractRejectsMissingReturnDescriptor(t *testing.T) {
	routine := &Udf{
		Language: "python",
		Body: `{"handler":"legacy","source":"def legacy(ctx, x): return x",` +
			`"mode":"SCALAR","null_policy":"CALLED_ON_NULL_INPUT",` +
			`"abi_contract":"PYTHON_ARROW","adapter_version":"2026-09",` +
			`"sdk_version":"1.0"}`,
		ArgsType: []types.Type{types.T_int32.ToType()},
		RetType:  "int",
	}
	require.ErrorContains(t, routine.LoadPythonTypeContract(), "return descriptor")
}

func TestPythonTypeContractRejectsArgumentCountMismatch(t *testing.T) {
	argument, err := NewPythonTypeDescriptor(types.T_int32.ToType())
	require.NoError(t, err)
	returnType, err := NewPythonTypeDescriptor(types.T_int32.ToType())
	require.NoError(t, err)
	body, err := json.Marshal(PythonRoutineBody{
		Handler:        "identity",
		Source:         "def identity(ctx, x): return x",
		Mode:           "SCALAR",
		NullPolicy:     udf.NullCallHandler,
		ABIContract:    udf.PythonABIContract,
		AdapterVersion: udf.PythonAdapterVersion,
		SDKVersion:     udf.PythonSDKVersion,
		ArgTypes:       []PythonTypeDescriptor{argument},
		ReturnType:     &returnType,
	})
	require.NoError(t, err)
	routine := &Udf{Language: "python", Body: string(body)}
	require.NoError(t, routine.LoadPythonTypeContract())
	require.ErrorContains(t, routine.ValidatePythonTypeContract(), "descriptors for 0 arguments")
}

func TestPythonTypeContractReloadReplacesDescriptors(t *testing.T) {
	argument, err := NewPythonTypeDescriptor(types.T_int32.ToType())
	require.NoError(t, err)
	returnType, err := NewPythonTypeDescriptor(types.T_int32.ToType())
	require.NoError(t, err)
	body, err := json.Marshal(PythonRoutineBody{
		Handler:        "identity",
		Source:         "def identity(ctx, x): return x",
		Mode:           "SCALAR",
		NullPolicy:     udf.NullCallHandler,
		ABIContract:    udf.PythonABIContract,
		AdapterVersion: udf.PythonAdapterVersion,
		SDKVersion:     udf.PythonSDKVersion,
		ArgTypes:       []PythonTypeDescriptor{argument},
		ReturnType:     &returnType,
	})
	require.NoError(t, err)
	routine := &Udf{Language: "python", Body: string(body)}
	require.NoError(t, routine.LoadPythonTypeContract())
	require.NoError(t, routine.LoadPythonTypeContract())
	require.Len(t, routine.PythonArgTypes, 1)
}

func TestPythonRoutineBodyRejectsUnknownAndTrailingData(t *testing.T) {
	valid, err := json.Marshal(PythonRoutineBody{
		Handler:        "identity",
		Source:         "def identity(ctx): return 1",
		Mode:           "SCALAR",
		NullPolicy:     udf.NullCallHandler,
		ABIContract:    udf.PythonABIContract,
		AdapterVersion: udf.PythonAdapterVersion,
		SDKVersion:     udf.PythonSDKVersion,
		ReturnType:     func() *PythonTypeDescriptor { d, _ := NewPythonTypeDescriptor(types.T_int32.ToType()); return &d }(),
	})
	require.NoError(t, err)

	var withUnknown map[string]any
	require.NoError(t, json.Unmarshal(valid, &withUnknown))
	withUnknown["future_field"] = true
	unknown, err := json.Marshal(withUnknown)
	require.NoError(t, err)
	require.ErrorContains(t, func() error { _, err := DecodePythonRoutineBody(string(unknown)); return err }(), "unknown field")
	require.ErrorContains(t, func() error { _, err := DecodePythonRoutineBody(string(valid) + string(valid)); return err }(), "multiple JSON values")
}

func TestPythonTypeContractRejectsUnrepresentableArrowPrecision(t *testing.T) {
	returnDescriptor := PythonTypeDescriptor{
		TypeID:      int32(types.T_decimal128),
		Width:       39,
		OffsetWidth: 32,
	}
	body, err := json.Marshal(PythonRoutineBody{
		Handler:        "identity",
		Source:         "def identity(ctx): return 1",
		Mode:           "SCALAR",
		NullPolicy:     udf.NullCallHandler,
		ABIContract:    udf.PythonABIContract,
		AdapterVersion: udf.PythonAdapterVersion,
		SDKVersion:     udf.PythonSDKVersion,
		ReturnType:     &returnDescriptor,
	})
	require.NoError(t, err)
	routine := &Udf{Language: "python", Body: string(body)}
	require.ErrorContains(t, routine.LoadPythonTypeContract(), "precision")
}

func TestPythonBindingNormalizesDecimalMetadata(t *testing.T) {
	received := types.New(types.T_decimal64, 18, 2)
	required := types.New(types.T_decimal64, 18, 6)
	inputs := []types.Type{
		types.T_text.ToType(),
		received,
		required,
		types.T_decimal64.ToType(),
	}
	result := checkPythonUdf(nil, inputs)
	require.Equal(t, succeedWithCast, result.status)
	require.Equal(t, required, result.finalType[1])
}

func TestPythonExecutionValidatesDescriptorAndInputTypes(t *testing.T) {
	empty, err := vector.NewConstBytes(types.T_text.ToType(), nil, 0, nil)
	require.NoError(t, err)
	require.ErrorContains(t, validatePythonRoutineDescriptor(empty), "descriptor is empty")

	nonConst := vector.NewOffHeapVecWithTypeAndData(
		types.T_text.ToType(), []byte("routine"), 1, 1,
	)
	require.ErrorContains(t, validatePythonRoutineDescriptor(nonConst), "must be constant")

	input := vector.NewOffHeapVecWithTypeAndData(types.T_int32.ToType(), make([]byte, 4), 1, 1)
	require.ErrorContains(t,
		validatePythonInputVectors([]*vector.Vector{input}, []types.Type{types.T_int64.ToType()}, 1),
		"does not match",
	)
}

func TestPythonRoutineIsVolatile(t *testing.T) {
	resolved, err := GetFunctionByName(context.Background(), "python_user_defined_function", []types.Type{
		types.T_text.ToType(),
		types.T_text.ToType(),
	})
	require.NoError(t, err)
	_, overloadID := DecodeOverloadID(resolved.GetEncodedOverloadID())
	require.True(t, allSupportedFunctions[PYTHON_UDF].Overloads[overloadID].CannotFold())
}
