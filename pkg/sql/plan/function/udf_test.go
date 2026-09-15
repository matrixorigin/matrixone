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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/stretchr/testify/require"
)

func pythonTestBody(body PythonRoutineBody) PythonRoutineBody {
	body.ArtifactDigest = udf.PythonInlineArtifactDigest(body.Handler, body.Source)
	body.EnvironmentDigest, _ = udf.PythonEnvironmentDigest()
	return body
}

func TestPythonTypeContractPreservesDeclaredShape(t *testing.T) {
	argument := types.New(types.T_decimal64, 18, 6)
	returnType := types.New(types.T_varchar, 64, 0)
	argumentDescriptor, err := NewPythonTypeDescriptor(argument)
	require.NoError(t, err)
	returnDescriptor, err := NewPythonTypeDescriptor(returnType)
	require.NoError(t, err)
	body, err := json.Marshal(pythonTestBody(PythonRoutineBody{
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		Handler:                 "identity",
		Source:                  "def identity(ctx, x): return x",
		Mode:                    "SCALAR",
		NullPolicy:              udf.NullCallHandler,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		SDKVersion:              udf.PythonSDKVersion,
		ArgTypes:                []PythonTypeDescriptor{argumentDescriptor},
		ReturnType:              &returnDescriptor,
	}))
	require.NoError(t, err)

	routine := &Udf{Language: "python", Body: string(body), RetType: "varchar", ArgsType: []types.Type{types.T_decimal64.ToType()}}
	require.NoError(t, routine.LoadPythonTypeContract())
	routine.Args = []*Arg{{Type: "decimal"}}
	require.NoError(t, routine.ValidatePythonTypeContract())
	require.NoError(t, routine.ValidatePythonCatalogSignature())
	require.Equal(t, argument, routine.GetArgsType()[0])
	require.Equal(t, returnType, routine.GetRetType())
	require.Equal(t, int32(6), routine.GetArgsPlanType()[0].Scale)
	require.Equal(t, int32(64), routine.GetRetPlanType().Width)
}

func TestPythonSignatureMetadataIsCanonicalAndSensitiveToShape(t *testing.T) {
	argument, err := NewPythonTypeDescriptor(types.New(types.T_decimal64, 18, 2))
	require.NoError(t, err)
	returnType, err := NewPythonTypeDescriptor(types.T_varchar.ToType())
	require.NoError(t, err)
	input, output, fingerprint, err := PythonSignatureMetadata([]PythonTypeDescriptor{argument}, &returnType)
	require.NoError(t, err)
	require.Equal(t, `[{"type_id":32,"width":18,"scale":2,"offset_width":32}]`, input)
	encodedReturn, err := json.Marshal(returnType)
	require.NoError(t, err)
	require.Equal(t, string(encodedReturn), output)
	require.Len(t, fingerprint, 64)

	otherArgument, err := NewPythonTypeDescriptor(types.New(types.T_decimal64, 18, 6))
	require.NoError(t, err)
	_, _, otherFingerprint, err := PythonSignatureMetadata([]PythonTypeDescriptor{otherArgument}, &returnType)
	require.NoError(t, err)
	require.NotEqual(t, fingerprint, otherFingerprint)
}

func TestPythonUdfArgTypeMatchPrefersExactDescriptor(t *testing.T) {
	source := types.New(types.T_decimal64, 18, 2)
	exact := types.New(types.T_decimal64, 18, 2)
	coerce := types.New(types.T_decimal64, 18, 6)
	exactOK, exactCost := PythonUdfArgTypeMatch([]types.Type{source}, []types.Type{exact})
	coerceOK, coerceCost := PythonUdfArgTypeMatch([]types.Type{source}, []types.Type{coerce})
	require.True(t, exactOK)
	require.True(t, coerceOK)
	require.Less(t, exactCost, coerceCost)
}

func TestPythonUdfArgTypeCastKeepsDeclaredDescriptorForSameOID(t *testing.T) {
	source := types.New(types.T_decimal64, 18, 2)
	target := types.New(types.T_decimal64, 18, 6)

	castTypes := PythonUdfArgTypeCast([]types.Type{source}, []types.Type{target})
	require.Equal(t, []types.Type{target}, castTypes)
	require.Nil(t, PythonUdfArgTypeCast([]types.Type{source}, nil))
}

func TestPythonCatalogTypeNamePreservesLogicalDecimalAlias(t *testing.T) {
	require.Equal(t, "decimal", PythonCatalogTypeName(types.New(types.T_decimal64, 18, 2)))
	require.Equal(t, "decimal", PythonCatalogTypeName(types.New(types.T_decimal128, 38, 10)))
	require.Equal(t, "bigint", PythonCatalogTypeName(types.T_int64.ToType()))
}

func TestPythonCatalogSignatureRejectsLogicalDescriptorDrift(t *testing.T) {
	argument, err := NewPythonTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	returnType, err := NewPythonTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	raw, err := json.Marshal(pythonTestBody(PythonRoutineBody{
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		Handler:                 "identity",
		Source:                  "def identity(ctx, value): return value",
		Mode:                    "SCALAR",
		NullPolicy:              udf.NullCallHandler,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		SDKVersion:              udf.PythonSDKVersion,
		ArgTypes:                []PythonTypeDescriptor{argument},
		ReturnType:              &returnType,
	}))
	require.NoError(t, err)
	routine := &Udf{Language: "python", Body: string(raw), RetType: "bigint"}
	require.NoError(t, routine.LoadPythonTypeContract())
	routine.Args = []*Arg{{Type: "varchar"}}
	require.ErrorContains(t, routine.ValidatePythonCatalogSignature(), "argument 1")

	routine.Args[0].Type = "bigint"
	routine.RetType = "varchar"
	require.ErrorContains(t, routine.ValidatePythonCatalogSignature(), "return type")
}

func TestPythonTypeContractRejectsMissingReturnDescriptor(t *testing.T) {
	body := pythonTestBody(PythonRoutineBody{
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		Handler:                 "legacy",
		Source:                  "def legacy(ctx, x): return x",
		Mode:                    "SCALAR",
		NullPolicy:              udf.NullCallHandler,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		SDKVersion:              udf.PythonSDKVersion,
	})
	rawBody, err := json.Marshal(body)
	require.NoError(t, err)
	routine := &Udf{
		Language: "python",
		Body:     string(rawBody),
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
	body, err := json.Marshal(pythonTestBody(PythonRoutineBody{
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		Handler:                 "identity",
		Source:                  "def identity(ctx, x): return x",
		Mode:                    "SCALAR",
		NullPolicy:              udf.NullCallHandler,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		SDKVersion:              udf.PythonSDKVersion,
		ArgTypes:                []PythonTypeDescriptor{argument},
		ReturnType:              &returnType,
	}))
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
	body, err := json.Marshal(pythonTestBody(PythonRoutineBody{
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		Handler:                 "identity",
		Source:                  "def identity(ctx, x): return x",
		Mode:                    "SCALAR",
		NullPolicy:              udf.NullCallHandler,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		SDKVersion:              udf.PythonSDKVersion,
		ArgTypes:                []PythonTypeDescriptor{argument},
		ReturnType:              &returnType,
	}))
	require.NoError(t, err)
	routine := &Udf{Language: "python", Body: string(body)}
	require.NoError(t, routine.LoadPythonTypeContract())
	require.NoError(t, routine.LoadPythonTypeContract())
	require.Len(t, routine.PythonArgTypes, 1)
}

func TestPythonRoutineBodyRejectsUnknownAndTrailingData(t *testing.T) {
	valid, err := json.Marshal(pythonTestBody(PythonRoutineBody{
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		Handler:                 "identity",
		Source:                  "def identity(ctx): return 1",
		Mode:                    "SCALAR",
		NullPolicy:              udf.NullCallHandler,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		SDKVersion:              udf.PythonSDKVersion,
		ReturnType:              func() *PythonTypeDescriptor { d, _ := NewPythonTypeDescriptor(types.T_int32.ToType()); return &d }(),
	}))
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
	body, err := json.Marshal(pythonTestBody(PythonRoutineBody{
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		Handler:                 "identity",
		Source:                  "def identity(ctx): return 1",
		Mode:                    "SCALAR",
		NullPolicy:              udf.NullCallHandler,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		SDKVersion:              udf.PythonSDKVersion,
		ReturnType:              &returnDescriptor,
	}))
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

func TestPythonTypeEqualityUsesSQLIntegerSemantics(t *testing.T) {
	declared := types.New(types.T_int32, 0, -1)
	caseResult := types.New(types.T_int32, 32, -1)
	require.True(t, pythonTypesEqual(declared, caseResult))
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

func TestPythonRoutineCallCarriesExactCatalogContract(t *testing.T) {
	descriptor, err := NewPythonTypeDescriptor(types.T_int64.ToType())
	require.NoError(t, err)
	body, err := json.Marshal(pythonTestBody(PythonRoutineBody{
		DefinitionSchemaVersion: udf.PythonDefinitionSchemaVersion,
		Handler:                 "identity",
		Source:                  "def identity(ctx, value): return value",
		Mode:                    "SCALAR",
		NullPolicy:              udf.NullCallHandler,
		ABIContract:             udf.PythonABIContract,
		AdapterVersion:          udf.PythonAdapterVersion,
		SDKVersion:              udf.PythonSDKVersion,
		ArgTypes:                []PythonTypeDescriptor{descriptor},
		ReturnType:              &descriptor,
	}))
	require.NoError(t, err)

	routine := &Udf{
		FunctionID:       41,
		AccountID:        7,
		DatabaseID:       8,
		Revision:         7,
		NamespaceVersion: 12,
		Language:         "python",
		Body:             string(body),
		Args:             []*Arg{{Name: "value", Type: "bigint"}},
	}
	require.NoError(t, routine.LoadPythonTypeContract())
	require.NoError(t, routine.ValidatePythonTypeContract())

	call, err := routine.GetRoutineCall()
	require.NoError(t, err)
	require.Equal(t, int32(udf.PythonPlanContractVersion), call.ContractVersion)
	require.Equal(t, &planpb.FunctionRef{FunctionId: 41, Revision: 7, NamespaceVersion: 12, AccountId: 7, DatabaseId: 8}, call.FunctionRef)
	require.Equal(t, "python", call.Language)
	require.Equal(t, "VOLATILE", call.Volatility)
	require.Equal(t, udf.NullCallHandler, call.NullPolicy)
	require.True(t, call.MayError)
	require.Equal(t, "INVOKER", call.SecurityMode)
	require.False(t, call.Leakproof)
	require.Empty(t, call.Context)
	require.Empty(t, call.GetPython().Source, "source is catalog payload, never executable plan state")
	require.NotEmpty(t, call.GetPython().DefinitionFingerprint)
}

func TestPythonRoutineCallRejectsNoncanonicalLanguage(t *testing.T) {
	routine := &Udf{Language: "PYTHON"}
	_, err := routine.GetRoutineCall()
	require.ErrorContains(t, err, `routine call has unsupported language "PYTHON"`)
}

func TestPythonContractPreflightRejectsNoncanonicalLanguage(t *testing.T) {
	routine := &Udf{Language: "PYTHON"}
	require.ErrorContains(t, routine.LoadPythonTypeContract(), "unsupported routine language")
	require.ErrorContains(t, routine.ValidatePythonTypeContract(), "unsupported routine language")
	require.ErrorContains(t, routine.ValidatePythonCatalogSignature(), "unsupported routine language")
}

func TestSQLRoutineFingerprintAndCallUseSharedRevisionContract(t *testing.T) {
	fingerprint, err := SQLRoutineFingerprint("select value + 1", "[\"bigint\"]", "bigint")
	require.NoError(t, err)
	require.Len(t, fingerprint, 64)
	changed, err := SQLRoutineFingerprint("select value + 2", "[\"bigint\"]", "bigint")
	require.NoError(t, err)
	require.NotEqual(t, fingerprint, changed)

	routine := &Udf{
		FunctionID:                      41,
		AccountID:                       7,
		DatabaseID:                      8,
		Revision:                        3,
		NamespaceVersion:                4,
		Language:                        udf.LanguageSQL,
		RetType:                         "bigint",
		ArgsType:                        []types.Type{types.T_int64.ToType()},
		DefinitionFingerprint:           fingerprint,
		SemanticDefinitionSchemaVersion: udf.SQLDefinitionSchemaVersion,
		Volatility:                      "VOLATILE",
		NullPolicy:                      udf.NullCallHandler,
	}
	call, err := routine.GetRoutineCall()
	require.NoError(t, err)
	require.Equal(t, udf.LanguageSQL, call.Language)
	require.Equal(t, &planpb.FunctionRef{
		FunctionId: 41, Revision: 3, NamespaceVersion: 4, AccountId: 7, DatabaseId: 8,
	}, call.FunctionRef)
	require.Equal(t, fingerprint, string(call.GetSql().DefinitionFingerprint))
	require.Equal(t, int32(udf.SQLDefinitionSchemaVersion), call.GetSql().SemanticDefinitionSchemaVersion)
	require.True(t, call.MayError)
	require.Equal(t, "DEFINER", call.SecurityMode)
	require.False(t, call.Leakproof)
}

func TestPythonJSONPlanRequiresExactIdentity(t *testing.T) {
	raw, err := json.Marshal(UdfWithContext{
		Udf: &Udf{
			Language: "python",
			Body:     `{"legacy":true}`,
		},
		PlanContractVersion: udf.PythonPlanContractVersion,
	})
	require.NoError(t, err)
	_, err = DecodeUdfWithContext(raw)
	require.ErrorContains(t, err, "JSON plan is not executable")
}

func TestPythonInvocationTupleUsesOneShotGroupsByDefault(t *testing.T) {
	first, err := NewInvocationTuple(nil, "query-1", 7)
	require.NoError(t, err)
	second, err := NewInvocationTuple(nil, "query-1", 7)
	require.NoError(t, err)
	require.NotEqual(t, first.InvocationID, second.InvocationID)
	require.NotEqual(t, first.GroupID, second.GroupID)
	require.Equal(t, first.GroupEpoch, uint64(1))
	require.Equal(t, second.GroupEpoch, uint64(1))

	_, err = NewInvocationTuple(map[string]string{"group_id": "shared-group"}, "query-1", 7)
	require.ErrorContains(t, err, "explicit group_id requires group_epoch")
	shared, err := NewInvocationTuple(map[string]string{
		"group_id":    "shared-group",
		"group_epoch": "3",
	}, "query-1", 7)
	require.NoError(t, err)
	require.Equal(t, "shared-group", shared.GroupID)
	require.Equal(t, uint64(3), shared.GroupEpoch)
}
