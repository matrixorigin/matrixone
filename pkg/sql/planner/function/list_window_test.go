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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestWindowFunctionCheckFn tests the checkFn for window functions
func TestWindowFunctionCheckFn(t *testing.T) {
	// Find the window functions in supportedWindowInNewFramework
	var lagFunc, leadFunc, firstValueFunc, lastValueFunc, nthValueFunc *FuncNew
	for i := range supportedWindowInNewFramework {
		switch supportedWindowInNewFramework[i].functionId {
		case LAG:
			lagFunc = &supportedWindowInNewFramework[i]
		case LEAD:
			leadFunc = &supportedWindowInNewFramework[i]
		case FIRST_VALUE:
			firstValueFunc = &supportedWindowInNewFramework[i]
		case LAST_VALUE:
			lastValueFunc = &supportedWindowInNewFramework[i]
		case NTH_VALUE:
			nthValueFunc = &supportedWindowInNewFramework[i]
		}
	}

	require.NotNil(t, lagFunc, "LAG function should be defined")
	require.NotNil(t, leadFunc, "LEAD function should be defined")
	require.NotNil(t, firstValueFunc, "FIRST_VALUE function should be defined")
	require.NotNil(t, lastValueFunc, "LAST_VALUE function should be defined")
	require.NotNil(t, nthValueFunc, "NTH_VALUE function should be defined")

	intType := types.T_int64.ToType()

	// Test LAG checkFn
	t.Run("LAG_valid_1_param", func(t *testing.T) {
		result := lagFunc.checkFn(lagFunc.Overloads, []types.Type{intType})
		require.Equal(t, 0, result.idx)
	})
	t.Run("LAG_valid_2_params", func(t *testing.T) {
		result := lagFunc.checkFn(lagFunc.Overloads, []types.Type{intType, intType})
		require.Equal(t, 0, result.idx)
	})
	t.Run("LAG_valid_3_params", func(t *testing.T) {
		result := lagFunc.checkFn(lagFunc.Overloads, []types.Type{intType, intType, intType})
		require.Equal(t, 0, result.idx)
	})
	t.Run("LAG_invalid_0_params", func(t *testing.T) {
		result := lagFunc.checkFn(lagFunc.Overloads, []types.Type{})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})
	t.Run("LAG_invalid_4_params", func(t *testing.T) {
		result := lagFunc.checkFn(lagFunc.Overloads, []types.Type{intType, intType, intType, intType})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})

	// Test LEAD checkFn
	t.Run("LEAD_valid_1_param", func(t *testing.T) {
		result := leadFunc.checkFn(leadFunc.Overloads, []types.Type{intType})
		require.Equal(t, 0, result.idx)
	})
	t.Run("LEAD_invalid_0_params", func(t *testing.T) {
		result := leadFunc.checkFn(leadFunc.Overloads, []types.Type{})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})
	t.Run("LEAD_invalid_4_params", func(t *testing.T) {
		result := leadFunc.checkFn(leadFunc.Overloads, []types.Type{intType, intType, intType, intType})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})

	// Test FIRST_VALUE checkFn
	t.Run("FIRST_VALUE_valid_1_param", func(t *testing.T) {
		result := firstValueFunc.checkFn(firstValueFunc.Overloads, []types.Type{intType})
		require.Equal(t, 0, result.idx)
	})
	t.Run("FIRST_VALUE_invalid_0_params", func(t *testing.T) {
		result := firstValueFunc.checkFn(firstValueFunc.Overloads, []types.Type{})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})
	t.Run("FIRST_VALUE_invalid_2_params", func(t *testing.T) {
		result := firstValueFunc.checkFn(firstValueFunc.Overloads, []types.Type{intType, intType})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})

	// Test LAST_VALUE checkFn
	t.Run("LAST_VALUE_valid_1_param", func(t *testing.T) {
		result := lastValueFunc.checkFn(lastValueFunc.Overloads, []types.Type{intType})
		require.Equal(t, 0, result.idx)
	})
	t.Run("LAST_VALUE_invalid_0_params", func(t *testing.T) {
		result := lastValueFunc.checkFn(lastValueFunc.Overloads, []types.Type{})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})
	t.Run("LAST_VALUE_invalid_2_params", func(t *testing.T) {
		result := lastValueFunc.checkFn(lastValueFunc.Overloads, []types.Type{intType, intType})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})

	// Test NTH_VALUE checkFn
	t.Run("NTH_VALUE_valid_2_params", func(t *testing.T) {
		result := nthValueFunc.checkFn(nthValueFunc.Overloads, []types.Type{intType, intType})
		require.Equal(t, 0, result.idx)
	})
	t.Run("NTH_VALUE_invalid_0_params", func(t *testing.T) {
		result := nthValueFunc.checkFn(nthValueFunc.Overloads, []types.Type{})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})
	t.Run("NTH_VALUE_invalid_1_param", func(t *testing.T) {
		result := nthValueFunc.checkFn(nthValueFunc.Overloads, []types.Type{intType})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})
	t.Run("NTH_VALUE_invalid_3_params", func(t *testing.T) {
		result := nthValueFunc.checkFn(nthValueFunc.Overloads, []types.Type{intType, intType, intType})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})
}

// TestWindowFunctionRetType tests the retType for window functions
func TestWindowFunctionRetType(t *testing.T) {
	// Find the window functions
	var lagFunc, leadFunc, firstValueFunc, lastValueFunc, nthValueFunc *FuncNew
	for i := range supportedWindowInNewFramework {
		switch supportedWindowInNewFramework[i].functionId {
		case LAG:
			lagFunc = &supportedWindowInNewFramework[i]
		case LEAD:
			leadFunc = &supportedWindowInNewFramework[i]
		case FIRST_VALUE:
			firstValueFunc = &supportedWindowInNewFramework[i]
		case LAST_VALUE:
			lastValueFunc = &supportedWindowInNewFramework[i]
		case NTH_VALUE:
			nthValueFunc = &supportedWindowInNewFramework[i]
		}
	}

	intType := types.T_int64.ToType()
	varcharType := types.T_varchar.ToType()

	// Test LAG retType
	t.Run("LAG_retType_with_params", func(t *testing.T) {
		retType := lagFunc.Overloads[0].retType([]types.Type{intType})
		require.Equal(t, intType, retType)
	})
	t.Run("LAG_retType_empty_params", func(t *testing.T) {
		retType := lagFunc.Overloads[0].retType([]types.Type{})
		require.Equal(t, types.T_any.ToType(), retType)
	})

	// Test LEAD retType
	t.Run("LEAD_retType_with_params", func(t *testing.T) {
		retType := leadFunc.Overloads[0].retType([]types.Type{varcharType})
		require.Equal(t, varcharType, retType)
	})
	t.Run("LEAD_retType_empty_params", func(t *testing.T) {
		retType := leadFunc.Overloads[0].retType([]types.Type{})
		require.Equal(t, types.T_any.ToType(), retType)
	})

	// Test FIRST_VALUE retType
	t.Run("FIRST_VALUE_retType_with_params", func(t *testing.T) {
		retType := firstValueFunc.Overloads[0].retType([]types.Type{intType})
		require.Equal(t, intType, retType)
	})
	t.Run("FIRST_VALUE_retType_empty_params", func(t *testing.T) {
		retType := firstValueFunc.Overloads[0].retType([]types.Type{})
		require.Equal(t, types.T_any.ToType(), retType)
	})

	// Test LAST_VALUE retType
	t.Run("LAST_VALUE_retType_with_params", func(t *testing.T) {
		retType := lastValueFunc.Overloads[0].retType([]types.Type{intType})
		require.Equal(t, intType, retType)
	})
	t.Run("LAST_VALUE_retType_empty_params", func(t *testing.T) {
		retType := lastValueFunc.Overloads[0].retType([]types.Type{})
		require.Equal(t, types.T_any.ToType(), retType)
	})

	// Test NTH_VALUE retType
	t.Run("NTH_VALUE_retType_with_params", func(t *testing.T) {
		retType := nthValueFunc.Overloads[0].retType([]types.Type{intType, intType})
		require.Equal(t, intType, retType)
	})
	t.Run("NTH_VALUE_retType_empty_params", func(t *testing.T) {
		retType := nthValueFunc.Overloads[0].retType([]types.Type{})
		require.Equal(t, types.T_any.ToType(), retType)
	})
}

// TestCumeDistCheckFn tests the checkFn for CUME_DIST window function
func TestCumeDistCheckFn(t *testing.T) {
	// Find CUME_DIST function
	var cumeDistFunc *FuncNew
	for i := range supportedWindowInNewFramework {
		if supportedWindowInNewFramework[i].functionId == CUME_DIST {
			cumeDistFunc = &supportedWindowInNewFramework[i]
			break
		}
	}

	require.NotNil(t, cumeDistFunc, "CUME_DIST function should be defined")

	intType := types.T_int64.ToType()

	// Test valid case: no parameters
	t.Run("CUME_DIST_valid_0_params", func(t *testing.T) {
		result := cumeDistFunc.checkFn(cumeDistFunc.Overloads, []types.Type{})
		require.Equal(t, 0, result.idx)
	})

	// Test invalid case: with parameters (this covers the error branch)
	t.Run("CUME_DIST_invalid_1_param", func(t *testing.T) {
		result := cumeDistFunc.checkFn(cumeDistFunc.Overloads, []types.Type{intType})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})

	t.Run("CUME_DIST_invalid_2_params", func(t *testing.T) {
		result := cumeDistFunc.checkFn(cumeDistFunc.Overloads, []types.Type{intType, intType})
		require.Equal(t, failedFunctionParametersWrong, result.status)
	})
}
