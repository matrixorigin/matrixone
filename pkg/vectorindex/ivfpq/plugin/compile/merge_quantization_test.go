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

package compile

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
)

// MERGE with a different QUANTIZATION is rejected; MERGE with the stored or no QUANTIZATION,
// and a non-MERGE change, are accepted.
func TestValidateReindexParams_MergeCannotChangeQuantization(t *testing.T) {
	old := map[string]string{
		catalog.IndexAlgoParamOpType: "vector_l2_ops",
		catalog.Quantization:         "float16",
	}

	_, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{
		Params: map[string]string{catalog.Quantization: "int8"},
		Merge:  true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "changing QUANTIZATION requires a REBUILD")

	_, err = Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{
		Params: map[string]string{catalog.Quantization: "FLOAT16"},
		Merge:  true,
	})
	require.NoError(t, err)

	_, err = Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{Merge: true})
	require.NoError(t, err)

	got, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{
		Params: map[string]string{catalog.Quantization: "int8"},
	})
	require.NoError(t, err)
	require.Equal(t, "int8", got[catalog.Quantization])
}

// A REINDEX that changes QUANTIZATION to a wider type than the base column is rejected; a change
// to an equal or narrower type, an unchanged value, and an unknown base type are accepted.
func TestValidateReindexParams_QuantizationUpcast(t *testing.T) {
	old := map[string]string{
		catalog.IndexAlgoParamOpType: "vector_l2_ops",
		catalog.Quantization:         "int8",
	}

	_, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{
		Params:         map[string]string{catalog.Quantization: "float32"},
		BaseVectorType: types.T_array_float16,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "IvfPQ QUANTIZATION 'float32' (4 bytes/element) cannot upcast base column VECF16")

	got, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{
		Params:         map[string]string{catalog.Quantization: "float16"},
		BaseVectorType: types.T_array_float16,
	})
	require.NoError(t, err)
	require.Equal(t, "float16", got[catalog.Quantization])

	// cuvs stores quantization 'float32' by default on a vecf16 base; re-stating it is not a change.
	_, err = Hooks{}.ValidateReindexParams(
		map[string]string{catalog.IndexAlgoParamOpType: "vector_l2_ops", catalog.Quantization: "float32"},
		compileplugin.ReindexParamUpdate{
			Params:         map[string]string{catalog.Quantization: "float32"},
			BaseVectorType: types.T_array_float16,
		})
	require.NoError(t, err)

	_, err = Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{
		Params: map[string]string{catalog.Quantization: "float32"},
	})
	require.NoError(t, err)
}
