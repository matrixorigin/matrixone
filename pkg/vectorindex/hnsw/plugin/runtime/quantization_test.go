// Copyright 2026 Matrix Origin
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

package runtime

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func TestHnswValidQuantization(t *testing.T) {
	for _, tc := range []struct {
		quant, op string
		ok        bool
	}{
		{"", metric.OpType_L2Distance, true},
		{"float32", metric.OpType_L2Distance, true},
		{"FLOAT16", metric.OpType_InnerProduct, true},
		{"bf16", metric.OpType_CosineDistance, true},
		{"float64", metric.OpType_L2Distance, true},
		{"int8", metric.OpType_L2Distance, true},
		{"int8", "", true},
		{"UINT8", metric.OpType_L2Distance, true},
		{"int8", metric.OpType_CosineDistance, false},
		{"int8", metric.OpType_InnerProduct, false},
		{"uint8", metric.OpType_CosineDistance, false},
		{"int4", metric.OpType_L2Distance, false},
	} {
		err := CatalogHooks{}.ValidQuantization(tc.quant, tc.op)
		if tc.ok {
			require.NoError(t, err, "%s/%s", tc.quant, tc.op)
		} else {
			require.Error(t, err, "%s/%s", tc.quant, tc.op)
		}
	}
}

func TestHnswCheckQuantizationBase(t *testing.T) {
	for _, tc := range []struct {
		quant string
		base  types.T
		ok    bool
	}{
		{"float32", types.T_array_float32, true},
		{"float16", types.T_array_float32, true},
		{"bf16", types.T_array_float32, true},
		{"int8", types.T_array_float32, true},
		{"float32", types.T_array_float64, true},
		{"float64", types.T_array_float64, true},
		{"float64", types.T_array_float32, false},
	} {
		err := CheckQuantizationBase(tc.quant, tc.base)
		if tc.ok {
			require.NoError(t, err, "%s on %s", tc.quant, tc.base)
		} else {
			require.Error(t, err, "%s on %s", tc.quant, tc.base)
		}
	}
}

func TestHnswParamsFromTreeQuantization(t *testing.T) {
	got, err := CatalogHooks{}.ParamsFromTree(&tree.Index{IndexOption: &tree.IndexOption{Quantization: "Float16"}})
	require.NoError(t, err)
	require.Equal(t, "float16", got[catalog.Quantization])

	got, err = CatalogHooks{}.ParamsFromTree(&tree.Index{IndexOption: &tree.IndexOption{}})
	require.NoError(t, err)
	require.NotContains(t, got, catalog.Quantization, "no QUANTIZATION keeps the base column type")

	// int8/uint8 with the default op_type (L2)
	got, err = CatalogHooks{}.ParamsFromTree(&tree.Index{IndexOption: &tree.IndexOption{Quantization: "int8"}})
	require.NoError(t, err)
	require.Equal(t, "int8", got[catalog.Quantization])
	got, err = CatalogHooks{}.ParamsFromTree(&tree.Index{IndexOption: &tree.IndexOption{Quantization: "uint8"}})
	require.NoError(t, err)
	require.Equal(t, "uint8", got[catalog.Quantization])

	// int8 is L2-only
	_, err = CatalogHooks{}.ParamsFromTree(&tree.Index{IndexOption: &tree.IndexOption{
		Quantization:          "int8",
		AlgoParamVectorOpType: metric.OpType_CosineDistance,
	}})
	require.Error(t, err)
}
