// Copyright 2021 - 2024 Matrix Origin
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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// setVecBlockOperand points o at a vecf8/vecf4 cell or a vecf32 array.
func setVecBlockOperand(o *metric.VecBlockOperand, oid types.T, v []byte) error {
	if !oid.IsBlockScaledVector() {
		o.Cell, o.F32 = types.BlockScaledCell{}, types.BytesToArray[float32](v)
		return nil
	}
	c, err := types.ParseBlockScaledCell(v)
	if err != nil {
		return err
	}
	o.Cell, o.F32 = c, nil
	return nil
}

// vecBlockDistance runs fn over vecf8/vecf4 arguments, each optionally vecf32.
func vecBlockDistance(fn func(x, y *metric.VecBlockOperand) (float64, error)) executeLogicOfOverload {
	return func(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
		oid1, oid2 := ivecs[0].GetType().Oid, ivecs[1].GetType().Oid
		var x, y metric.VecBlockOperand
		return opBinaryBytesBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v1, v2 []byte) (float64, error) {
			if err := setVecBlockOperand(&x, oid1, v1); err != nil {
				return 0, err
			}
			if err := setVecBlockOperand(&y, oid2, v2); err != nil {
				return 0, err
			}
			return fn(&x, &y)
		}, selectList)
	}
}

// InnerProductVecBlock is inner_product: the negated dot product.
var InnerProductVecBlock = vecBlockDistance(func(x, y *metric.VecBlockOperand) (float64, error) {
	d, err := metric.VecBlockDot(x, y)
	if err != nil {
		return 0, err
	}
	return metric.CheckFiniteDist(metric.RoundDistanceToElemDomain(-d), metric.MetricWhat(metric.Metric_InnerProduct))
})

// L2DistanceVecBlock is l2_distance.
var L2DistanceVecBlock = vecBlockDistance(func(x, y *metric.VecBlockOperand) (float64, error) {
	sq, err := metric.VecBlockL2DistanceSq(x, y)
	if err != nil {
		return 0, err
	}
	d, err := metric.L2FromSquared(sq)
	if err != nil {
		return 0, err
	}
	return metric.CheckFiniteDist(metric.RoundDistanceToElemDomain(d), metric.MetricWhat(metric.Metric_L2Distance))
})

// L2DistanceSqVecBlock is l2_distance_sq: the unrounded square.
var L2DistanceSqVecBlock = vecBlockDistance(func(x, y *metric.VecBlockOperand) (float64, error) {
	sq, err := metric.VecBlockL2DistanceSq(x, y)
	if err != nil {
		return 0, err
	}
	return metric.CheckFiniteDist(sq, metric.MetricWhat(metric.Metric_L2sqDistance))
})

// L1DistanceVecBlock is l1_distance.
var L1DistanceVecBlock = vecBlockDistance(func(x, y *metric.VecBlockOperand) (float64, error) {
	d, err := metric.VecBlockL1Distance(x, y)
	if err != nil {
		return 0, err
	}
	return metric.CheckFiniteDist(metric.RoundDistanceToElemDomain(d), metric.MetricWhat(metric.Metric_L1Distance))
})

// CosineDistanceVecBlock is cosine_distance.
var CosineDistanceVecBlock = vecBlockDistance(func(x, y *metric.VecBlockOperand) (float64, error) {
	d, err := metric.VecBlockCosineDistance(x, y)
	return metric.RoundDistanceToElemDomain(d), err
})

// CosineSimilarityVecBlock is cosine_similarity.
var CosineSimilarityVecBlock = vecBlockDistance(func(x, y *metric.VecBlockOperand) (float64, error) {
	d, err := metric.VecBlockCosineSimilarity(x, y)
	if err != nil {
		return 0, err
	}
	return metric.RoundDistanceToElemDomain(d), nil
})

// VectorDimsVecBlock is vector_dims over a vecf8/vecf4 cell.
func VectorDimsVecBlock(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToFixedWithErrorCheck[int64](ivecs, result, proc, length, func(in []byte) (int64, error) {
		c, err := types.ParseBlockScaledCell(in)
		if err != nil {
			return 0, err
		}
		return int64(c.Dim), nil
	}, selectList)
}

// NormalizeL2VecBlock is normalize_l2 over a vecf8/vecf4 cell; the result is re-encoded in the
// argument's format.
func NormalizeL2VecBlock(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	var in, out []float32
	var cell []byte
	return opUnaryBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(v []byte) ([]byte, error) {
		c, err := types.ParseBlockScaledCell(v)
		if err != nil {
			return nil, err
		}
		in = slices.Grow(in[:0], c.Dim)[:c.Dim]
		out = slices.Grow(out[:0], c.Dim)[:c.Dim]
		c.Dequantize(in)
		if err = metric.NormalizeL2(in, out); err != nil {
			return nil, err
		}
		cell, err = types.AppendBlockScaled(cell[:0], c.Format, out)
		return cell, err
	}, selectList)
}

// vecBlockBinaryOverloads returns the vecf8/vecf4 overloads of a binary distance function,
// numbered from first.
func vecBlockBinaryOverloads(first int, op executeLogicOfOverload) []overload {
	args := [][]types.T{
		{types.T_array_float8, types.T_array_float8},
		{types.T_array_float4, types.T_array_float4},
		{types.T_array_float8, types.T_array_float32},
		{types.T_array_float32, types.T_array_float8},
		{types.T_array_float4, types.T_array_float32},
		{types.T_array_float32, types.T_array_float4},
		{types.T_array_float8, types.T_array_float4},
		{types.T_array_float4, types.T_array_float8},
	}
	ret := make([]overload, len(args))
	for i, a := range args {
		ret[i] = overload{
			overloadId: first + i,
			args:       a,
			retType:    func(parameters []types.Type) types.Type { return types.T_float64.ToType() },
			newOp:      func() executeLogicOfOverload { return op },
		}
	}
	return ret
}
