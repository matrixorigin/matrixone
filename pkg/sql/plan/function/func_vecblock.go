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
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// vecBlockFloat32 returns a vecf32 value, or a vecf8/vecf4 cell dequantized into buf.
func vecBlockFloat32(oid types.T, v []byte, buf *[]float32) ([]float32, error) {
	if !oid.IsBlockScaledVector() {
		return types.BytesToArray[float32](v), nil
	}
	c, err := types.ParseBlockScaledCell(v)
	if err != nil {
		return nil, err
	}
	*buf = slices.Grow((*buf)[:0], c.Dim)[:c.Dim]
	c.Dequantize(*buf)
	return *buf, nil
}

// InnerProductVecBlock is inner_product over vecf8/vecf4 arguments, each optionally
// vecf32: an fp32 dot product over the dequantized values.
func InnerProductVecBlock(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	oid1, oid2 := ivecs[0].GetType().Oid, ivecs[1].GetType().Oid
	var buf1, buf2 []float32
	return opBinaryBytesBytesToFixedWithErrorCheck[float64](ivecs, result, proc, length, func(v1, v2 []byte) (float64, error) {
		a1, err := vecBlockFloat32(oid1, v1, &buf1)
		if err != nil {
			return 0, err
		}
		a2, err := vecBlockFloat32(oid2, v2, &buf2)
		if err != nil {
			return 0, err
		}
		return moarray.InnerProduct[float32](a1, a2)
	}, selectList)
}
