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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// vecf8/vecf4 casts: text "[...]", a BLOB of little-endian float32 elements (the binary
// vector input of vecf32), vecf32 and vecf8/vecf4 sources quantize into the target cell
// format; the exact text (a JSON object, types.BlockScaledToJSON) and the stored cell as a
// BLOB (vecblock_binary) build the cell as written; vecf8/vecf4 sources dequantize to vecf32. Text targets are not
// casts, as for vecf32; values render as text through the output path.

func init() {
	blockScaled := []types.T{types.T_array_float8, types.T_array_float4}
	for _, s := range []types.T{types.T_any, types.T_char, types.T_varchar, types.T_text, types.T_blob, types.T_array_float32} {
		supportedTypeCast[s] = append(supportedTypeCast[s], blockScaled...)
	}
	for _, s := range blockScaled {
		supportedTypeCast[s] = append(supportedTypeCast[s], types.T_array_float32, types.T_array_float8, types.T_array_float4)
	}
}

// castRowSkipped reports a row outside the select list; it is NULL and not converted.
func castRowSkipped(selectList *FunctionSelectList, i uint64) bool {
	return selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(i)
}

// checkVectorCastDim enforces a declared target dimension; MaxArrayDimension is unsized.
func checkVectorCastDim(to types.Type, dim int) error {
	if w := int(to.Width); w > 0 && w != types.MaxArrayDimension && w != dim {
		return moerr.NewArrayDefMismatchNoCtx(w, dim)
	}
	return nil
}

// castToBlockScaled casts text, vecf32, vecf8 or vecf4 to a vecf8/vecf4 target.
func castToBlockScaled(proc *process.Process, from *vector.Vector, toType types.Type,
	result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	f, _ := toType.Oid.BlockScaledFormat()
	src := vector.GenerateFunctionStrParameter(from)
	rs := vector.MustFunctionResult[types.Varlena](result)
	fromOid := from.GetType().Oid
	var cell []byte
	for i := uint64(0); i < uint64(length); i++ {
		v, null := src.GetStrValue(i)
		if null || (len(v) == 0 && fromOid.IsMySQLString()) || castRowSkipped(selectList, i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		var arr []float32
		switch {
		case fromOid == types.T_blob:
			// the stored cell (vecblock_binary) as is, or float32 elements quantized
			c, err := types.BlockScaledFromBinary(f, int(toType.Width), v)
			if err != nil {
				return err
			}
			if err := checkVectorCastDim(toType, types.BlockScaledDim(c)); err != nil {
				return err
			}
			if err := rs.AppendBytes(c, false); err != nil {
				return err
			}
			continue
		case fromOid.IsMySQLString() && types.IsBlockScaledJSON(convertByteSliceToString(v)):
			// the exact form: the cell as written, not quantized
			exact, err := types.BlockScaledFromJSON(f, convertByteSliceToString(v))
			if err != nil {
				return err
			}
			if err := checkVectorCastDim(toType, types.BlockScaledDim(exact)); err != nil {
				return err
			}
			if err := rs.AppendBytes(exact, false); err != nil {
				return err
			}
			continue
		case fromOid.IsMySQLString():
			a, err := types.StringToArray[float32](convertByteSliceToString(v))
			if err != nil {
				return err
			}
			arr = a
		case fromOid == types.T_array_float32:
			if len(v)%4 != 0 {
				return moerr.NewInvalidInputNoCtx("vector payload is not aligned to its element size")
			}
			arr = types.BytesToArray[float32](v)
		case fromOid.IsBlockScaledArray():
			c, err := types.ParseBlockScaledCell(v)
			if err != nil {
				return err
			}
			if err := checkVectorCastDim(toType, c.Dim); err != nil {
				return err
			}
			if c.Format == f {
				if err := rs.AppendBytes(v, false); err != nil {
					return err
				}
				continue
			}
			arr = make([]float32, c.Dim)
			c.Dequantize(arr)
		default:
			return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("unsupported cast from %s to %s", from.GetType(), toType))
		}
		if err := checkVectorCastDim(toType, len(arr)); err != nil {
			return err
		}
		var err error
		if cell, err = types.AppendBlockScaled(cell[:0], f, arr); err != nil {
			return err
		}
		if err := rs.AppendBytes(cell, false); err != nil {
			return err
		}
	}
	return nil
}

// blockScaledToOthers casts a vecf8/vecf4 source to vecf32.
func blockScaledToOthers(proc *process.Process, from *vector.Vector, toType types.Type,
	result vector.FunctionResultWrapper, length int, selectList *FunctionSelectList) error {
	if toType.Oid != types.T_array_float32 {
		return moerr.NewInternalError(proc.Ctx, fmt.Sprintf("unsupported cast from %s to %s", from.GetType(), toType))
	}
	src := vector.GenerateFunctionStrParameter(from)
	rs := vector.MustFunctionResult[types.Varlena](result)
	var arr []float32
	for i := uint64(0); i < uint64(length); i++ {
		v, null := src.GetStrValue(i)
		if null || castRowSkipped(selectList, i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		c, err := types.ParseBlockScaledCell(v)
		if err != nil {
			return err
		}
		if err := checkVectorCastDim(toType, c.Dim); err != nil {
			return err
		}
		arr = append(arr[:0], make([]float32, c.Dim)...)
		c.Dequantize(arr)
		if err := rs.AppendBytes(types.ArrayToBytes(arr), false); err != nil {
			return err
		}
	}
	return nil
}
