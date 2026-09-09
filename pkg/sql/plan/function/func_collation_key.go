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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/collationkey"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// collationKeyV2TypeMatch is intentionally stricter than fixedTypeMatch. The
// key primitive is planner-owned and must never silently cast an input into a
// different storage identity. T_text is accepted alongside T_varchar because
// both carry the same varlena bytes; the descriptor arguments are always
// planner materialized int64 constants.
func collationKeyV2TypeMatch(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) != 3 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	if !isCollationKeyTextType(inputs[0].Oid) ||
		inputs[1].Oid != types.T_int64 || inputs[2].Oid != types.T_int64 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	return newCheckResultWithSuccess(0)
}

func collationKeyV2ReturnType(_ []types.Type) types.Type {
	result := types.T_blob.ToType()
	result.Width = types.MaxBlobLen
	result.Charset = types.CharsetBinary
	return result
}

func collationKeyV2Charset(value int64) (uint8, bool) {
	switch value {
	case int64(types.CharsetUTF8):
		return collationkey.CharsetUTF8, true
	case int64(types.CharsetUTF8MB4Bin):
		return collationkey.CharsetUTF8MB4Bin, true
	default:
		return 0, false
	}
}

// BuiltInCollationKeyV2 materializes one v2 text identity envelope per row.
//
// This primitive is deliberately not a SQL function: it has no entry in
// functionIdRegister and is only reachable from a planner expression created
// after the relation capability/activation checks. It is nevertheless kept in
// the normal vector execution framework so future sidecar writers and
// distributed plans can share exactly the same implementation and NULL/error
// behavior. Until those consumers exist, v2 relation admission rejects plans
// before this operator can be reached.
func BuiltInCollationKeyV2(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	_ *FunctionSelectList,
) error {
	if len(parameters) != 3 || parameters[0] == nil || parameters[1] == nil || parameters[2] == nil {
		return moerr.NewInvalidInput(proc.Ctx, "collation key v2 expects value, prefix, and charset")
	}
	valueType := parameters[0].GetType()
	if valueType == nil || !isCollationKeyTextType(valueType.Oid) {
		return moerr.NewInvalidInput(proc.Ctx, "collation key v2 expects VARCHAR or TEXT")
	}
	if parameters[1].GetType().Oid != types.T_int64 || parameters[2].GetType().Oid != types.T_int64 {
		return moerr.NewInvalidInput(proc.Ctx, "collation key v2 descriptor arguments must be INT64")
	}

	values := vector.GenerateFunctionStrParameter(parameters[0])
	prefixes := vector.GenerateFunctionFixedTypeParameter[int64](parameters[1])
	charsets := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
	rs := vector.MustFunctionResult[types.Varlena](result)

	for row := uint64(0); row < uint64(length); row++ {
		prefix, prefixNull := prefixes.GetValue(row)
		charsetValue, charsetNull := charsets.GetValue(row)
		if prefixNull || charsetNull {
			return moerr.NewInvalidInput(proc.Ctx, "collation key v2 descriptor cannot be NULL")
		}
		if prefix < 0 || uint64(prefix) > math.MaxUint32 {
			return moerr.NewInvalidInputf(proc.Ctx, "collation key v2 prefix %d is out of range", prefix)
		}
		charset, ok := collationKeyV2Charset(charsetValue)
		if !ok {
			return moerr.NewInvalidInputf(proc.Ctx, "collation key v2 charset %d is unsupported", charsetValue)
		}
		if valueType.Charset != uint8(charsetValue) {
			return moerr.NewInvalidInputf(proc.Ctx, "collation key v2 value charset %d does not match descriptor %d", valueType.Charset, charsetValue)
		}

		value, valueNull := values.GetStrValue(row)
		if valueNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		domain := collationkey.Domain{
			Type:    collationkey.Text,
			Charset: charset,
			Prefix:  uint32(prefix),
			Unit:    collationkey.PrefixCharacters,
		}
		encoded, err := collationkey.EncodePart(nil, collationkey.Part{Domain: domain, Value: value})
		if err != nil {
			return err
		}
		if err := rs.AppendBytes(encoded, false); err != nil {
			return err
		}
	}
	return nil
}
