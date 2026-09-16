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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// internalCollationKey executes a schema-resolved PAD SPACE v1 key expression.
// The constant second argument comes from the effective comparison domain in
// the plan, not from any bytes stored in an index. The binary result can be fed
// to ordinary serial without changing the legacy value-serialization contract.
func internalCollationKey(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	if selectList != nil && selectList.IgnoreAllRow() {
		rs.SetNullResult(uint64(length))
		return nil
	}
	if len(parameters) != 2 || parameters[1].GetType().Oid != types.T_uint64 || !parameters[1].IsConst() || parameters[1].IsConstNull() {
		return moerr.NewInvalidInput(proc.Ctx, "collation key requires a constant schema charset")
	}
	switch parameters[0].GetType().Oid {
	case types.T_char, types.T_varchar, types.T_text, types.T_binary, types.T_varbinary, types.T_blob:
	default:
		return moerr.NewInvalidInput(proc.Ctx, "collation key requires a string operand")
	}
	charset := vector.GetFixedAtNoTypeCheck[uint64](parameters[1], 0)
	if charset > uint64(types.CharsetUTF8MB40900Bin) {
		return moerr.NewInvalidInput(proc.Ctx, "unsupported collation key charset")
	}
	// Resolve against text: the target domain is explicit even when a prepared
	// parameter arrives with binary protocol metadata. No width cast is applied.
	inputType := *parameters[0].GetType()
	switch inputType.Oid {
	case types.T_binary, types.T_varbinary, types.T_blob:
		// Binary protocol parameters are commonly represented as VARBINARY/BLOB.
		// An explicit COLLATE in the plan still gives them a text comparison
		// domain; resolving the original binary OID would silently select the raw
		// path and store a key that cannot match the table's collation.
		inputType.Oid = types.T_varchar
	}
	// The plan carries the semantic version on the source expression.  The
	// charset argument remains the compact, backwards-compatible wire contract;
	// never infer a new version from the historical collation name alone.
	inputType.Charset = uint8(charset)
	inputType.CollationVersion = types.CollationVersionV1
	part, err := types.ResolveStringKeyPart(inputType, types.PADSpaceKeyV1)
	if err != nil {
		return err
	}
	input := vector.GenerateFunctionStrParameter(parameters[0])
	var scratch []byte
	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && !selectList.ShouldEvalAllRow() && selectList.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		value, isNull := input.GetStrValue(i)
		if isNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		key, err := part.Key(scratch, value)
		if err != nil {
			return moerr.NewInvalidInputf(proc.Ctx, "collation key: %v", err)
		}
		// AppendBytes copies the row before scratch is reused.
		if err := rs.AppendBytes(key, false); err != nil {
			return err
		}
		if part.Transformed() {
			scratch = key
		}
	}
	return nil
}
