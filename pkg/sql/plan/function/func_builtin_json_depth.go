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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// jsonDepthStringTypeSupported describes the text transport accepted by
// JSON_DEPTH. Binary string domains are opaque values, even when their OID is
// text-shaped, and must not be parsed as JSON documents.
func jsonDepthStringTypeSupported(typ types.Type) bool {
	switch typ.Oid {
	case types.T_char, types.T_varchar, types.T_text:
		return types.StaticStringDomain(typ) != types.StringDomainBinary
	default:
		return false
	}
}

func jsonDepthTypeSupported(typ types.Type) bool {
	return typ.Oid == types.T_json || typ.Oid == types.T_any ||
		jsonDepthStringTypeSupported(typ)
}

func jsonDepthInvalidType(proc *process.Process) error {
	ctx := context.Background()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	return moerr.NewInvalidTypeForJSON(ctx, 1, "json_depth")
}

func jsonDepthInvalidBinary(proc *process.Process) error {
	ctx := context.Background()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	return moerr.NewInvalidJSONCharset(ctx, "binary")
}

func jsonDepthCheckFn(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) != 1 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}
	if inputs[0].Oid == types.T_any {
		return newCheckResultWithCast(0, []types.Type{types.T_varchar.ToType()})
	}
	if jsonDepthTypeSupported(inputs[0]) {
		return newCheckResultWithSuccess(0)
	}
	return newCheckResultWithInvalidJSONArgument(1)
}

// jsonDepthCheckPreparedInput preserves the concrete source domain of a
// prepared parameter after its text wire transport. The executor sees only
// bytes, so numeric, boolean, binary, and other non-text sources are rejected
// before those bytes can be interpreted as JSON text. SQL NULL is still
// accepted and is handled by the ordinary strict-function NULL path.
func jsonDepthCheckPreparedInput(
	input *vector.Vector,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	if input == nil || selectList != nil && selectList.IgnoreAllRow() {
		return nil
	}

	preparedType := input.GetPrepareParamType()
	for row := 0; row < length; row++ {
		if selectList != nil && selectList.Contains(uint64(row)) {
			continue
		}
		physicalRow := row
		if input.IsConst() {
			physicalRow = 0
		}
		if input.IsNull(uint64(physicalRow)) {
			continue
		}

		if !jsonDepthTypeSupported(*input.GetType()) {
			return jsonDepthInvalidType(proc)
		}
		if input.GetIsBinaryStringAt(physicalRow) {
			return jsonDepthInvalidBinary(proc)
		}
		if preparedType != types.T_any {
			if !jsonDepthTypeSupported(preparedType.ToType()) {
				return jsonDepthInvalidType(proc)
			}
			continue
		}
		if input.GetPrepareParamKindAt(physicalRow) != vector.PrepareParamNone {
			return jsonDepthInvalidType(proc)
		}
	}
	return nil
}

func JsonDepth(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	if err := jsonDepthCheckPreparedInput(ivecs[0], proc, length, selectList); err != nil {
		return err
	}
	checkCancelled := func() error {
		if proc == nil || proc.Ctx == nil {
			return nil
		}
		return proc.Ctx.Err()
	}

	depth := func(value []byte) (int64, error) {
		if err := checkCancelled(); err != nil {
			return 0, err
		}
		var (
			document bytejson.ByteJson
			err      error
		)
		if ivecs[0].GetType().Oid == types.T_json {
			document = types.DecodeJson(value)
			if !bytejson.IsValidByteJson(document) {
				return 0, jsonStorageInvalidArg(proc, "json_depth")
			}
			err = bytejson.ValidateJSONDocumentDepth(document)
		} else {
			document, err = types.ParseSliceToByteJsonWithDepthLimit(
				value,
				bytejson.JSONDocumentMaxNestingDepth,
			)
		}
		if err := checkCancelled(); err != nil {
			return 0, err
		}
		if err != nil {
			if bytejson.IsJSONDocumentDepthError(err) {
				return 0, err
			}
			return 0, jsonStorageInvalidArg(proc, "json_depth")
		}
		valueDepth, err := document.DepthWithCheck(checkCancelled)
		if err != nil {
			return 0, err
		}
		return int64(valueDepth), nil
	}

	return opUnaryBytesToFixedWithErrorCheck[int64](
		ivecs,
		result,
		proc,
		length,
		depth,
		selectList,
	)
}
