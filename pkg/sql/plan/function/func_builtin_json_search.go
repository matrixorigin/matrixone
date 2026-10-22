// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	jsonSearchMaxResultPaths = 1 << 20
	jsonSearchMaxResultBytes = types.MaxBlobLen
)

func jsonSearchTypeSupported(typ types.Type, document bool) bool {
	if typ.Oid == types.T_any {
		return true
	}
	if document && typ.Oid == types.T_json {
		return true
	}
	return typ.Oid.IsMySQLString()
}

func jsonSearchCheckFn(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) < 3 {
		return newCheckResultWithFailure(failedFunctionParametersWrong)
	}

	castTypes := make([]types.Type, 0, len(inputs))
	needCast := false
	for i, input := range inputs {
		if !jsonSearchTypeSupported(input, i == 0) {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
		if input.Oid == types.T_any {
			input = types.T_varchar.ToType()
			needCast = true
		}
		castTypes = append(castTypes, input)
	}
	if needCast {
		return newCheckResultWithCast(0, castTypes)
	}
	return newCheckResultWithSuccess(0)
}

func jsonSearchCheckPreparedInputs(
	inputs []*vector.Vector,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	for argument, input := range inputs {
		if input == nil || selectList != nil && selectList.IgnoreAllRow() {
			continue
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
			if input.GetIsBinaryStringAt(physicalRow) {
				return moerr.NewInvalidJSONCharset(proc.Ctx, "binary")
			}
			if preparedType != types.T_any && !jsonSearchTypeSupported(preparedType.ToType(), argument == 0) {
				return moerr.NewInvalidArg(proc.Ctx, "json_search argument", argument+1)
			}
			if preparedType == types.T_any &&
				input.GetPrepareParamKindAt(physicalRow) != vector.PrepareParamNone {
				return moerr.NewInvalidArg(proc.Ctx, "json_search argument", argument+1)
			}
		}
	}
	return nil
}

func jsonSearchParseDocument(
	proc *process.Process,
	input *vector.Vector,
	raw []byte,
) (bytejson.ByteJson, error) {
	if input.GetType().Oid == types.T_json {
		document := types.DecodeJson(raw)
		if !bytejson.IsValidByteJson(document) {
			return bytejson.Null, moerr.NewInvalidArg(proc.Ctx, "json_search", "invalid JSON document")
		}
		if err := bytejson.ValidateJSONDocumentDepth(document); err != nil {
			return bytejson.Null, err
		}
		return document, nil
	}
	document, err := types.ParseSliceToByteJsonWithDepthLimit(raw, bytejson.JSONDocumentMaxNestingDepth)
	if err != nil {
		if bytejson.IsJSONDocumentDepthError(err) {
			return bytejson.Null, err
		}
		return bytejson.Null, moerr.NewInvalidArg(proc.Ctx, "json_search", "invalid JSON document")
	}
	return document, nil
}

func jsonSearchEscape(
	parameters []*vector.Vector,
	proc *process.Process,
) (rune, bool, error) {
	if len(parameters) < 4 {
		if likeNoBackslashEscapes(proc) {
			return 0, false, nil
		}
		return DefaultEscapeChar, true, nil
	}

	input := parameters[3]
	if !input.IsConst() {
		return 0, false, moerr.NewWrongArguments(proc.Ctx, "json_search")
	}
	if input.IsConstNull() {
		return DefaultEscapeChar, true, nil
	}
	value, null := vector.GenerateFunctionStrParameter(input).GetStrValue(0)
	if null {
		return DefaultEscapeChar, true, nil
	}
	if !utf8.Valid(value) || utf8.RuneCount(value) > 1 {
		return 0, false, moerr.NewWrongArguments(proc.Ctx, "json_search")
	}
	if len(value) == 0 {
		if likeNoBackslashEscapes(proc) {
			return 0, false, moerr.NewWrongArguments(proc.Ctx, "json_search")
		}
		return 0, false, nil
	}
	escape, _ := utf8.DecodeRune(value)
	return escape, true, nil
}

func JSONSearch(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	if len(parameters) < 3 {
		return moerr.NewWrongArguments(proc.Ctx, "json_search")
	}
	if err := jsonSearchCheckPreparedInputs(parameters, proc, length, selectList); err != nil {
		return err
	}

	documentWrapper := vector.GenerateFunctionStrParameter(parameters[0])
	modeWrapper := vector.GenerateFunctionStrParameter(parameters[1])
	termWrapper := vector.GenerateFunctionStrParameter(parameters[2])
	pathStart := 3
	if len(parameters) >= 4 {
		pathStart = 4
	}
	pathWrappers := make([]vector.FunctionParameterWrapper[types.Varlena], len(parameters)-pathStart)
	for i := range pathWrappers {
		pathWrappers[i] = vector.GenerateFunctionStrParameter(parameters[i+pathStart])
	}
	rs := vector.MustFunctionResult[types.Varlena](result)
	regexpSet := newOpBuiltInRegexp().regMap

	if selectList != nil && selectList.IgnoreAllRow() {
		for row := 0; row < length; row++ {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		return nil
	}

	escape, escapeEnabled, err := jsonSearchEscape(parameters, proc)
	if err != nil {
		return err
	}
	for row := 0; row < length; row++ {
		if functionRowSkipped(selectList, uint64(row)) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		rawDocument, documentNull := documentWrapper.GetStrValue(uint64(row))
		if documentNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		document, err := jsonSearchParseDocument(proc, parameters[0], rawDocument)
		if err != nil {
			return err
		}

		rawMode, modeNull := modeWrapper.GetStrValue(uint64(row))
		if modeNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if !utf8.Valid(rawMode) {
			return moerr.NewInvalidArg(proc.Ctx, "json_search", "one_or_all must be one or all")
		}
		modeText := string(rawMode)
		var one bool
		switch {
		case strings.EqualFold(modeText, "one"):
			one = true
		case strings.EqualFold(modeText, "all"):
		default:
			return moerr.NewWrongArguments(proc.Ctx, "json_search")
		}

		term, termNull := termWrapper.GetStrValue(uint64(row))
		if termNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if !utf8.Valid(term) {
			return moerr.NewInvalidArg(proc.Ctx, "json_search", "invalid search string")
		}

		paths := make([]*bytejson.Path, 0, max(1, len(pathWrappers)))
		if len(pathWrappers) == 0 {
			paths = append(paths, bytejson.NewRecursiveSearchPath())
		} else {
			for _, wrapper := range pathWrappers {
				rawPath, pathNull := wrapper.GetStrValue(uint64(row))
				if pathNull {
					paths = nil
					break
				}
				if !utf8.Valid(rawPath) {
					return moerr.NewInvalidArg(proc.Ctx, "json_search", "invalid path expression")
				}
				path, parseErr := types.ParseStringToPath(string(rawPath))
				if parseErr != nil {
					return parseErr
				}
				paths = append(paths, &path)
			}
			if paths == nil {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
		}

		pattern, err := regexpSet.prepareLikeRegexp(term, escape, escapeEnabled, false)
		if err != nil {
			return err
		}
		if pattern == nil {
			return moerr.NewInvalidArg(proc.Ctx, "json_search", "invalid search pattern")
		}
		locations, err := document.SearchStringPaths(
			proc.Ctx,
			paths,
			func(value []byte) (bool, error) {
				return pattern.Match(value), nil
			},
			one,
			jsonSearchMaxResultPaths,
			jsonSearchMaxResultBytes,
		)
		if err != nil {
			return err
		}
		if len(locations) == 0 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		var output bytejson.ByteJson
		if one {
			output, err = bytejson.CreateByteJSON(locations[0])
		} else {
			values := make([]any, len(locations))
			for i := range locations {
				values[i] = locations[i]
			}
			output, err = bytejson.CreateByteJSON(values)
		}
		if err != nil {
			return err
		}
		encoded, err := types.EncodeJson(output)
		if err != nil {
			return err
		}
		if len(encoded) > jsonSearchMaxResultBytes {
			return moerr.NewInvalidInput(proc.Ctx, "JSON_SEARCH result exceeds the byte limit")
		}
		if err := rs.AppendBytes(encoded, false); err != nil {
			return err
		}
	}
	return nil
}
