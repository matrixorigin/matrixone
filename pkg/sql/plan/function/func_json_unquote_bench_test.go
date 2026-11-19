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
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// jsonUnquoteBeforeBorrowedString is the implementation from 730437ed46,
// retained here to compare the complete vector path with the same append APIs.
func jsonUnquoteBeforeBorrowedString(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	jsonSingle := func(v []byte) (string, error) {
		bj := types.DecodeJson(v)
		return bj.Unquote()
	}

	stringSingle := func(v []byte) (string, error) {
		if !utf8.Valid(v) {
			return "", moerr.NewInvalidInput(proc.Ctx, "invalid utf-8 string for json_unquote")
		}
		if len(v) < 2 || v[0] != '"' || v[len(v)-1] != '"' {
			return string(v), nil
		}
		bj, err := types.ParseSliceToByteJson(v)
		if err != nil {
			return "", err
		}
		return bj.Unquote()
	}

	single := func(v []byte, row int) (string, error) {
		// Evaluate the selected row's domain after the template has filtered
		// NULL and masked rows.  Runtime binary provenance can vary within a
		// VARCHAR vector, and a selected text row can override a static binary
		// result type.
		if ivecs[0].GetIsBinaryStringAt(row) {
			return "", moerr.NewInvalidInput(proc.Ctx, "binary data not supported by json_unquote")
		}
		if ivecs[0].GetType().Oid.IsMySQLString() {
			return stringSingle(v)
		}
		return jsonSingle(v)
	}

	return opUnaryBytesToStrWithRowErrorCheck(ivecs, result, length, single, selectList)
}

func BenchmarkJsonUnquote(b *testing.B) {
	for _, scenario := range []struct {
		name     string
		document string
		wanted   string
		rows     int
		text     bool
		constant bool
		alsoCold bool
	}{
		{name: "Native8", document: strconv.Quote("stable_a"), wanted: "stable_a", rows: 8192, alsoCold: true},
		{name: "Native23", document: strconv.Quote(strings.Repeat("a", 23)), wanted: strings.Repeat("a", 23), rows: 8192},
		{name: "Native24", document: strconv.Quote(strings.Repeat("a", 24)), wanted: strings.Repeat("a", 24), rows: 8192},
		{name: "Native64", document: strconv.Quote(strings.Repeat("a", 64)), wanted: strings.Repeat("a", 64), rows: 8192, alsoCold: true},
		{name: "Native1KiB", document: strconv.Quote(strings.Repeat("a", 1024)), wanted: strings.Repeat("a", 1024), rows: 8192, alsoCold: true},
		{name: "NativeEmpty", document: `""`, rows: 8192},
		{name: "NativeOneRow", document: `"stable_a"`, wanted: "stable_a", rows: 1},
		{name: "NativeConstant", document: strconv.Quote(strings.Repeat("a", 64)), wanted: strings.Repeat("a", 64), rows: 8192, constant: true},
		{name: "NumberFallback", document: "123", wanted: "123", rows: 8192},
		{name: "ObjectFallback", document: `{"a":1}`, wanted: `{"a": 1}`, rows: 8192},
		{name: "TextPassthrough", document: "stable_a", wanted: "stable_a", rows: 8192, text: true},
		{name: "TextQuoted", document: `"stable_a"`, wanted: "stable_a", rows: 8192, text: true},
	} {
		for _, implementation := range []struct {
			name string
			fn   executeLogicOfOverload
		}{
			{"Original", jsonUnquoteBeforeBorrowedString}, {"Candidate", JsonUnquote},
		} {
			modes := []bool{false}
			if scenario.alsoCold {
				modes = append(modes, true)
			}
			for _, cold := range modes {
				mode := "ReusedResult"
				if cold {
					mode = "FreshResult"
				}
				b.Run(scenario.name+"/"+mode+"/"+implementation.name, func(b *testing.B) {
					typ := types.T_json.ToType()
					input := scenario.document
					if scenario.text {
						typ = types.T_varchar.ToType()
					} else {
						bj, err := types.ParseStringToByteJson(input)
						if err != nil {
							b.Fatal(err)
						}
						encoded, err := bj.Marshal()
						if err != nil {
							b.Fatal(err)
						}
						input = string(encoded)
					}
					inputRows := scenario.rows
					if scenario.constant {
						inputRows = 1
					}
					values, wanted := make([]string, inputRows), make([]string, scenario.rows)
					for row := range values {
						values[row] = input
					}
					for row := range wanted {
						wanted[row] = scenario.wanted
					}
					parameter := NewFunctionTestInput(typ, values, nil)
					if scenario.constant {
						parameter = NewFunctionTestConstInput(typ, values, nil)
					}
					proc := testutil.NewProcess(b)
					resultType := types.T_varchar.ToType()
					fc := NewFunctionTestCase(proc, []FunctionTestInput{parameter}, NewFunctionTestResult(resultType, false, wanted, nil), implementation.fn)
					defer fc.Free()
					fc.fnLength = scenario.rows
					fc.parameters[0].SetLength(scenario.rows)
					b.SetBytes(int64(scenario.rows * len(scenario.wanted)))
					if !cold {
						fc.Benchmark(b)
						return
					}
					if ok, info := fc.Run(); !ok {
						b.Fatal(info)
					}
					b.ReportAllocs()
					for b.Loop() {
						fc.result.Free()
						fc.result = vector.NewFunctionResultWrapper(resultType, proc.Mp())
						if err := fc.result.PreExtendAndReset(scenario.rows); err != nil {
							b.Fatal(err)
						}
						if err := implementation.fn(fc.parameters, fc.result, proc, scenario.rows, nil); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
