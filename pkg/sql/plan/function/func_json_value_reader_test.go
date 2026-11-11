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
	"encoding/binary"
	"runtime"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJsonValueReaderExecutorIsolation(t *testing.T) {
	for _, contract := range []bool{false, true} {
		name := "legacy"
		if contract {
			name = "contract"
		}
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			raw := mustJsonBinaryString(t, `{"keep":1,"large":[0,0]}`)
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{raw}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"$.keep"}, nil),
			}
			expected := NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"1"}, nil)
			if contract {
				inputs = append(inputs,
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueErrorResponse}, nil),
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueErrorResponse}, nil),
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}))
				expected = NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil)
			}
			fc := NewFunctionTestCase(proc, inputs, expected, JsonValue)
			defer fc.Free()
			reader, err := vector.NewJSONReaderSnapshot(fc.parameters[0], proc.Mp())
			require.NoError(t, err)
			defer reader.Close()
			root := types.DecodeJson(fc.parameters[0].GetBytesAt(0))
			binary.LittleEndian.PutUint64(root.GetObjectVal(0).Data, 2)
			root.GetObjectVal(1).Data[8+5] = 0xfd
			require.NoError(t, fc.result.PreExtendAndReset(1))
			require.NoError(t, JsonValueWithReader(reader, fc.parameters, fc.result, proc, 1, nil))
			if contract {
				require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](fc.result.GetResultVector())[0])
			} else {
				require.Equal(t, "1", string(fc.result.GetResultVector().GetBytesAt(0)))
			}
			require.NoError(t, fc.result.PreExtendAndReset(1))
			require.Error(t, JsonValue(fc.parameters, fc.result, proc, 1, nil), "ordinary mutable input must still reject the corrupt descendant")
			_, err = vector.NewJSONReaderSnapshot(fc.parameters[0], proc.Mp())
			require.Error(t, err)
			require.NoError(t, vector.SetBytesAt(fc.parameters[0], 0, []byte(raw), proc.Mp()))
			descriptor := &types.DecodeSlice[types.Varlena](fc.parameters[0].GetData())[0]
			saved := *descriptor
			descriptor.SetOffsetLen(uint32(len(fc.parameters[0].GetArea())+1), 32)
			require.NoError(t, fc.result.PreExtendAndReset(1))
			require.Error(t, JsonValue(fc.parameters, fc.result, proc, 1, nil), "corrupt descriptor must be a hard error")
			*descriptor = saved
			require.NoError(t, fc.result.PreExtendAndReset(1))
			require.NoError(t, JsonValue(fc.parameters, fc.result, proc, 1, nil), "restored input must recover")
			original := proc.Ctx
			canceled, cancel := context.WithCancel(original)
			cancel()
			originalTop := proc.GetTopContext()
			originalRuntime := proc.GetBaseProcessRunningStatus()
			proc.Ctx = canceled
			proc.ReplaceTopCtx(original)
			proc.SetBaseProcessRunningStatus(false)
			defer func() {
				proc.Ctx = original
				proc.ReplaceTopCtx(originalTop)
				proc.SetBaseProcessRunningStatus(originalRuntime)
			}()
			require.NoError(t, fc.result.PreExtendAndReset(1))
			require.NoError(t, JsonValue(fc.parameters, fc.result, proc, 1, nil), "binding must ignore an ended pipeline")
			proc.SetBaseProcessRunningStatus(true)
			require.ErrorIs(t, JsonValue(fc.parameters, fc.result, proc, 1, nil), context.Canceled)
			require.ErrorIs(t, JsonValueWithReader(reader, fc.parameters, fc.result, proc, 1, nil), context.Canceled)

		})
	}
}

func TestJSONConstructorReaderPartialFailure(t *testing.T) {
	proc := testutil.NewProcess(t)
	fc := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"keep", "unused"}, []bool{false, true}),
		NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 2}, nil),
	}, NewFunctionTestResult(types.T_json.ToType(), false, []string{}, nil), JsonValue)
	defer fc.Free()
	before := proc.Mp().CurrNB()
	reader, err := EvalJSONConstructorReader(JSON_OBJECT, fc.parameters, proc, 2, nil, nil)
	require.ErrorContains(t, err, "NULL member names")
	require.Nil(t, reader)
	require.Equal(t, before, proc.Mp().CurrNB(), "completed prefix must be released on a later row error")
}

func TestJSONCastReaderIsolationAndPartialFailure(t *testing.T) {
	for _, oid := range []types.T{types.T_varchar, types.T_json} {
		t.Run(oid.String(), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer func() { require.Zero(t, proc.Mp().CurrNB()) }()
			source := vector.NewVec(oid.ToType())
			defer source.Free(proc.Mp())
			target := vector.NewConstNull(types.T_json.ToType(), 1, proc.Mp())
			defer target.Free(proc.Mp())
			for _, text := range []string{`{"keep":1}`, `{}`} {
				if oid == types.T_json {
					text = mustJsonBinaryString(t, text)
				}
				require.NoError(t, vector.AppendBytes(source, []byte(text), false, proc.Mp()))
			}
			require.NoError(t, vector.AppendBytes(source, nil, true, proc.Mp()))
			params := []*vector.Vector{source, target}
			reader, err := EvalJSONConstructorReader(CAST, params, proc, 3, nil, nil)
			require.NoError(t, err)
			defer reader.Close()
			path, err := types.ParseStringToPath("$.keep")
			require.NoError(t, err)
			check := func(reader vector.JSONReader, want string) {
				t.Helper()
				value, err := reader.Query(0, &path)
				require.NoError(t, err)
				defer value.Close()
				text, err := value.Text()
				require.NoError(t, err)
				require.Equal(t, want, text)
			}
			raw := source.GetBytesAt(0)
			if oid == types.T_json {
				binary.LittleEndian.PutUint64(raw[len(raw)-8:], 9)
			} else {
				raw[len(raw)-2] = '9'
			}
			check(reader, "1")
			updated, err := EvalJSONConstructorReader(CAST, params, proc, 3, nil, nil)
			require.NoError(t, err)
			check(updated, "9")
			updated.Close()
			isNull, err := reader.IsNull(2)
			require.NoError(t, err)
			require.True(t, isNull)
			// Failure after an already copied row must release that private prefix.
			source.GetBytesAt(1)[0] = 0xff
			before := proc.Mp().CurrNB()
			failed, err := EvalJSONConstructorReader(CAST, params, proc, 3, nil, nil)
			require.Error(t, err)
			require.Nil(t, failed)
			require.Equal(t, before, proc.Mp().CurrNB())
			check(reader, "1")
		})
	}
}

func TestJSONValueReaderPathNullOrdering(t *testing.T) {
	for _, mode := range []struct {
		name     string
		contract bool
		text     bool
	}{{"legacy", false, true}, {"text-contract", true, true}, {"typed-contract", true, false}} {
		for _, scenario := range []string{"null-document", "null-path", "both-null", "invalid-path"} {
			t.Run(mode.name+"/"+scenario, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				docNull := scenario == "null-document" || scenario == "both-null"
				pathNull := scenario == "null-path" || scenario == "both-null"
				raw := mustJsonBinaryString(t, `{"keep":1}`)
				inputs := []FunctionTestInput{
					NewFunctionTestInput(types.T_json.ToType(), []string{raw}, []bool{docNull}),
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"$["}, []bool{pathNull}),
				}
				expected := NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true})
				if mode.contract {
					nullValue := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true})
					if mode.text {
						nullValue = NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true})
					}
					inputs = append(inputs,
						nullValue,
						NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueNullResponse}, nil),
						nullValue,
						NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueNullResponse}, nil),
						nullValue)
					if !mode.text {
						expected = NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true})
					}
				}
				fc := NewFunctionTestCase(proc, inputs, expected, JsonValue)
				defer fc.Free()
				reader, err := vector.NewJSONReaderSnapshot(fc.parameters[0], proc.Mp())
				require.NoError(t, err)
				defer reader.Close()
				if scenario == "null-path" {
					fc.parameters[0].GetBytesAt(0)[0] = 0xfd
				}
				for _, trusted := range []bool{false, true} {
					require.NoError(t, fc.result.PreExtendAndReset(1))
					if trusted {
						err = JsonValueWithReader(reader, fc.parameters, fc.result, proc, 1, nil)
					} else {
						err = JsonValue(fc.parameters, fc.result, proc, 1, nil)
					}
					wantError := !pathNull && (mode.contract || !docNull)
					if wantError {
						require.ErrorContains(t, err, "invalid path expression")
					} else {
						require.NoError(t, err)
						require.True(t, fc.result.GetResultVector().IsNull(0))
					}
				}
			})
		}
	}
}

func TestJSONValueReaderTextContractMatchesMutableInput(t *testing.T) {
	boundary := strings.Repeat("猫", 512)
	long := strings.Repeat("猫", 513)
	for _, test := range []struct {
		name, document, path, want string
		width                      int32
		implicit, nullDoc          bool
		wantNull, wantError        bool
		jsonOutput                 bool
	}{
		{"value", `{"keep":"value"}`, "$.keep", "value", 512, true, false, false, false, false},
		{"empty-default", `{"keep":1}`, "$.missing", "empty", 512, true, false, false, false, false},
		{"multiple-default", `{"items":[1,2]}`, "$.items[*]", "fallback", 512, true, false, false, false, false},
		{"composite-implicit", `{"obj":{"x":1}}`, "$.obj", `{"x":1}`, 512, true, false, false, false, true},
		{"composite-explicit", `{"obj":{"x":1}}`, "$.obj", "fallback", 10, false, false, false, false, false},
		{"json-null", `{"keep":null}`, "$.keep", "", 512, true, false, true, false, false},
		{"sql-null", `{"keep":1}`, "$.keep", "", 512, true, true, true, false, false},
		{"invalid-path", `{"keep":1}`, "$[", "", 512, true, false, false, true, false},
		{"null-invalid-path", `{"keep":1}`, "$[", "", 512, true, true, false, true, false},
		{"width-512", `{"keep":"` + boundary + `"}`, "$.keep", boundary, 512, true, false, false, false, false},
		{"width-513", `{"keep":"` + long + `"}`, "$.keep", "fallback", 512, true, false, false, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			target := types.New(types.T_varchar, test.width, 0)
			if test.implicit {
				target = types.NewWithCharset(types.T_varchar, 512, 0, types.CharsetUTF8MB4Bin)
			}
			raw := mustJsonBinaryString(t, test.document)
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{raw}, []bool{test.nullDoc}),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{test.path}, nil),
				NewFunctionTestConstInput(target, []string{""}, []bool{true}),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueDefaultResponse}, nil),
				NewFunctionTestConstInput(target, []string{"empty"}, nil),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueDefaultResponse}, nil),
				NewFunctionTestConstInput(target, []string{"fallback"}, nil),
			}
			fc := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(target, false, []string{""}, nil), JsonValue)
			defer fc.Free()
			reader, err := vector.NewJSONReaderSnapshot(fc.parameters[0], proc.Mp())
			require.NoError(t, err)
			defer reader.Close()
			type outcome struct {
				text, failure string
				null          bool
			}
			run := func(immutable bool) outcome {
				t.Helper()
				require.NoError(t, fc.result.PreExtendAndReset(1))
				var e error
				if immutable {
					e = JsonValueWithReader(reader, fc.parameters, fc.result, proc, 1, nil)
				} else {
					e = JsonValue(fc.parameters, fc.result, proc, 1, nil)
				}
				if e != nil {
					return outcome{failure: e.Error()}
				}
				result := fc.result.GetResultVector()
				if result.IsNull(0) {
					return outcome{null: true}
				}
				return outcome{text: string(result.GetBytesAt(0))}
			}
			ordinary := run(false)
			frozen := run(true)
			require.Equal(t, ordinary, frozen)
			if test.wantError {
				require.Contains(t, frozen.failure, "invalid path expression")
			} else if test.wantNull {
				require.True(t, frozen.null)
			} else if test.jsonOutput {
				require.JSONEq(t, test.want, frozen.text)
			} else {
				require.Equal(t, test.want, frozen.text)
			}
		})
	}
}

func TestJSONValueReaderTextOutputCapacityFailureIsTerminal(t *testing.T) {
	proc := testutil.NewProcess(t)
	target := types.NewWithCharset(types.T_varchar, 512, 0, types.CharsetUTF8MB4Bin)
	payload := strings.Repeat("x", 128)
	raw := mustJsonBinaryString(t, `{"a":"`+payload+`"}`)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_json.ToType(), []string{raw}, nil),
		NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"$.a"}, nil),
		NewFunctionTestConstInput(target, []string{""}, []bool{true}),
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueNullResponse}, nil),
		NewFunctionTestConstInput(target, []string{""}, []bool{true}),
		NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueNullResponse}, nil),
		NewFunctionTestConstInput(target, []string{""}, []bool{true}),
	}
	fc := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(target, false, []string{payload}, nil), JsonValue)
	defer fc.Free()
	reader, err := vector.NewJSONReaderSnapshot(fc.parameters[0], proc.Mp())
	require.NoError(t, err)
	defer reader.Close()
	const capacity = int64(1 << 20)
	mp, err := mpool.NewMPool(t.Name(), capacity, mpool.NoLock)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)
	result := vector.NewFunctionResultWrapper(target, mp)
	var filler []byte
	defer func() { mp.Free(filler); result.Free(); require.Zero(t, mp.CurrNB()) }()
	require.NoError(t, result.PreExtendAndReset(1))
	filler, err = mp.Alloc(int(capacity-mp.CurrNB()), true)
	require.NoError(t, err)
	err = JsonValueWithReader(reader, fc.parameters, result, proc, 1, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrMPoolCapacity), "output allocation failure must remain terminal")
	mp.Free(filler)
	filler = nil
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, JsonValueWithReader(reader, fc.parameters, result, proc, 1, nil))
	reader.Close()
	require.Equal(t, payload, string(result.GetResultVector().GetBytesAt(0)))
}

type cancelDuringJSONQueryReader struct {
	vector.JSONReader
	cancel context.CancelFunc
}

func (r *cancelDuringJSONQueryReader) QueryText(row int, path []byte, legacyFloat bool) (vector.JSONTextSelection, error) {
	selected, err := r.JSONReader.QueryText(row, path, legacyFloat)
	if err == nil {
		r.cancel()
	}
	return selected, err
}

func (r *cancelDuringJSONQueryReader) QueryTextAtPath(row int, path bytejson.Path, legacyFloat bool) (vector.JSONTextSelection, error) {
	selected, err := r.JSONReader.QueryTextAtPath(row, path, legacyFloat)
	if err == nil {
		r.cancel()
	}
	return selected, err
}

func (r *cancelDuringJSONQueryReader) OpenCursor() (vector.JSONCursor, error) {
	cursor, err := r.JSONReader.OpenCursor()
	if err != nil {
		return nil, err
	}
	return &cancelDuringJSONQueryCursor{JSONCursor: cursor, cancel: r.cancel}, nil
}

type cancelDuringJSONQueryCursor struct {
	vector.JSONCursor
	cancel context.CancelFunc
}

func (c *cancelDuringJSONQueryCursor) ResetQuery(row int, path bytejson.Path) error {
	err := c.JSONCursor.ResetQuery(row, path)
	if err == nil {
		c.cancel()
	}
	return err
}

func (c *cancelDuringJSONQueryCursor) QueryText(row int, path []byte, legacyFloat bool) (vector.JSONTextSelection, error) {
	selected, err := c.JSONCursor.QueryText(row, path, legacyFloat)
	if err == nil {
		c.cancel()
	}
	return selected, err
}

func TestJSONValueCancellationDuringReaderExecution(t *testing.T) {
	for _, test := range []struct {
		name     string
		contract bool
		text     bool
	}{{"legacy", false, true}, {"text-contract", true, true}, {"typed-contract", true, false}} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			original := proc.Ctx
			ctx, cancel := context.WithCancel(original)
			defer cancel()
			proc.Ctx = ctx
			proc.SetBaseProcessRunningStatus(true)
			defer func() { proc.Ctx = original; proc.SetBaseProcessRunningStatus(false) }()
			raw := mustJsonBinaryString(t, `{"keep":1}`)
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{raw, raw}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"$.keep"}, nil),
			}
			expected := NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"1", "1"}, nil)
			if test.contract {
				nullValue := NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true})
				if test.text {
					nullValue = NewFunctionTestConstInput(types.T_varchar.ToType(), []string{""}, []bool{true})
				}
				inputs = append(inputs,
					nullValue,
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueNullResponse}, nil),
					nullValue,
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueNullResponse}, nil),
					nullValue)
				if !test.text {
					expected = NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1, 1}, nil)
				}
			}
			fc := NewFunctionTestCase(proc, inputs, expected, JsonValue)
			defer fc.Free()
			reader, err := vector.NewJSONReaderSnapshot(fc.parameters[0], proc.Mp())
			require.NoError(t, err)
			defer reader.Close()
			require.NoError(t, fc.result.PreExtendAndReset(2))
			err = jsonValueWithAdmittedReader(&cancelDuringJSONQueryReader{JSONReader: reader, cancel: cancel}, fc.parameters, fc.result, proc, 2, nil)
			require.ErrorIs(t, err, context.Canceled, "a successful row query cannot turn in-flight cancellation into success or an ON ERROR response")
		})
	}
}

type forgedJSONValueReader struct {
	vector.JSONReader
	inspected bool
}
type forgedJSONValueCursor struct{ vector.JSONCursor }

func (r *forgedJSONValueReader) Length() int {
	r.inspected = true
	return r.JSONReader.Length()
}

func (r *forgedJSONValueReader) OpenCursor() (vector.JSONCursor, error) {
	cursor, err := r.JSONReader.OpenCursor()
	if err != nil {
		return nil, err
	}
	return &forgedJSONValueCursor{JSONCursor: cursor}, nil
}

func (c *forgedJSONValueCursor) QueryText(int, []byte, bool) (vector.JSONTextSelection, error) {
	return vector.JSONTextSelection{Exists: true, Text: "forged"}, nil
}

func TestJSONValueRejectsForgedReader(t *testing.T) {
	for _, contract := range []bool{false, true} {
		name := "legacy"
		if contract {
			name = "contract"
		}
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			raw := mustJsonBinaryString(t, `{"keep":1}`)
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{raw}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"$.keep"}, nil),
			}
			expected := NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"1"}, nil)
			if contract {
				inputs = append(inputs,
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueNullResponse}, nil),
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueNullResponse}, nil),
					NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}))
				expected = NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil)
			}
			fc := NewFunctionTestCase(proc, inputs, expected, JsonValue)
			defer fc.Free()
			reader, err := vector.NewJSONReaderSnapshot(fc.parameters[0], proc.Mp())
			require.NoError(t, err)
			defer reader.Close()
			require.NoError(t, fc.result.PreExtendAndReset(1))
			forged := &forgedJSONValueReader{JSONReader: reader}
			err = JsonValueWithReader(forged, fc.parameters, fc.result, proc, 1, nil)
			require.Error(t, err, "an arbitrary interface wrapper must not acquire a validated-reader capability")
			require.False(t, forged.inspected, "reject the wrapper before calling any interface method")
		})
	}
}

func TestJSONValueReaderTextOutputIsOwned(t *testing.T) {
	for _, contract := range []bool{false, true} {
		for _, constOutput := range []bool{false, true} {
			for _, sample := range []struct{ name, value string }{
				{"empty", ""},
				{"inline", "short"},
				{"long-unicode", strings.Repeat("猫🌟", 180)},
			} {
				name := "legacy"
				if contract {
					name = "contract"
				}
				if constOutput {
					name += "/constant-output"
				}
				name += "/" + sample.name
				t.Run(name, func(t *testing.T) {
					proc := testutil.NewProcess(t)
					text := sample.value
					raw := mustJsonBinaryString(t, `{"keep":"`+text+`"}`)
					inputs := []FunctionTestInput{
						NewFunctionTestInput(types.T_json.ToType(), []string{raw}, nil),
						NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"$.keep"}, nil),
					}
					textType := types.New(types.T_varchar, 512, 0)
					if contract {
						inputs = append(inputs,
							NewFunctionTestConstInput(textType, []string{""}, []bool{true}),
							NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueNullResponse}, nil),
							NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
							NewFunctionTestConstInput(types.T_int64.ToType(), []int64{jsonValueNullResponse}, nil),
							NewFunctionTestConstInput(types.T_int64.ToType(), []int64{0}, []bool{true}))
					}
					fc := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(textType, false, []string{text}, nil), JsonValue)
					defer fc.Free()
					reader, err := vector.NewJSONReaderSnapshot(fc.parameters[0], proc.Mp())
					require.NoError(t, err)
					defer reader.Close()
					require.NoError(t, fc.result.PreExtendAndReset(1))
					if constOutput {
						require.NoError(t, vector.SetConstBytes(fc.result.GetResultVector(), []byte("seed"), 1, proc.Mp()))
					}
					require.NoError(t, JsonValueWithReader(reader, fc.parameters, fc.result, proc, 1, nil))
					reader.Close()
					fc.parameters[0].GetBytesAt(0)[0] = 0xfd
					runtime.GC()
					require.False(t, fc.result.GetResultVector().IsNull(0))
					require.Equal(t, text, string(fc.result.GetResultVector().GetBytesAt(0)))
				})
			}
		}
	}
}
