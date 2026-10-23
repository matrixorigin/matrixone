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
	"bytes"
	"compress/zlib"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestJSONStringConsumerJSONOverloads(t *testing.T) {
	wantType := types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8MB4Bin)
	cases := []struct {
		name string
		args []types.Type
	}{
		{
			name: "concat",
			args: []types.Type{types.T_json.ToType(), types.T_varchar.ToType()},
		},
		{
			name: "concat_ws",
			args: []types.Type{types.T_json.ToType(), types.T_varchar.ToType(), types.T_json.ToType()},
		},
		{
			name: "elt",
			args: []types.Type{types.T_int64.ToType(), types.T_json.ToType(), types.T_varchar.ToType()},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GetFunctionByName(context.Background(), tc.name, tc.args)
			require.NoError(t, err)
			_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
			wantOverload := int32(0)
			if tc.name == "elt" {
				wantOverload = 1
			}
			require.Equal(t, wantOverload, overloadIndex)
			_, shouldCast := got.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast)
			if tc.name == "elt" {
				require.Equal(t, wantType, got.GetReturnType())
			}
		})
	}
}

func TestJSONStringConsumerRemainingFunctionsKeepJSONAsSource(t *testing.T) {
	cases := []struct {
		name         string
		args         []types.Type
		wantOverload int32
		wantCast     bool
	}{
		{name: "md5", args: []types.Type{types.T_json.ToType()}, wantOverload: 0},
		{name: "sha1", args: []types.Type{types.T_json.ToType()}, wantOverload: 0},
		{name: "sha2", args: []types.Type{types.T_json.ToType(), types.T_int64.ToType()}, wantOverload: 0},
		{name: "sha2", args: []types.Type{types.T_json.ToType(), types.T_varchar.ToType()}, wantOverload: 0, wantCast: true},
		{name: "compress", args: []types.Type{types.T_json.ToType()}, wantOverload: 0},
		{name: "quote", args: []types.Type{types.T_json.ToType()}, wantOverload: 0},
		{name: "soundex", args: []types.Type{types.T_json.ToType()}, wantOverload: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/"+tc.args[len(tc.args)-1].Oid.String(), func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), tc.name, tc.args)
			require.NoError(t, err)
			_, overloadIndex := DecodeOverloadID(resolved.GetEncodedOverloadID())
			require.Equal(t, tc.wantOverload, overloadIndex)
			casts, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.Equal(t, tc.wantCast, shouldCast)
			if shouldCast {
				require.Equal(t, []types.Type{types.T_json.ToType(), types.T_int64.ToType()}, casts)
			}
		})
	}
}

func TestJSONStringConsumerNonJSONTypesKeepExistingOverloads(t *testing.T) {
	varcharType := types.T_varchar.ToType()
	cases := []struct {
		name string
		args []types.Type
	}{
		{name: "md5", args: []types.Type{varcharType}},
		{name: "sha1", args: []types.Type{varcharType}},
		{name: "sha2", args: []types.Type{varcharType, types.T_int64.ToType()}},
		{name: "compress", args: []types.Type{varcharType}},
		{name: "quote", args: []types.Type{varcharType}},
		{name: "soundex", args: []types.Type{varcharType}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), tc.name, tc.args)
			require.NoError(t, err)
			_, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast)
		})
	}
}

func TestJSONStringConsumerJSONResultBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		want types.Type
	}{
		{name: "compress", want: types.T_blob.ToType()},
		{name: "quote", want: types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8MB4Bin)},
		{name: "soundex", want: types.NewWithCharset(types.T_text, types.MaxLongTextLen, 0, types.CharsetUTF8)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), tc.name, []types.Type{types.T_json.ToType()})
			require.NoError(t, err)
			require.Equal(t, tc.want, resolved.GetReturnType())
		})
	}
}

func TestJSONStringConsumerRegisteredExecutors(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)

	tests := []struct {
		name   string
		args   []types.Type
		inputs []FunctionTestInput
		want   string
	}{
		{
			name: "concat",
			args: []types.Type{types.T_json.ToType(), types.T_varchar.ToType()},
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{mustJsonBinaryString(t, `{"a":1}`)}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"!"}, nil),
			},
			want: `{"a": 1}!`,
		},
		{
			name: "concat_ws",
			args: []types.Type{types.T_json.ToType(), types.T_varchar.ToType(), types.T_json.ToType()},
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{mustJsonBinaryString(t, `"|"`)}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"x"}, nil),
				NewFunctionTestInput(types.T_json.ToType(), []string{mustJsonBinaryString(t, `1`)}, nil),
			},
			want: `x"|"1`,
		},
		{
			name: "elt",
			args: []types.Type{types.T_int64.ToType(), types.T_json.ToType()},
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_json.ToType(), []string{mustJsonBinaryString(t, `[1]`)}, nil),
			},
			want: `[1]`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GetFunctionByName(proc.Ctx, tc.name, tc.args)
			require.NoError(t, err)
			_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
			wantOverload := int32(0)
			if tc.name == "elt" {
				wantOverload = 1
			}
			require.Equal(t, wantOverload, overloadIndex)

			inputs := make([]*vector.Vector, len(tc.inputs))
			for i, input := range tc.inputs {
				inputs[i] = newVectorByType(proc.Mp(), input.typ, input.values, nil)
			}
			defer func() {
				for _, input := range inputs {
					input.Free(proc.Mp())
				}
			}()

			result, err := RunFunctionDirectly(proc, got.GetEncodedOverloadID(), inputs, 1)
			require.NoError(t, err)
			defer result.Free(proc.Mp())
			require.Equal(t, tc.want, string(result.GetBytesAt(0)))
		})
	}
}

func runRegisteredJSONStringConsumer(
	t *testing.T,
	proc *process.Process,
	name string,
	argTypes []types.Type,
	values []any,
) []byte {
	t.Helper()
	require.Len(t, values, len(argTypes))
	resolved, err := GetFunctionByName(proc.Ctx, name, argTypes)
	require.NoError(t, err)
	inputs := make([]*vector.Vector, len(argTypes))
	for i := range argTypes {
		inputs[i] = newVectorByType(proc.Mp(), argTypes[i], values[i], nil)
	}
	defer func() {
		for _, input := range inputs {
			input.Free(proc.Mp())
		}
	}()
	if targets, shouldCast := resolved.ShouldDoImplicitTypeCast(); shouldCast {
		require.Len(t, targets, len(inputs))
		for i, targetType := range targets {
			if argTypes[i].Eq(targetType) {
				continue
			}
			// Reproduce the binder-owned integer parameter conversion instead
			// of passing the original string vector to an INT64 executor.
			integerTarget, integerContext := IntegerArgumentTarget(name, i)
			require.True(t, integerContext)
			require.Equal(t, integerTarget, targetType.Oid)
			target := vector.NewVec(targetType)
			defer target.Free(proc.Mp())
			converted, err := RunFunctionDirectly(proc, EncodeOverloadID(CAST, IntegerArgumentCastOverload), []*vector.Vector{inputs[i], target}, 1)
			require.NoError(t, err)
			inputs[i].Free(proc.Mp())
			inputs[i] = converted
		}
	}
	result, err := RunFunctionDirectly(proc, resolved.GetEncodedOverloadID(), inputs, 1)
	require.NoError(t, err)
	defer result.Free(proc.Mp())
	return append([]byte(nil), result.GetBytesAt(0)...)
}

func requireJSONStringConsumerCompression(t *testing.T, compressed, visible []byte) {
	t.Helper()
	require.GreaterOrEqual(t, len(compressed), 4)
	require.Equal(t, uint32(len(visible)), binary.LittleEndian.Uint32(compressed[:4]))
	reader, err := zlib.NewReader(bytes.NewReader(compressed[4:]))
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, visible, decompressed)
}

func TestJSONStringConsumersPreserveEscapedVisibleBytes(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	jsonValue := mustJsonBinaryString(t, `"a\nb"`)
	visible := []byte(`"a\nb"`)
	md5Sum := md5.Sum(visible)
	sha1Sum := sha1.Sum(visible)
	sha256Sum := sha256.Sum256(visible)
	jsonType := types.T_json.ToType()
	checks := []struct {
		name   string
		args   []types.Type
		values []any
		want   []byte
	}{
		{name: "md5", args: []types.Type{jsonType}, values: []any{[]string{jsonValue}}, want: []byte(hex.EncodeToString(md5Sum[:]))},
		{name: "sha1", args: []types.Type{jsonType}, values: []any{[]string{jsonValue}}, want: []byte(hex.EncodeToString(sha1Sum[:]))},
		{name: "sha2", args: []types.Type{jsonType, types.T_int64.ToType()}, values: []any{[]string{jsonValue}, []int64{256}}, want: []byte(hex.EncodeToString(sha256Sum[:]))},
		{name: "sha2", args: []types.Type{jsonType, types.T_varchar.ToType()}, values: []any{[]string{jsonValue}, []string{"256tail"}}, want: []byte(hex.EncodeToString(sha256Sum[:]))},
		{name: "quote", args: []types.Type{jsonType}, values: []any{[]string{jsonValue}}, want: []byte("'\"a\\\\nb\"'")},
		{name: "soundex", args: []types.Type{jsonType}, values: []any{[]string{jsonValue}}, want: []byte("A510")},
	}
	for _, tc := range checks {
		t.Run(tc.name+"/"+tc.args[len(tc.args)-1].Oid.String(), func(t *testing.T) {
			got := runRegisteredJSONStringConsumer(t, proc, tc.name, tc.args, tc.values)
			require.Equal(t, tc.want, got)
		})
	}
	requireJSONStringConsumerCompression(t,
		runRegisteredJSONStringConsumer(t, proc, "compress", []types.Type{jsonType}, []any{[]string{jsonValue}}),
		visible,
	)
}

func TestJSONStringConsumersDoNotTruncateWideJSON(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	visible := []byte("[\"" + strings.Repeat("a", 65536) + "\"]")
	require.Len(t, visible, 65540)
	jsonValue := mustJsonBinaryString(t, string(visible))
	jsonType := types.T_json.ToType()
	md5Sum := md5.Sum(visible)
	sha1Sum := sha1.Sum(visible)
	sha256Sum := sha256.Sum256(visible)
	checks := []struct {
		name string
		args []types.Type
		vals []any
		want []byte
	}{
		{name: "md5", args: []types.Type{jsonType}, vals: []any{[]string{jsonValue}}, want: []byte(hex.EncodeToString(md5Sum[:]))},
		{name: "sha1", args: []types.Type{jsonType}, vals: []any{[]string{jsonValue}}, want: []byte(hex.EncodeToString(sha1Sum[:]))},
		{name: "sha2", args: []types.Type{jsonType, types.T_int64.ToType()}, vals: []any{[]string{jsonValue}, []int64{256}}, want: []byte(hex.EncodeToString(sha256Sum[:]))},
		{name: "quote", args: []types.Type{jsonType}, vals: []any{[]string{jsonValue}}, want: append(append([]byte("'"), visible...), '\'')},
		{name: "soundex", args: []types.Type{jsonType}, vals: []any{[]string{jsonValue}}, want: []byte("A000")},
	}
	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			got := runRegisteredJSONStringConsumer(t, proc, tc.name, tc.args, tc.vals)
			require.Equal(t, tc.want, got)
		})
	}
	requireJSONStringConsumerCompression(t,
		runRegisteredJSONStringConsumer(t, proc, "compress", []types.Type{jsonType}, []any{[]string{jsonValue}}),
		visible,
	)
}

func TestJSONStringConsumersJSONNullAndMaskedRows(t *testing.T) {
	visible := []string{`null`, `"a\nb"`}
	digests := func(hash func([]byte) []byte) []string {
		return []string{hex.EncodeToString(hash([]byte(visible[0]))), hex.EncodeToString(hash([]byte(visible[1])))}
	}
	md5Want := digests(func(v []byte) []byte { sum := md5.Sum(v); return sum[:] })
	sha1Want := digests(func(v []byte) []byte { sum := sha1.Sum(v); return sum[:] })
	sha2Want := digests(func(v []byte) []byte { sum := sha256.Sum256(v); return sum[:] })
	for _, tc := range []struct {
		name       string
		fn         executeLogicOfOverload
		lengthType types.T
		want       []string
	}{
		{name: "md5", fn: Md5, want: md5Want},
		{name: "sha1", fn: SHA1Func, want: sha1Want},
		{name: "sha2 integer length", fn: SHA2Func, lengthType: types.T_int64, want: sha2Want},
		// Retained character-length overloads can still execute directly even
		// though new bindings normalize the length to the integer overload.
		{name: "sha2 character length", fn: SHA2StringLengthFunc, lengthType: types.T_varchar, want: sha2Want},
		{name: "compress", fn: Compress},
		{name: "quote", fn: Quote, want: []string{"'null'", "'\"a\\\\nb\"'"}},
		{name: "soundex", fn: Soundex, want: []string{"N400", "A510"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			nativeBefore, heapBefore := proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()
			func() {
				input := newVectorByType(proc.Mp(), types.T_json.ToType(), []string{
					mustJsonBinaryString(t, `1`), mustJsonBinaryString(t, `1`),
					mustJsonBinaryString(t, visible[0]), mustJsonBinaryString(t, visible[1]),
				}, nil)
				defer input.Free(proc.Mp())
				input.GetNulls().Add(1)
				// A masked row must not decode stored JSON at all. Corrupt only
				// after normal vector admission, as in the malformed-input suite.
				input.GetBytesAt(0)[0] = 0xff
				parameters := []*vector.Vector{input}
				if tc.lengthType != types.T_any {
					var lengths *vector.Vector
					if tc.lengthType == types.T_int64 {
						lengths = newVectorByType(proc.Mp(), types.T_int64.ToType(), []int64{256, 256, 256, 256}, nil)
					} else {
						lengths = newVectorByType(proc.Mp(), types.T_varchar.ToType(), []string{"256tail", "256tail", "256tail", "256tail"}, nil)
					}
					defer lengths.Free(proc.Mp())
					parameters = append(parameters, lengths)
				}
				resultType := types.T_text.ToType()
				if tc.name == "compress" {
					resultType = types.T_blob.ToType()
				}
				result := vector.NewFunctionResultWrapper(resultType, proc.Mp())
				defer result.Free()
				require.NoError(t, result.PreExtendAndReset(4))
				require.NoError(t, tc.fn(parameters, result, proc, 4,
					&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, true, true}}))
				got := result.GetResultVector()
				require.True(t, got.IsNull(0), "masked malformed JSON must not be evaluated")
				if tc.name == "quote" {
					require.False(t, got.IsNull(1))
					require.Equal(t, "NULL", string(got.GetBytesAt(1)))
				} else {
					require.True(t, got.IsNull(1), "SQL NULL must remain SQL NULL")
				}
				for i, v := range visible {
					row := uint64(i + 2)
					require.False(t, got.IsNull(row), "JSON null is a visible non-SQL-NULL value")
					if tc.name == "compress" {
						requireJSONStringConsumerCompression(t, got.GetBytesAt(int(row)), []byte(v))
					} else {
						require.Equal(t, tc.want[i], string(got.GetBytesAt(int(row))))
					}
				}
			}()
			require.Equal(t, nativeBefore, proc.Mp().CurrNB())
			require.Equal(t, heapBefore, proc.Mp().OnHeapCurrNB())
		})
	}
}

func TestJSONStringConsumerBinaryReturnType(t *testing.T) {
	binaryType := types.T_blob.ToType()
	for _, name := range []string{"concat", "concat_ws", "elt"} {
		t.Run(name, func(t *testing.T) {
			args := map[string][]types.Type{
				"concat":    {types.T_json.ToType(), binaryType},
				"concat_ws": {types.T_json.ToType(), binaryType},
				"elt":       {types.T_int64.ToType(), types.T_json.ToType(), binaryType},
			}[name]
			got, err := GetFunctionByName(context.Background(), name, args)
			require.NoError(t, err)
			_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
			wantOverload := int32(0)
			if name == "elt" {
				wantOverload = 1
			}
			require.Equal(t, wantOverload, overloadIndex)
			require.Equal(t, binaryType, got.GetReturnType())
		})
	}
}

func TestJSONStringConsumerChecksRejectMissingOverload(t *testing.T) {
	missingJSONOverload := []overload{{overloadId: 0}}
	for _, tc := range []struct {
		name  string
		check func([]overload, []types.Type) checkResult
		args  []types.Type
	}{
		{name: "elt", check: eltCheck, args: []types.Type{types.T_int64.ToType(), types.T_json.ToType()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.check(missingJSONOverload, tc.args)
			require.Equal(t, failedFunctionParametersWrong, got.status)
		})
	}
}

func TestJSONStringConsumersRejectMalformedStoredJSON(t *testing.T) {
	validJSON := mustJsonBinaryString(t, `1`)
	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
		jsonAt int
		fn     executeLogicOfOverload
	}{
		{
			name: "concat",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"x"}, nil),
			},
			jsonAt: 0,
			fn:     builtInConcat,
		},
		{
			name: "concat_ws separator",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"x"}, nil),
			},
			jsonAt: 0,
			fn:     ConcatWs,
		},
		{
			name: "concat_ws value",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"|"}, nil),
				NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil),
			},
			jsonAt: 1,
			fn:     ConcatWs,
		},
		{
			name: "elt selected value",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil),
			},
			jsonAt: 1,
			fn:     Elt,
		},
		{
			name:   "md5",
			inputs: []FunctionTestInput{NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil)},
			fn:     Md5,
		},
		{
			name:   "sha1",
			inputs: []FunctionTestInput{NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil)},
			fn:     SHA1Func,
		},
		{
			name: "sha2 integer length",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{256}, nil),
			},
			fn: SHA2Func,
		},
		{
			name: "sha2 character length",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"256tail"}, nil),
			},
			fn: SHA2StringLengthFunc,
		},
		{
			name:   "compress",
			inputs: []FunctionTestInput{NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil)},
			fn:     Compress,
		},
		{
			name:   "quote",
			inputs: []FunctionTestInput{NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil)},
			fn:     Quote,
		},
		{
			name:   "soundex",
			inputs: []FunctionTestInput{NewFunctionTestInput(types.T_json.ToType(), []string{validJSON}, nil)},
			fn:     Soundex,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			nativeBefore, heapBefore := proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()
			caseUnderTest := NewFunctionTestCase(
				proc, tc.inputs,
				NewFunctionTestResult(types.T_text.ToType(), true, nil, nil),
				tc.fn,
			)
			defer caseUnderTest.Free()
			caseUnderTest.parameters[tc.jsonAt].GetBytesAt(0)[0] = 0xff
			ok, info := caseUnderTest.RunAndFree()
			require.True(t, ok, info)
			require.Equal(t, nativeBefore, proc.Mp().CurrNB())
			require.Equal(t, heapBefore, proc.Mp().OnHeapCurrNB())
		})
	}
}

func TestJSONStringConsumerCastsOnlyNonJSONOperands(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	tests := []struct {
		name       string
		args       []types.Type
		jsonAt     int
		castAt     int
		castType   types.T
		castWidth  int32
		returnType types.Type
	}{
		{
			name:       "concat",
			args:       []types.Type{types.T_json.ToType(), types.T_int64.ToType()},
			jsonAt:     0,
			castAt:     1,
			castType:   types.T_varchar,
			castWidth:  20,
			returnType: types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8),
		},
		{
			name:       "concat_ws",
			args:       []types.Type{types.T_json.ToType(), types.T_int64.ToType(), types.T_json.ToType()},
			jsonAt:     0,
			castAt:     1,
			castType:   types.T_varchar,
			castWidth:  20,
			returnType: types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8),
		},
		{
			name:       "elt",
			args:       []types.Type{types.T_int32.ToType(), types.T_json.ToType()},
			jsonAt:     1,
			castAt:     0,
			castType:   types.T_int64,
			returnType: types.NewWithCharset(types.T_text, 0, 0, types.CharsetUTF8MB4Bin),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GetFunctionByName(proc.Ctx, tc.name, tc.args)
			require.NoError(t, err)
			_, overloadIndex := DecodeOverloadID(got.GetEncodedOverloadID())
			wantOverload := int32(0)
			if tc.name == "elt" {
				wantOverload = 1
			}
			require.Equal(t, wantOverload, overloadIndex)
			casts, shouldCast := got.ShouldDoImplicitTypeCast()
			require.True(t, shouldCast)
			require.Equal(t, types.T_json, casts[tc.jsonAt].Oid)
			require.Equal(t, tc.castType, casts[tc.castAt].Oid)
			if tc.castWidth != 0 {
				require.Equal(t, tc.castWidth, casts[tc.castAt].Width)
			}
			require.Equal(t, tc.returnType, got.GetReturnType())
		})
	}
}

func TestJSONStringConsumersUseVisibleJSON(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	nativeBefore, heapBefore := proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()
	defer func() {
		require.Equal(t, nativeBefore, proc.Mp().CurrNB())
		require.Equal(t, heapBefore, proc.Mp().OnHeapCurrNB())
	}()

	jsonValues := []string{
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `[1,true,"x"]`),
		mustJsonBinaryString(t, `"x"`),
		mustJsonBinaryString(t, `12.5`),
		mustJsonBinaryString(t, `false`),
		mustJsonBinaryString(t, `null`),
	}
	jsonNulls := []bool{false, false, false, false, false, true}
	prefix := []string{"p", "p", "p", "p", "p", "p"}
	suffix := []string{"s", "s", "s", "s", "s", "s"}
	concat := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), prefix, nil),
			NewFunctionTestInput(types.T_json.ToType(), jsonValues, jsonNulls),
			NewFunctionTestInput(types.T_varchar.ToType(), suffix, nil),
		},
		NewFunctionTestResult(types.T_text.ToType(), false,
			[]string{
				`p{"a": 1}s`,
				`p[1, true, "x"]s`,
				`p"x"s`,
				`p12.5s`,
				`pfalses`,
				"",
			}, []bool{false, false, false, false, false, true}),
		builtInConcat,
	)
	ok, info := concat.RunAndFree()
	require.True(t, ok, info)

	textControl := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`{"a":1}`}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"x"}, nil),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{`{"a":1}x`}, nil),
		builtInConcat,
	)
	ok, info = textControl.RunAndFree()
	require.True(t, ok, info)

	binaryValue := string([]byte{'a', 0, 0xff})
	binaryControl := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_blob.ToType(), []string{binaryValue}, nil),
			NewFunctionTestInput(types.T_blob.ToType(), []string{"!"}, nil),
		},
		NewFunctionTestResult(types.T_blob.ToType(), false, []string{binaryValue + "!"}, nil),
		builtInConcat,
	)
	ok, info = binaryControl.RunAndFree()
	require.True(t, ok, info)
}

func TestConcatWsJSONStringOperands(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	nativeBefore, heapBefore := proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()
	defer func() {
		require.Equal(t, nativeBefore, proc.Mp().CurrNB())
		require.Equal(t, heapBefore, proc.Mp().OnHeapCurrNB())
	}()

	separator := []string{
		mustJsonBinaryString(t, `"|"`),
		mustJsonBinaryString(t, `"|"`),
		mustJsonBinaryString(t, `"|"`),
		mustJsonBinaryString(t, `"|"`),
		mustJsonBinaryString(t, `"|"`),
	}
	values := []string{
		mustJsonBinaryString(t, `null`),
		mustJsonBinaryString(t, `"x"`),
		mustJsonBinaryString(t, `{"a":1}`),
		"",
		mustJsonBinaryString(t, `1`),
	}
	valueNulls := []bool{false, false, false, true, false}
	other := []string{"z", "z", "", "z", "z"}
	otherNulls := []bool{false, false, true, false, false}
	separatorNulls := []bool{false, false, false, false, true}

	tc := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), separator, separatorNulls),
			NewFunctionTestInput(types.T_json.ToType(), values, valueNulls),
			NewFunctionTestInput(types.T_varchar.ToType(), other, otherNulls),
		},
		NewFunctionTestResult(types.T_text.ToType(), false,
			[]string{
				`null"|"z`,
				`"x""|"z`,
				`{"a": 1}`,
				"z",
				"",
			}, []bool{false, false, false, false, true}),
		ConcatWs,
	)
	ok, info := tc.RunAndFree()
	require.True(t, ok, info)
}

func TestEltJSONStringOperandsAndSelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	nativeBefore, heapBefore := proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()
	defer func() {
		require.Equal(t, nativeBefore, proc.Mp().CurrNB())
		require.Equal(t, heapBefore, proc.Mp().OnHeapCurrNB())
	}()

	jsonValues := []string{
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `{"a":1}`),
		mustJsonBinaryString(t, `null`),
	}
	jsonNulls := []bool{false, false, false, false, false, true}
	thirdJSON := []string{
		mustJsonBinaryString(t, `[1,true]`),
		mustJsonBinaryString(t, `[1,true]`),
		mustJsonBinaryString(t, `[1,true]`),
		mustJsonBinaryString(t, `[1,true]`),
		mustJsonBinaryString(t, `[1,true]`),
		mustJsonBinaryString(t, `[1,true]`),
	}
	indices := []int64{1, 2, 0, 3, 4, 1}
	textValues := []string{"text", "text", "text", "text", "text", "text"}

	tc := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), indices, nil),
			NewFunctionTestInput(types.T_json.ToType(), jsonValues, jsonNulls),
			NewFunctionTestInput(types.T_varchar.ToType(), textValues, nil),
			NewFunctionTestInput(types.T_json.ToType(), thirdJSON, nil),
		},
		NewFunctionTestResult(types.T_text.ToType(), false,
			[]string{`{"a": 1}`, "text", "", `[1, true]`, "", ""},
			[]bool{false, false, true, false, true, true}),
		Elt,
	)
	ok, info := tc.RunAndFree()
	require.True(t, ok, info)

	// The JSON vector is deliberately malformed. Every row selects the text
	// operand or is out of range, so ELT must never marshal the unselected JSON.
	unselected := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{2, 0, 2}, nil),
			NewFunctionTestInput(types.T_json.ToType(), []string{
				mustJsonBinaryString(t, `1`),
				mustJsonBinaryString(t, `1`),
				mustJsonBinaryString(t, `1`),
			}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"chosen", "ignored", "chosen"}, nil),
		},
		NewFunctionTestResult(types.T_text.ToType(), false,
			[]string{"chosen", "", "chosen"}, []bool{false, true, false}),
		Elt,
	)
	defer unselected.Free()
	// The vector admission path has already accepted these values. Mutating the
	// stored type byte after admission gives the executor an invalid, unselected
	// payload without teaching the test framework to admit malformed JSON.
	for i := 0; i < 3; i++ {
		unselected.parameters[1].GetBytesAt(i)[0] = 0xff
	}
	ok, info = unselected.RunAndFree()
	require.True(t, ok, info)
}
