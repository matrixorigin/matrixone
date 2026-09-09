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
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSHA2PreservesWideStringInputs(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	tests := []struct {
		name      string
		inputType types.Type
		input     string
		algorithm int64
	}{
		{
			name:      "longtext over varchar limit",
			inputType: types.New(types.T_text, types.MaxLongTextLen, 0),
			input:     strings.Repeat("x", 160000),
			algorithm: 256,
		},
		{
			name:      "mediumtext just over varchar limit",
			inputType: types.New(types.T_text, types.MaxMediumTextLen, 0),
			input:     strings.Repeat("a", 65535) + "b",
			algorithm: 512,
		},
		{
			name:      "blob preserves binary bytes",
			inputType: types.New(types.T_blob, types.MaxBlobLen, 0),
			input:     string([]byte(strings.Repeat("\x00\x01\x02\x03", 20000))),
			algorithm: 224,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), "sha2", []types.Type{
				test.inputType,
				types.T_int64.ToType(),
			})
			require.NoError(t, err)
			targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast)
			require.Empty(t, targets)

			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(test.inputType, []string{test.input}, nil),
					NewFunctionTestInput(types.T_int64.ToType(), []int64{test.algorithm}, nil),
				},
				NewFunctionTestResult(
					types.T_varchar.ToType(),
					false,
					[]string{sha2TestDigest(test.input, test.algorithm)},
					nil,
				),
				SHA2Func,
			)
			succeeded, info := testCase.Run()
			require.True(t, succeeded, info)
		})
	}
}

func TestSHA2StillFormatsScalarInput(t *testing.T) {
	resolved, err := GetFunctionByName(context.Background(), "sha2", []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
	})
	require.NoError(t, err)
	targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
	require.True(t, shouldCast)
	require.Len(t, targets, 2)
	require.Equal(t, types.T_varchar, targets[0].Oid)
	require.Equal(t, int32(20), targets[0].Width)
	require.Equal(t, types.T_int64, targets[1].Oid)
}

func TestSHA2DefersUnknownHashLengthToStringOverload(t *testing.T) {
	resolved, err := GetFunctionByName(context.Background(), "sha2", []types.Type{
		types.T_varchar.ToType(),
		types.T_any.ToType(),
	})
	require.NoError(t, err)
	_, overload := DecodeOverloadID(resolved.GetEncodedOverloadID())
	require.Equal(t, int32(1), overload)
	targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
	require.True(t, shouldCast)
	require.Len(t, targets, 2)
	require.Equal(t, types.T_varchar, targets[0].Oid)
	require.Equal(t, types.T_varchar, targets[1].Oid)
}

func TestSHA2PreservesEveryMySQLStringDomain(t *testing.T) {
	for _, oid := range []types.T{
		types.T_char,
		types.T_varchar,
		types.T_binary,
		types.T_varbinary,
		types.T_blob,
		types.T_text,
	} {
		t.Run(oid.String(), func(t *testing.T) {
			resolved, err := GetFunctionByName(context.Background(), "sha2", []types.Type{
				oid.ToType(),
				types.T_int64.ToType(),
			})
			require.NoError(t, err)
			targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast)
			require.Empty(t, targets)
		})
	}
}

func TestSHA2StringLengthUsesMySQLIntegerConversion(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	values := []string{"256tail", "  +224tail", "abc", "", "123", "0", "-0tail", "-256tail", "9223372036854775808tail"}
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(),
			[]string{"hello", "hello", "hello", "hello", "hello", "hello", "hello", "hello", "hello"},
			[]bool{false, false, false, false, false, false, false, false, false}),
		NewFunctionTestInput(types.T_varchar.ToType(), values,
			[]bool{false, false, false, false, false, false, false, false, false}),
	}
	want := sha2TestDigest("hello", 256)
	want224 := sha2TestDigest("hello", 224)
	testCase := NewFunctionTestCase(
		proc,
		inputs,
		NewFunctionTestResult(
			types.T_varchar.ToType(),
			false,
			[]string{want, want224, want, want, "", want, want, "", ""},
			[]bool{false, false, false, false, true, false, false, true, true},
		),
		SHA2StringLengthFunc,
	)
	succeeded, info := testCase.Run()
	require.True(t, succeeded, info)
}

func TestSHA2StringLengthPreservesBinaryOperands(t *testing.T) {
	binaryType := types.NewWithCharset(types.T_varbinary, 32, 0, types.CharsetBinary)
	resolved, err := GetFunctionByName(context.Background(), "sha2", []types.Type{
		binaryType,
		binaryType,
	})
	require.NoError(t, err)
	targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
	require.False(t, shouldCast)
	require.Empty(t, targets)
	_, overload := DecodeOverloadID(resolved.GetEncodedOverloadID())
	require.Equal(t, int32(1), overload)

	proc := testutil.NewProcess(t)
	defer proc.Free()
	value := string([]byte{0xff, 0x00, 'm', 'o'})
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(binaryType, []string{value}, []bool{false}),
			NewFunctionTestConstInput(binaryType, []string{"256tail"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false,
			[]string{sha2TestDigest(value, 256)}, []bool{false}),
		SHA2StringLengthFunc,
	)
	succeeded, info := testCase.Run()
	require.True(t, succeeded, info)
}

func TestSHA2StringLengthAcceptsEveryMySQLStringDomain(t *testing.T) {
	for _, oid := range []types.T{
		types.T_char,
		types.T_varchar,
		types.T_binary,
		types.T_varbinary,
		types.T_blob,
		types.T_text,
	} {
		t.Run(oid.String(), func(t *testing.T) {
			lengthType := oid.ToType()
			resolved, err := GetFunctionByName(context.Background(), "sha2", []types.Type{
				types.T_varchar.ToType(),
				lengthType,
			})
			require.NoError(t, err)
			targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, shouldCast)
			require.Empty(t, targets)
			_, overload := DecodeOverloadID(resolved.GetEncodedOverloadID())
			require.Equal(t, int32(1), overload)
		})
	}
}

func TestSHA2StringLengthHonorsSelectList(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"hello", "hello"}, []bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-a-length", "256"}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false,
			[]string{"", sha2TestDigest("hello", 256)}, []bool{true, false}),
		SHA2StringLengthFunc,
	).WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
	succeeded, info := testCase.Run()
	require.True(t, succeeded, info)
}

func sha2TestDigest(input string, algorithm int64) string {
	var digest []byte
	switch algorithm {
	case 224:
		sum := sha256.Sum224([]byte(input))
		digest = sum[:]
	case 256:
		sum := sha256.Sum256([]byte(input))
		digest = sum[:]
	case 384:
		sum := sha512.Sum384([]byte(input))
		digest = sum[:]
	case 512:
		sum := sha512.Sum512([]byte(input))
		digest = sum[:]
	default:
		panic("unsupported SHA2 test algorithm")
	}
	return hex.EncodeToString(digest)
}
