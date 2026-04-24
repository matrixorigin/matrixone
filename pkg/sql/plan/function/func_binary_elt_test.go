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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestElt(t *testing.T) {
	proc := testutil.NewProcess(t)

	// ELT(1, 'a', 'b', 'c') => 'a'
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"a"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"b"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"c"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"a"}, []bool{false}),
		Elt,
	)
	ok, info := tc.Run()
	require.True(t, ok, fmt.Sprintf("elt(1) failed: %s", info))

	// ELT(2, 'a', 'b', 'c') => 'b'
	tc2 := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"a"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"b"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"c"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"b"}, []bool{false}),
		Elt,
	)
	ok, info = tc2.Run()
	require.True(t, ok, fmt.Sprintf("elt(2) failed: %s", info))

	// ELT(0, 'a') => NULL (out of range)
	tc3 := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"a"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}),
		Elt,
	)
	ok, info = tc3.Run()
	require.True(t, ok, fmt.Sprintf("elt(0) failed: %s", info))

	// ELT(NULL, 'a') => NULL
	tc4 := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"a"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}),
		Elt,
	)
	ok, info = tc4.Run()
	require.True(t, ok, fmt.Sprintf("elt(null) failed: %s", info))

	// ELT with uint64 index
	tc5 := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"x"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"x"}, []bool{false}),
		Elt,
	)
	ok, info = tc5.Run()
	require.True(t, ok, fmt.Sprintf("elt(uint64) failed: %s", info))
}

func TestEltCheck(t *testing.T) {
	// Too few args
	r := eltCheck(nil, []types.Type{types.T_int64.ToType()})
	require.Equal(t, failedFunctionParametersWrong, r.status)

	// Normal case
	r = eltCheck(nil, []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()})
	require.NotEqual(t, failedFunctionParametersWrong, r.status)

	// Bool index should cast
	r = eltCheck(nil, []types.Type{types.T_bool.ToType(), types.T_varchar.ToType()})
	require.NotEqual(t, failedFunctionParametersWrong, r.status)
}

func TestMakeSet(t *testing.T) {
	proc := testutil.NewProcess(t)

	// MAKE_SET(5, 'a', 'b', 'c') => 'a,c' (bits 0 and 2 set)
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{5}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"a"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"b"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"c"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"a,c"}, []bool{false}),
		MakeSet,
	)
	ok, info := tc.Run()
	require.True(t, ok, fmt.Sprintf("make_set(5) failed: %s", info))

	// MAKE_SET(0, 'a', 'b') => '' (no bits set)
	tc2 := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"a"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"b"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{false}),
		MakeSet,
	)
	ok, info = tc2.Run()
	require.True(t, ok, fmt.Sprintf("make_set(0) failed: %s", info))

	// MAKE_SET(NULL, 'a') => NULL
	tc3 := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"a"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}),
		MakeSet,
	)
	ok, info = tc3.Run()
	require.True(t, ok, fmt.Sprintf("make_set(null) failed: %s", info))
}

func TestExportSet(t *testing.T) {
	proc := testutil.NewProcess(t)

	// EXPORT_SET(5, 'Y', 'N', ',', 4) => 'Y,N,Y,N'
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{5}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"Y"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"N"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{","}, []bool{false}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{4}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"Y,N,Y,N"}, []bool{false}),
		ExportSet,
	)
	ok, info := tc.Run()
	require.True(t, ok, fmt.Sprintf("export_set(5) failed: %s", info))

	// EXPORT_SET(NULL, 'Y', 'N', ',', 4) => NULL
	tc2 := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, []bool{true}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"Y"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"N"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{","}, []bool{false}),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{4}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}),
		ExportSet,
	)
	ok, info = tc2.Run()
	require.True(t, ok, fmt.Sprintf("export_set(null) failed: %s", info))
}

func TestPKCS7PaddingUnpadding(t *testing.T) {
	data := []byte("hello")
	padded := pkcs7Padding(data, 16)
	require.Equal(t, 16, len(padded))

	unpadded, err := pkcs7Unpadding(padded)
	require.NoError(t, err)
	require.Equal(t, data, unpadded)

	_, err = pkcs7Unpadding([]byte{})
	require.Error(t, err)

	_, err = pkcs7Unpadding([]byte{0})
	require.Error(t, err)

	_, err = pkcs7Unpadding([]byte{5})
	require.Error(t, err)
}

func TestEncryptDecryptECBDirect(t *testing.T) {
	key, _ := generateAESKey([]byte("testkey"), 16)
	ct, err := encryptECB([]byte("hello world test"), key)
	require.NoError(t, err)

	pt, err := decryptECB(ct, key)
	require.NoError(t, err)
	require.Equal(t, "hello world test", string(pt))

	_, err = decryptECB([]byte{1, 2, 3}, key)
	require.Error(t, err)
}

func TestEncryptDecryptCBCDirect(t *testing.T) {
	key, _ := generateAESKey([]byte("testkey-for-cbc-256"), 32)
	iv := []byte("0123456789abcdef")
	ct, err := encryptCBC([]byte("hello cbc"), key, iv)
	require.NoError(t, err)

	pt, err := decryptCBC(ct, key, iv)
	require.NoError(t, err)
	require.Equal(t, "hello cbc", string(pt))

	_, err = encryptCBC([]byte("x"), key, []byte("short"))
	require.Error(t, err)

	_, err = decryptCBC(ct, key, []byte("short"))
	require.Error(t, err)

	_, err = decryptCBC([]byte{1, 2, 3}, key, iv)
	require.Error(t, err)
}

func TestGetAESMode(t *testing.T) {
	m, err := getAESMode(nil)
	require.NoError(t, err)
	require.Equal(t, 16, m.keyLen)
	require.False(t, m.needsIV)

	proc := newAESProcess(t, "aes-256-cbc")
	m, err = getAESMode(proc)
	require.NoError(t, err)
	require.Equal(t, 32, m.keyLen)
	require.True(t, m.needsIV)
	require.True(t, m.useCBC)

	proc2 := newAESProcess(t, "bad-mode")
	_, err = getAESMode(proc2)
	require.Error(t, err)
}
