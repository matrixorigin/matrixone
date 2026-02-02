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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newAESProcess(t *testing.T, mode string) *process.Process {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		if varName == "block_encryption_mode" {
			return mode, nil
		}
		return "", nil
	})
	return proc
}

func TestAESEncryptDecryptECB(t *testing.T) {
	proc := newAESProcess(t, "aes-128-ecb")
	plain := "hello"
	key := "secret-key"

	aesKey, err := generateAESKey([]byte(key), 16)
	require.NoError(t, err)
	ciphertext, err := encryptECB([]byte(plain), aesKey)
	require.NoError(t, err)

	encryptCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{plain}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{key}, []bool{false}),
		},
		NewFunctionTestResult(types.T_blob.ToType(), false, []string{string(ciphertext)}, []bool{false}),
		AESEncrypt,
	)
	ok, info := encryptCase.Run()
	require.True(t, ok, fmt.Sprintf("encrypt ecb failed: %s", info))

	decryptCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_blob.ToType(), []string{string(ciphertext)}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{key}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{plain}, []bool{false}),
		AESDecrypt,
	)
	ok, info = decryptCase.Run()
	require.True(t, ok, fmt.Sprintf("decrypt ecb failed: %s", info))
}

func TestAESEncryptDecryptCBC(t *testing.T) {
	proc := newAESProcess(t, "aes-256-cbc")
	plain := "hello cbc"
	key := "secret-key-for-cbc"
	iv := "0123456789abcdef"

	aesKey, err := generateAESKey([]byte(key), 32)
	require.NoError(t, err)
	ciphertext, err := encryptCBC([]byte(plain), aesKey, []byte(iv))
	require.NoError(t, err)

	encryptCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{plain}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{key}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{iv}, []bool{false}),
		},
		NewFunctionTestResult(types.T_blob.ToType(), false, []string{string(ciphertext)}, []bool{false}),
		AESEncrypt,
	)
	ok, info := encryptCase.Run()
	require.True(t, ok, fmt.Sprintf("encrypt cbc failed: %s", info))

	decryptCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_blob.ToType(), []string{string(ciphertext)}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{key}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{iv}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{plain}, []bool{false}),
		AESDecrypt,
	)
	ok, info = decryptCase.Run()
	require.True(t, ok, fmt.Sprintf("decrypt cbc failed: %s", info))
}

func TestAESEncryptCBCMissingIV(t *testing.T) {
	proc := newAESProcess(t, "aes-256-cbc")
	plain := "missing iv"
	key := "secret-key-for-cbc"

	encryptCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{plain}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{key}, []bool{false}),
		},
		NewFunctionTestResult(types.T_blob.ToType(), false, []string{""}, []bool{true}),
		AESEncrypt,
	)
	ok, info := encryptCase.Run()
	require.True(t, ok, fmt.Sprintf("encrypt cbc missing iv failed: %s", info))
}

func TestAESInvalidModeReturnsNull(t *testing.T) {
	proc := newAESProcess(t, "aes-999-foo")

	encryptCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"abc"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"key"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_blob.ToType(), false, []string{""}, []bool{true}),
		AESEncrypt,
	)
	ok, info := encryptCase.Run()
	require.True(t, ok, fmt.Sprintf("encrypt invalid mode failed: %s", info))

	decryptCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_blob.ToType(), []string{"abc"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"key"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}),
		AESDecrypt,
	)
	ok, info = decryptCase.Run()
	require.True(t, ok, fmt.Sprintf("decrypt invalid mode failed: %s", info))
}

func TestAESDecryptInvalidCiphertextReturnsNull(t *testing.T) {
	proc := newAESProcess(t, "aes-128-ecb")
	invalid := string(make([]byte, 15))

	decryptCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_blob.ToType(), []string{invalid}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"key"}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}),
		AESDecrypt,
	)
	ok, info := decryptCase.Run()
	require.True(t, ok, fmt.Sprintf("decrypt invalid ciphertext failed: %s", info))
}

func TestAESCBCIVTooShortReturnsNull(t *testing.T) {
	proc := newAESProcess(t, "aes-256-cbc")
	iv := "short"

	encryptCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"abc"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"key"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{iv}, []bool{false}),
		},
		NewFunctionTestResult(types.T_blob.ToType(), false, []string{""}, []bool{true}),
		AESEncrypt,
	)
	ok, info := encryptCase.Run()
	require.True(t, ok, fmt.Sprintf("encrypt short iv failed: %s", info))

	decryptCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_blob.ToType(), []string{"abc"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"key"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{iv}, []bool{false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}),
		AESDecrypt,
	)
	ok, info = decryptCase.Run()
	require.True(t, ok, fmt.Sprintf("decrypt short iv failed: %s", info))
}

func TestGenerateAESKeyEdgeCases(t *testing.T) {
	zero, err := generateAESKey([]byte{}, 16)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 16), zero)

	key16 := []byte("0123456789abcdef")
	k16, err := generateAESKey(key16, 16)
	require.NoError(t, err)
	require.Equal(t, key16, k16)

	key32 := []byte("0123456789abcdef0123456789abcdef")
	k32, err := generateAESKey(key32, 32)
	require.NoError(t, err)
	require.Equal(t, key32, k32)

	longKey := make([]byte, 32)
	for i := 0; i < 32; i++ {
		longKey[i] = byte(i)
	}
	expect := make([]byte, 16)
	for i := 0; i < 16; i++ {
		expect[i] = byte(i) ^ byte(i+16)
	}
	kfold, err := generateAESKey(longKey, 16)
	require.NoError(t, err)
	require.Equal(t, expect, kfold)
}
