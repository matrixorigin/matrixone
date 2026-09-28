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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func aesHex(t *testing.T, value string) []byte {
	t.Helper()
	b, err := hex.DecodeString(value)
	require.NoError(t, err)
	return b
}

func aesCase(t *testing.T, proc *process.Process, inputs []FunctionTestInput, expected FunctionTestResult, fn fEvalFn) FunctionTestCase {
	t.Helper()
	c := NewFunctionTestCase(proc, inputs, expected, fn)
	t.Cleanup(func() {
		c.result.Free()
		for _, p := range c.parameters {
			p.Free(proc.Mp())
		}
	})
	return c
}

// Oracle: OpenSSL 3.6.3 (9 Jun 2026), matching MySQL 8.0.45 EVP mode
// semantics. Input hex 4d61747269784f6e652041455300ff73747265616d2d6d6f6465,
// IV ASCII 0123456789abcdef; key is the first keyLen bytes of
// 0123456789abcdef0123456789abcdef. Generate with:
// printf 'MatrixOne AES\000\377stream-mode' | openssl enc -aes-<bits>-<mode>
//
//	-K <key hex> -iv 30313233343536373839616263646566 | xxd -p
//
// Omit -iv for ECB; OpenSSL calls CFB128 "cfb". No live oracle is needed.
func TestAESModeKnownAnswers(t *testing.T) {
	vectors := []struct{ mode, ciphertext string }{
		{"aes-128-ecb", "5badfd1415739b9107e0c81ac459e8109e00e6f9ee9976749bc724dec474eed2"},
		{"aes-128-cbc", "45bc4a925addbee64885c38106ccf74ee168013279256edb43f6009c6ae58ea6"},
		{"aes-128-cfb1", "74f9e1799f05f22fc8f52cf0ce8c8cd8240041268dee7fe09871"},
		{"aes-128-cfb8", "3fd2c9edd9a6ade1abfa7bda30b66f5f9eda430861789e5218f5"},
		{"aes-128-cfb128", "3f130afa77a4b26f6587592d2a094a16402eca6559ea5ef00208"},
		{"aes-128-ofb", "3f130afa77a4b26f6587592d2a094a161007b724d0bb9987cc74"},
		{"aes-192-ecb", "dcac0ea5d119cb0138f84016ff2a42b2441d3ab38118a08252dbbfee2f43f0b1"},
		{"aes-192-cbc", "d3a4b34bbbcde1ae9fb8c6cb28aca87b4a888fa247c3f8cd139c503dce43dc1c"},
		{"aes-192-cfb1", "f7e1991e4b7574f8a89967d0ac2702fe8bd2164c115881b21b67"},
		{"aes-192-cfb8", "97a7843feae0450b9443a1fe8f0c43ebb4e57d59cc0c60e7b380"},
		{"aes-192-cfb128", "970d7a5bafea3d4b833f1848e7ea8784069d7a4290cf80e8e79c"},
		{"aes-192-ofb", "970d7a5bafea3d4b833f1848e7ea87848d4ddc4f09bbd84cf74c"},
		{"aes-256-ecb", "0701dfefdddc0b965109e3a3e3ca847dd8ecb5fe4120b194c62d63248d08ec14"},
		{"aes-256-cbc", "f225b1587f6102c61756fe5a60ea10120ef8416f13dbc67ab9fb686970a0e079"},
		{"aes-256-cfb1", "b76ee843a367e202519969f6f6c24984158fb3ce1d06312d5541"},
		{"aes-256-cfb8", "b52caf30ccad332330ca3ff38da279aedc0ba13e108483fb0d08"},
		{"aes-256-cfb128", "b55dee12b57494f644bf389386dbe9463547552c3370fee980ee"},
		{"aes-256-ofb", "b55dee12b57494f644bf389386dbe9460f8ef01e87cbd8b1b77a"},
	}
	for _, tc := range vectors {
		t.Run(tc.mode, func(t *testing.T) {
			proc := newAESProcess(t, strings.ToUpper(tc.mode))
			mode, err := getAESMode(proc)
			require.NoError(t, err)
			key := "0123456789abcdef0123456789abcdef"[:mode.keyLen]
			plain, ciphertext := "MatrixOne AES\x00\xffstream-mode", string(aesHex(t, tc.ciphertext))
			for _, decrypt := range []bool{false, true} {
				input, want, fn := plain, ciphertext, fEvalFn(AESEncrypt)
				if decrypt {
					input, want, fn = ciphertext, plain, AESDecrypt
				}
				c := aesCase(t, proc, []FunctionTestInput{
					NewFunctionTestInput(types.T_blob.ToType(), []string{input, input}, nil),
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{key}, nil),
					NewFunctionTestInput(types.T_varchar.ToType(), []string{"0123456789abcdef", "0123456789abcdefignored"}, nil),
				}, NewFunctionTestResult(types.T_blob.ToType(), false, []string{want, want}, nil), fn)
				ok, info := c.Run()
				require.True(t, ok, info)
				// Re-evaluation must reset feedback and not modify any borrowed argument.
				ok, info = c.Run()
				require.True(t, ok, info)
			}
		})
	}
}

func TestAESLengthBoundariesAndOwnership(t *testing.T) {
	key, iv := []byte("0123456789abcdef"), []byte("0123456789abcdef")
	for _, mode := range []string{"ecb", "cbc", "cfb1", "cfb8", "cfb128", "ofb"} {
		for _, size := range []int{0, 1, 15, 16, 17, 33} {
			t.Run(fmt.Sprintf("%s/%d", mode, size), func(t *testing.T) {
				backing := bytes.Repeat([]byte{0xa5}, size+16)
				input := backing[:size]
				original := bytes.Clone(backing)
				var encrypted, plain []byte
				var err error
				switch mode {
				case "ecb":
					encrypted, err = encryptECB(input, key)
				case "cbc":
					encrypted, err = encryptCBC(input, key, iv)
				default:
					encrypted, err = cryptAESStream(t.Context(), input, key, iv, mode, false)
				}
				require.NoError(t, err)
				require.Equal(t, original, backing, "encryption mutated borrowed capacity")
				wantLen := size
				if mode == "ecb" || mode == "cbc" {
					wantLen = (size/16 + 1) * 16
				}
				require.Len(t, encrypted, wantLen)
				switch mode {
				case "ecb":
					plain, err = decryptECB(encrypted, key)
				case "cbc":
					plain, err = decryptCBC(encrypted, key, iv)
				default:
					plain, err = cryptAESStream(t.Context(), encrypted, key, iv, mode, true)
				}
				require.NoError(t, err)
				require.Equal(t, input, plain)
				require.Equal(t, []byte("0123456789abcdef"), key)
				require.Equal(t, key, iv)
			})
		}
	}
	for _, bad := range [][]byte{nil, {0}, {2, 3, 3}, bytes.Repeat([]byte{17}, 17), bytes.Repeat([]byte{32}, 32)} {
		_, err := pkcs7Unpadding(bad)
		require.Error(t, err)
	}
	for _, size := range []int{16, 24, 32} {
		input := make([]byte, size*2)
		for i := range input {
			input[i] = byte(i)
		}
		want := bytes.Repeat([]byte{byte(size)}, size)
		if size == 24 {
			want = aesHex(t, "181818181818181828282828282828283838383838383838")
		}
		key, err := generateAESKey(input, size)
		require.NoError(t, err)
		require.Equal(t, want, key)
	}
}

// OpenSSL 3.6.3: openssl kdf -keylen <16|24|32> -kdfopt digest:SHA512
// -kdfopt key:password -kdfopt salt:salt -kdfopt info:info HKDF;
// PBKDF2 uses -kdfopt pass:password -kdfopt salt:salt -kdfopt iter:1000.
func TestAESKDFKnownAnswers(t *testing.T) {
	for _, tc := range []struct{ name, option, hex string }{
		{"hkdf", "info", "8a1d29022550950fdd28c02e39384b00456270e09a04421a40f5e0e73c8ad012"},
		{"pbkdf2_hmac", "1000", "afe6c5530785b6cc6b1c6453384731bd5ee432ee549fd42fb6695779ad8a1c5b"},
	} {
		for _, size := range []int{16, 24, 32} {
			key, err := deriveAESKey(t.Context(), []byte("password"), size, [][]byte{[]byte(tc.name), []byte("salt"), []byte(tc.option)})
			require.NoError(t, err)
			require.Equal(t, aesHex(t, tc.hex)[:size], key)
		}
	}
	for _, tc := range []struct {
		options []string
		hex     string
	}{
		{[]string{"hkdf"}, "b78ebf8cbf4621845bb9b0531c3915d1"},
		{[]string{"hkdf", ""}, "b78ebf8cbf4621845bb9b0531c3915d1"},
		{[]string{"hkdf", "salt"}, "dd1893e26644afff749b0e665ac7fda3"},
		{[]string{"pbkdf2_hmac"}, "037e94baa9506c5ba26bd3bbaa3684b9"},
		{[]string{"pbkdf2_hmac", "salt"}, "afe6c5530785b6cc6b1c6453384731bd"},
		{[]string{"pbkdf2_hmac", "salt", "65535"}, "a338f06a90afe0c3ebe80bcb93cd572b"},
	} {
		var options [][]byte
		for _, s := range tc.options {
			options = append(options, []byte(s))
		}
		started := time.Now()
		key, err := deriveAESKey(t.Context(), []byte("password"), 16, options)
		elapsed := time.Since(started)
		require.NoError(t, err)
		require.Equal(t, aesHex(t, tc.hex), key)
		if len(tc.options) == 3 {
			t.Logf("65535-iteration SHA512 PBKDF2 observed latency: %s (not a timing assertion)", elapsed)
		}
	}
	for _, value := range []string{"1000", "65535", "1000x", "+1000", " 1000", "1000\x00"} {
		got, err := aesIterations([]byte(value))
		require.NoError(t, err, value)
		require.GreaterOrEqual(t, got, 1000)
	}
	for _, value := range []string{"", "999", "65536", "99999", "100000", "-1000", "1e3", "0x400", "1000xx"} {
		_, err := aesIterations([]byte(value))
		require.Error(t, err, value)
	}
	for _, name := range []string{"HKDF", "PBKDF2_HMAC", "", "sha512"} {
		_, err := deriveAESKey(t.Context(), nil, 16, [][]byte{[]byte(name)})
		require.Error(t, err)
	}
	for _, size := range []int{255, 256} {
		for _, position := range []int{1, 2} {
			options := [][]byte{[]byte("hkdf"), nil, nil}
			options[position] = bytes.Repeat([]byte{0xff}, size)
			_, err := deriveAESKey(t.Context(), nil, 16, options)
			if size == 255 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		}
	}
}

func TestAESKDFVectorContract(t *testing.T) {
	// Ciphertext from OpenSSL enc -aes-128-ecb, using the independently
	// derived keys above; the plaintext is ASCII "hello".
	for _, tc := range []struct {
		options []string
		hex     string
	}{
		{[]string{"hkdf"}, "7d0cfc63f883519e5b8f78de8933dee6"},
		{[]string{"pbkdf2_hmac"}, "cac24ca8c9fcd9bba907863d39d55cd8"},
		{[]string{"hkdf", "salt"}, "98b27bbb4fce2ee0d2a57e55828b8cb0"},
		{[]string{"pbkdf2_hmac", "salt"}, "8d7ee6aaba97089a79d01672f0afb145"},
		{[]string{"hkdf", "salt", "info"}, "f35282c2b466630949eb317c0f1b6f57"},
		{[]string{"pbkdf2_hmac", "salt", "1000x"}, "8d7ee6aaba97089a79d01672f0afb145"},
	} {
		for _, decrypt := range []bool{false, true} {
			data, want, fn := "hello", string(aesHex(t, tc.hex)), fEvalFn(AESEncrypt)
			if decrypt {
				data, want, fn = want, data, AESDecrypt
			}
			args := []string{data, "password", ""}
			args = append(args, tc.options...)
			var inputs []FunctionTestInput
			for _, arg := range args {
				inputs = append(inputs, NewFunctionTestInput(types.T_varchar.ToType(), []string{arg}, nil))
			}
			c := aesCase(t, newAESProcess(t, "aes-128-ecb"), inputs, NewFunctionTestResult(types.T_blob.ToType(), false, []string{want}, nil), fn)
			ok, info := c.Run()
			require.True(t, ok, info)
		}
	}
	// Bad options in masked rows must not be validated. NULL data/key wins over
	// missing/short IV and NULL KDF options. The active row uses a fixed oracle.
	for _, fn := range []fEvalFn{AESEncrypt, AESDecrypt} {
		for nullAt := 3; nullAt <= 5; nullAt++ {
			inputs := make([]FunctionTestInput, 6)
			for i, arg := range []string{"hello", "password", "", "hkdf", "salt", "info"} {
				inputs[i] = NewFunctionTestInput(types.T_varchar.ToType(), []string{arg}, []bool{i == nullAt})
			}
			c := aesCase(t, newAESProcess(t, "aes-128-ecb"), inputs, NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil), fn)
			ok, info := c.Run()
			require.True(t, ok, info)
		}
	}
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"ignored", "hello", ""}, []bool{false, false, true}),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"password", "password", "password"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"", "", ""}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"bad", "hkdf", "bad"}, nil),
	}
	c := aesCase(t, newAESProcess(t, "aes-128-ecb"), inputs, NewFunctionTestResult(types.T_blob.ToType(), false,
		[]string{"", string(aesHex(t, "7d0cfc63f883519e5b8f78de8933dee6")), ""}, []bool{true, false, true}), AESEncrypt).
		WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, true}})
	ok, info := c.Run()
	require.True(t, ok, info)
}

func TestAESBindingAndSessionMode(t *testing.T) {
	for _, decrypt := range []bool{false, true} {
		name, fid := "aes_encrypt", AES_ENCRYPT
		inputs := []types.T{types.T_varchar, types.T_char, types.T_text, types.T_blob}
		if decrypt {
			name, fid = "aes_decrypt", AES_DECRYPT
			inputs = []types.T{types.T_blob, types.T_varchar, types.T_char, types.T_text}
		}
		for arity := 2; arity <= 6; arity++ {
			for index, input := range inputs {
				args := make([]types.Type, arity)
				for i := range args {
					args[i] = types.T_varchar.ToType()
				}
				args[0] = input.ToType()
				bound, err := GetFunctionByName(t.Context(), name, args)
				require.NoError(t, err)
				require.Equal(t, encodeOverloadID(int32(fid), int32((arity-2)*4+index)), bound.GetEncodedOverloadID())
				ov, err := GetFunctionById(t.Context(), bound.GetEncodedOverloadID())
				require.NoError(t, err)
				require.True(t, ov.CannotFold(), "session mode must not freeze in a prepared expression")
				require.Contains(t, []types.T{types.T_blob, types.T_varbinary}, bound.GetReturnType().Oid)
			}
		}
		for _, arity := range []int{1, 7} {
			args := make([]types.Type, arity)
			for i := range args {
				args[i] = types.T_varchar.ToType()
			}
			_, err := GetFunctionByName(t.Context(), name, args)
			require.Error(t, err)
		}
		args := []types.Type{types.T_blob.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_int64.ToType()}
		bound, err := GetFunctionByName(t.Context(), name, args)
		require.NoError(t, err)
		cast, needed := bound.ShouldDoImplicitTypeCast()
		require.True(t, needed)
		require.Equal(t, types.T_varchar, cast[5].Oid)
		for _, binary := range []types.T{types.T_blob, types.T_varbinary, types.T_binary} {
			args := []types.Type{types.T_blob.ToType(), binary.ToType(), binary.ToType(), types.T_varchar.ToType(), binary.ToType(), binary.ToType()}
			bound, err := GetFunctionByName(t.Context(), name, args)
			require.NoError(t, err)
			_, needed := bound.ShouldDoImplicitTypeCast()
			require.False(t, needed, "binary key, IV, salt and info must not pass through a text conversion")
		}
	}
}

func TestAESRuntimeModeAndInvalidPadding(t *testing.T) {
	mode := "aes-128-ecb"
	proc := newAESProcess(t, mode)
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return mode, nil })
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"MatrixOne AES\x00\xffstream-mode"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"0123456789abcdef"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"0123456789abcdef"}, nil),
	}
	bound, err := GetFunctionByName(t.Context(), "aes_encrypt", []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()})
	require.NoError(t, err)
	ov, err := GetFunctionById(t.Context(), bound.GetEncodedOverloadID())
	require.NoError(t, err)
	fn, _, _, _ := ov.GetExecuteMethod()
	c := aesCase(t, proc, inputs, NewFunctionTestResult(types.T_blob.ToType(), false,
		[]string{string(aesHex(t, "5badfd1415739b9107e0c81ac459e8109e00e6f9ee9976749bc724dec474eed2"))}, nil), fEvalFn(fn))
	ok, info := c.Run()
	require.True(t, ok, info)
	mode = "aes-128-cfb1"
	c.expected = NewFunctionTestResult(types.T_blob.ToType(), false,
		[]string{string(aesHex(t, "74f9e1799f05f22fc8f52cf0ce8c8cd8240041268dee7fe09871"))}, nil)
	ok, info = c.Run()
	require.True(t, ok, info)
	// OpenSSL enc -aes-128-ecb -nopad -K 30313233343536373839616263646566
	// on 32 ASCII spaces: decrypts to an invalid 32-byte padding run.
	bad := string(aesHex(t, "92f3e260a3927b6043f3e17f0db5413292f3e260a3927b6043f3e17f0db54132"))
	d := aesCase(t, newAESProcess(t, "aes-128-ecb"), []FunctionTestInput{
		NewFunctionTestInput(types.T_blob.ToType(), []string{bad, "", "short"}, nil),
		NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"0123456789abcdef"}, nil),
	}, NewFunctionTestResult(types.T_blob.ToType(), false, []string{"", "", ""}, []bool{true, true, true}), AESDecrypt)
	ok, info = d.Run()
	require.True(t, ok, info)
}

// A deterministic context allows testing cancellation polls without sleeps or
// making a scheduler race the cryptographic work.
type aesCancelAfterContext struct {
	context.Context
	calls, after int
}

func (c *aesCancelAfterContext) Err() error {
	c.calls++
	if c.calls >= c.after {
		return context.Canceled
	}
	return nil
}

func TestAESCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := deriveAESKey(ctx, nil, 16, [][]byte{[]byte("pbkdf2_hmac"), nil, []byte("65535")})
	require.ErrorIs(t, err, context.Canceled)
	for _, mode := range []string{"cfb1", "cfb8"} {
		ctx := &aesCancelAfterContext{Context: t.Context(), after: 3}
		_, err := cryptAESStream(ctx, make([]byte, 4097), make([]byte, 16), make([]byte, 16), mode, false)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 3, ctx.calls)
	}
	proc := newAESProcess(t, "aes-128-ecb")
	proc.Ctx = ctx
	c := aesCase(t, proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"hello"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"password"}, nil),
	}, NewFunctionTestResult(types.T_blob.ToType(), true, nil, nil), AESEncrypt)
	require.NoError(t, c.result.PreExtendAndReset(1))
	require.ErrorIs(t, c.fn(c.parameters, c.result, proc, 1, nil), context.Canceled)
}

func TestAESAllMaskedSkipsWork(t *testing.T) {
	for _, fn := range []fEvalFn{AESEncrypt, AESDecrypt} {
		proc := newAESProcess(t, "aes-192-cfb1")
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		proc.Ctx = ctx
		// AllNull intentionally carries no bitmap. Even cancellation, invalid
		// IV and invalid KDF must not be evaluated for a wholly masked input.
		var inputs []FunctionTestInput
		for _, arg := range []string{"hello", "password", "short", "bad"} {
			inputs = append(inputs, NewFunctionTestInput(types.T_varchar.ToType(), []string{arg, arg}, nil))
		}
		c := aesCase(t, proc, inputs, NewFunctionTestResult(types.T_blob.ToType(), false,
			[]string{"", ""}, []bool{true, true}), fn).
			WithSelectList(&FunctionSelectList{AllNull: true, AnyNull: true})
		ok, info := c.Run()
		require.True(t, ok, info)
	}
}
