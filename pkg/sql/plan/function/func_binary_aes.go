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
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha512"
	"io"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/crypto/pbkdf2"
)

// generateAESKey generates an AES key using MySQL-style XOR folding.
func generateAESKey(key []byte, keyLen int) ([]byte, error) {
	if keyLen != 16 && keyLen != 24 && keyLen != 32 {
		return nil, moerr.NewInvalidInputNoCtx("unsupported aes key length")
	}
	out := make([]byte, keyLen)
	for i, b := range key {
		out[i%keyLen] ^= b
	}
	return out, nil
}

// pkcs7Padding adds PKCS7 padding to the data
func pkcs7Padding(data []byte, blockSize int) []byte {
	padding := blockSize - len(data)%blockSize
	out := make([]byte, len(data)+padding)
	copy(out, data)
	for i := len(data); i < len(out); i++ {
		out[i] = byte(padding)
	}
	return out
}

// pkcs7Unpadding removes PKCS7 padding from the data
func pkcs7Unpadding(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid padding")
	}
	padding := int(data[len(data)-1])
	if padding > aes.BlockSize || padding > len(data) || padding == 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid padding")
	}
	// Verify padding
	for i := len(data) - padding; i < len(data); i++ {
		if data[i] != byte(padding) {
			return nil, moerr.NewInvalidInputNoCtx("invalid padding")
		}
	}
	return data[:len(data)-padding], nil
}

// encryptECB encrypts data using AES-ECB mode
func encryptECB(plaintext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// Add PKCS7 padding
	padded := pkcs7Padding(plaintext, aes.BlockSize)

	// Encrypt each block independently (ECB mode)
	for i := 0; i < len(padded); i += aes.BlockSize {
		block.Encrypt(padded[i:i+aes.BlockSize], padded[i:i+aes.BlockSize])
	}

	return padded, nil
}

// decryptECB decrypts data using AES-ECB mode
func decryptECB(ciphertext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// Check that ciphertext length is a multiple of block size
	if len(ciphertext)%aes.BlockSize != 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid ciphertext length")
	}

	// Decrypt each block independently (ECB mode)
	plaintext := make([]byte, len(ciphertext))
	for i := 0; i < len(ciphertext); i += aes.BlockSize {
		block.Decrypt(plaintext[i:i+aes.BlockSize], ciphertext[i:i+aes.BlockSize])
	}

	// Remove PKCS7 padding
	return pkcs7Unpadding(plaintext)
}

// encryptCBC encrypts data using AES-CBC mode
func encryptCBC(plaintext, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(iv) < aes.BlockSize {
		return nil, moerr.NewInvalidInputNoCtx("invalid iv length")
	}
	padded := pkcs7Padding(plaintext, aes.BlockSize)
	mode := cipher.NewCBCEncrypter(block, iv[:aes.BlockSize])
	mode.CryptBlocks(padded, padded)
	return padded, nil
}

// decryptCBC decrypts data using AES-CBC mode
func decryptCBC(ciphertext, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(iv) < aes.BlockSize {
		return nil, moerr.NewInvalidInputNoCtx("invalid iv length")
	}
	if len(ciphertext)%aes.BlockSize != 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid ciphertext length")
	}
	plaintext := make([]byte, len(ciphertext))
	mode := cipher.NewCBCDecrypter(block, iv[:aes.BlockSize])
	mode.CryptBlocks(plaintext, ciphertext)
	return pkcs7Unpadding(plaintext)
}

type aesModeInfo struct {
	keyLen  int
	needsIV bool
	mode    string
}

func validateAESIV(functionName string, modeInfo aesModeInfo, hasIV, nullIV bool, iv []byte) error {
	if !modeInfo.needsIV {
		return nil
	}
	if !hasIV {
		return moerr.NewWrongParamCountToNativeFctNoCtx(functionName)
	}
	if nullIV || len(iv) < aes.BlockSize {
		return moerr.NewAESInvalidIVNoCtx(functionName, aes.BlockSize)
	}
	return nil
}

func getAESMode(proc *process.Process) (aesModeInfo, error) {
	mode := "aes-128-ecb"
	if proc != nil && proc.GetResolveVariableFunc() != nil {
		if v, err := proc.GetResolveVariableFunc()("block_encryption_mode", true, false); err == nil && v != nil {
			if s, ok := v.(string); ok && s != "" {
				mode = s
			}
		}
	}
	parts := strings.Split(strings.ToLower(mode), "-")
	if len(parts) == 3 && parts[0] == "aes" {
		var keyLen int
		switch parts[1] {
		case "128":
			keyLen = 16
		case "192":
			keyLen = 24
		case "256":
			keyLen = 32
		}
		if keyLen != 0 {
			switch parts[2] {
			case "ecb", "cbc", "cfb1", "cfb8", "cfb128", "ofb":
				return aesModeInfo{keyLen: keyLen, needsIV: parts[2] != "ecb", mode: parts[2]}, nil
			}
		}
	}
	return aesModeInfo{}, moerr.NewInvalidInputNoCtx("unsupported block_encryption_mode")
}

// MySQL bounds the iteration argument to five bytes, then uses atoi semantics.
// Parse the decimal prefix (including ASCII whitespace/sign) without permitting
// overflow or an unbounded amount of work.
func aesIterations(value []byte) (int, error) {
	if len(value) > 5 {
		return 0, moerr.NewInvalidInputNoCtx("invalid AES KDF iterations")
	}
	i := 0
	for i < len(value) && (value[i] == ' ' || value[i] >= '\t' && value[i] <= '\r') {
		i++
	}
	negative := false
	if i < len(value) && (value[i] == '+' || value[i] == '-') {
		negative = value[i] == '-'
		i++
	}
	n := 0
	for i < len(value) && value[i] >= '0' && value[i] <= '9' {
		n = n*10 + int(value[i]-'0')
		i++
	}
	if negative || n < 1000 || n > 65535 {
		return 0, moerr.NewInvalidInputNoCtx("invalid AES KDF iterations")
	}
	return n, nil
}

// deriveAESKey uses original key bytes for KDFs, never the legacy XOR-folded key.
// NULL options are rejected by the vector adapter; omitted options are empty.
func deriveAESKey(ctx context.Context, key []byte, keyLen int, options [][]byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(options) == 0 {
		return generateAESKey(key, keyLen)
	}
	if len(options) > 3 {
		return nil, moerr.NewInvalidInputNoCtx("invalid AES KDF options")
	}
	for _, option := range options {
		if len(option) > 255 {
			return nil, moerr.NewInvalidInputNoCtx("AES KDF option exceeds 255 bytes")
		}
	}
	name := string(options[0])
	if name != "hkdf" && name != "pbkdf2_hmac" {
		return nil, moerr.NewInvalidInputNoCtx("invalid AES KDF name")
	}
	var salt, info []byte
	if len(options) > 1 {
		salt = options[1]
	}
	if len(options) > 2 {
		info = options[2]
	}
	if name == "hkdf" {
		out := make([]byte, keyLen)
		_, err := io.ReadFull(hkdf.New(sha512.New, key, salt, info), out)
		return out, err
	}
	iterations := 1000
	if len(options) > 2 {
		var err error
		iterations, err = aesIterations(info)
		if err != nil {
			return nil, err
		}
	}
	// One SHA-512 output block, at most 65535 iterations; no abandoned goroutine.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return pbkdf2.Key(key, salt, iterations, keyLen, sha512.New), nil
}

// cryptAESStream does not pad: CFB and OFB preserve even empty/partial lengths.
// Each row owns its output and feedback; borrowed input/key/IV remain unchanged.
func cryptAESStream(ctx context.Context, input, key, iv []byte, mode string, decrypt bool) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(iv) < aes.BlockSize {
		return nil, moerr.NewInvalidInputNoCtx("invalid iv length")
	}
	out := make([]byte, len(input))
	switch mode {
	case "cfb128":
		var stream cipher.Stream
		if decrypt {
			stream = cipher.NewCFBDecrypter(block, iv[:aes.BlockSize])
		} else {
			stream = cipher.NewCFBEncrypter(block, iv[:aes.BlockSize])
		}
		stream.XORKeyStream(out, input)
	case "ofb":
		cipher.NewOFB(block, iv[:aes.BlockSize]).XORKeyStream(out, input)
	case "cfb1", "cfb8":
		var feedback, encrypted [aes.BlockSize]byte
		copy(feedback[:], iv)
		for i, b := range input {
			if i%4096 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			if mode == "cfb8" {
				block.Encrypt(encrypted[:], feedback[:])
				out[i] = b ^ encrypted[0]
				next := out[i]
				if decrypt {
					next = b
				}
				copy(feedback[:], feedback[1:])
				feedback[aes.BlockSize-1] = next
			} else {
				// CFB1 consumes each byte MSB first and always feeds back ciphertext.
				for bit := 7; bit >= 0; bit-- {
					block.Encrypt(encrypted[:], feedback[:])
					next := ((b >> uint(bit)) ^ (encrypted[0] >> 7)) & 1
					out[i] |= next << uint(bit)
					if decrypt {
						next = (b >> uint(bit)) & 1
					}
					for j := 0; j < aes.BlockSize-1; j++ {
						feedback[j] = feedback[j]<<1 | feedback[j+1]>>7
					}
					feedback[aes.BlockSize-1] = feedback[aes.BlockSize-1]<<1 | next
				}
			}
		}
	default:
		return nil, moerr.NewInvalidInputNoCtx("unsupported AES stream mode")
	}
	return out, nil
}

// aesOverloads keeps IDs 0..7 for the existing 2/3-argument forms. The KDF
// forms append IDs 8..19. Session mode must be read on each execution, including
// prepared executions after SET, so none of the forms may be constant-folded.
func aesOverloads(decrypt bool) []overload {
	inputs := []types.T{types.T_varchar, types.T_char, types.T_text, types.T_blob}
	operation := AESEncrypt
	resultType := aesEncryptReturnType
	if decrypt {
		inputs = []types.T{types.T_blob, types.T_varchar, types.T_char, types.T_text}
		operation = AESDecrypt
		resultType = func([]types.Type) types.Type { return types.T_blob.ToType() }
	}
	result := make([]overload, 0, 20)
	for arity := 2; arity <= 6; arity++ {
		for _, input := range inputs {
			args := make([]types.T, arity)
			args[0] = input
			for i := 1; i < arity; i++ {
				args[i] = types.T_varchar
			}
			result = append(result, overload{
				overloadId: len(result), args: args, retType: resultType,
				newOp: func() executeLogicOfOverload { return operation }, volatile: true,
			})
		}
	}
	return result
}

func AESEncrypt(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return evalAES(ivecs, result, proc, length, selectList, false)
}

func AESDecrypt(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return evalAES(ivecs, result, proc, length, selectList, true)
}

func evalAES(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList, decrypt bool) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	params := make([]vector.FunctionParameterWrapper[types.Varlena], len(ivecs))
	for i, v := range ivecs {
		params[i] = vector.GenerateFunctionStrParameter(v)
	}
	functionName := "aes_encrypt"
	if decrypt {
		functionName = "aes_decrypt"
	}
	modeInfo, modeErr := getAESMode(proc)
	for i := uint64(0); i < uint64(length); i++ {
		if selectList != nil && (selectList.IgnoreAllRow() || selectList.Contains(i)) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if err := proc.Ctx.Err(); err != nil {
			return err
		}
		input, nullInput := params[0].GetStrValue(i)
		key, nullKey := params[1].GetStrValue(i)
		if nullInput || nullKey || modeErr != nil {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		var iv []byte
		var nullIV bool
		hasIV := len(params) >= 3
		if hasIV {
			iv, nullIV = params[2].GetStrValue(i)
		}
		if err := validateAESIV(functionName, modeInfo, hasIV, nullIV, iv); err != nil {
			return err
		}
		var options [3][]byte
		count := 0
		for j := 3; j < len(params); j++ {
			value, isNull := params[j].GetStrValue(i)
			if isNull {
				return moerr.NewInvalidInputNoCtx("AES KDF options must not be NULL")
			}
			options[count] = value
			count++
		}
		aesKey, err := deriveAESKey(proc.Ctx, key, modeInfo.keyLen, options[:count])
		if err != nil {
			return err
		}
		var output []byte
		switch modeInfo.mode {
		case "ecb":
			if decrypt {
				output, err = decryptECB(input, aesKey)
			} else {
				output, err = encryptECB(input, aesKey)
			}
		case "cbc":
			if decrypt {
				output, err = decryptCBC(input, aesKey, iv)
			} else {
				output, err = encryptCBC(input, aesKey, iv)
			}
		default:
			output, err = cryptAESStream(proc.Ctx, input, aesKey, iv, modeInfo.mode, decrypt)
		}
		// Cancellation is an execution error, never a row-level decrypt NULL.
		if ctxErr := proc.Ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if appendErr := rs.AppendBytes(output, err != nil); appendErr != nil {
			return appendErr
		}
	}
	return nil
}
