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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// isBinaryStringVector reports whether a string argument has byte-string
// semantics. The static type is authoritative; vectors carry no per-row
// string domain. isBin is the caller-published binary-literal flag of the
// argument (vector.FunctionResultWrapper.ArgumentIsBin).
func isBinaryStringVector(vec *vector.Vector, isBin bool) bool {
	if vec == nil {
		return false
	}
	return types.StaticStringDomain(*vec.GetType()) == types.StringDomainBinary || isBin
}

// IsBinaryCharacterType identifies byte strings carried in CHAR/VARCHAR,
// the physical representation of SQL HEX and BIT literals and their aliases.
func IsBinaryCharacterType(typ types.Type) bool {
	return typ.Charset == types.CharsetBinary && (typ.Oid == types.T_char || typ.Oid == types.T_varchar)
}

func stringCharsetAndCollationName(typ types.Type) (charset, collation string) {
	if !typ.Oid.IsMySQLString() || types.StaticStringDomain(typ) == types.StringDomainBinary {
		return "binary", "binary"
	}
	switch typ.Charset {
	case types.CharsetUTF8:
		return "utf8mb4", "utf8mb4_general_ci"
	case types.CharsetUTF8MB3UnicodeCI:
		return "utf8", "utf8_unicode_ci"
	case types.CharsetUTF8MB4UnicodeCI:
		return "utf8mb4", "utf8mb4_unicode_ci"
	case types.CharsetUTF8MB4Bin:
		return "utf8mb4", "utf8mb4_bin"
	default:
		// CharsetLegacy and unknown text identities retain the protocol's
		// compatibility fallback rather than inventing a new public identity.
		return "utf8", "utf8_general_ci"
	}
}

func isExplicitUTF8Charset(charset uint8) bool {
	return charset == types.CharsetUTF8 || charset == types.CharsetUTF8MB4Bin ||
		types.IsUnicodeCollation(charset)
}

func charsetAndCollationTypeMatch(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) == 1 {
		return newCheckResultWithSuccess(0)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func opUnaryBytesToFixedByStringDomain[T types.FixedSizeTExceptStrType](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	textFn func([]byte) T,
	binaryFn func([]byte) T,
) error {
	fn := textFn
	if isBinaryStringVector(parameters[0], result.ArgumentIsBin(0)) {
		fn = binaryFn
	}
	return opUnaryBytesToFixed[T](parameters, result, proc, length, fn)
}

func opUnaryBytesToBytesByStringDomain(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	textFn func([]byte) []byte,
	binaryFn func([]byte) []byte,
) error {
	fn := textFn
	if isBinaryStringVector(parameters[0], result.ArgumentIsBin(0)) {
		fn = binaryFn
	}
	return opUnaryBytesToBytes(parameters, result, proc, length, fn)
}
