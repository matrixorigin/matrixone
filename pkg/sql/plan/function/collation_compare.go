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
	"github.com/matrixorigin/matrixone/pkg/common/collationkey"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// collationKeyTextDomain resolves the explicit text domains that have a
// frozen v2 key contract. CharsetLegacy is intentionally excluded: a zero
// charset is historical bytewise metadata and must not be reinterpreted by a
// new comparison kernel.
func collationKeyTextDomain(left, right types.Type) (collationkey.Domain, bool) {
	// comparisonTypeCastRule aligns both operands before normal execution.
	// Keep this check for direct callers and tests that bypass the binder.
	if left.Charset != right.Charset || !isCollationKeyTextType(left.Oid) || !isCollationKeyTextType(right.Oid) {
		return collationkey.Domain{}, false
	}
	switch left.Charset {
	case types.CharsetUTF8:
		return collationkey.Domain{
			Type: collationkey.Text, Charset: collationkey.CharsetUTF8,
			Unit: collationkey.PrefixCharacters,
		}, true
	case types.CharsetUTF8MB4Bin:
		return collationkey.Domain{
			Type: collationkey.Text, Charset: collationkey.CharsetUTF8MB4Bin,
			Unit: collationkey.PrefixCharacters,
		}, true
	default:
		return collationkey.Domain{}, false
	}
}

func isCollationKeyTextType(typ types.T) bool {
	// CHAR, binary strings, and JSON deliberately stay on their existing
	// conservative paths. Their comparison/storage contracts are not the
	// VARCHAR/TEXT v2 identity contract.
	return typ == types.T_varchar || typ == types.T_text
}

// CollationKeyEqual evaluates an already type-aligned text comparison with
// the same normalization that produces a persisted v2 key. It is an
// internal adapter: legacy equality callers continue to use their existing
// bytewise implementation until the relation/version gate is wired.
func CollationKeyEqual(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	domain, ok := collationKeyTextDomain(*parameters[0].GetType(), *parameters[1].GetType())
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "collation comparison domains are not aligned")
	}
	return opBinaryBytesBytesToFixedWithErrorCheck(
		parameters, result, proc, length,
		func(left, right []byte) (bool, error) {
			return collationkey.Equal(domain, left, right)
		}, selectList)
}

// CollationKeyNullSafeEqual is the NULL-safe counterpart of CollationKeyEqual.
// It shares the same domain and normalization path while preserving SQL
// <=> semantics for NULL values.
func CollationKeyNullSafeEqual(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	domain, ok := collationKeyTextDomain(*parameters[0].GetType(), *parameters[1].GetType())
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "collation comparison domains are not aligned")
	}
	return opBinaryBytesBytesToFixedNullSafeWithErrorCheck(
		parameters, result, proc, length,
		func(left, right []byte) (bool, error) {
			return collationkey.Equal(domain, left, right)
		}, selectList)
}

func opBinaryBytesBytesToFixedNullSafeWithErrorCheck(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	cmpFn func(v1, v2 []byte) (bool, error),
	selectList *FunctionSelectList,
) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedColNoTypeCheck[bool](rsVec)

	// Result of <=> is never NULL. Keep the same masked-row behavior as the
	// existing NULL-safe bytewise helper.
	rsVec.GetNulls().Reset()
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		switch {
		case null1 && null2:
			rss[i] = true
		case null1 || null2:
			rss[i] = false
		default:
			matched, err := cmpFn(v1, v2)
			if err != nil {
				return err
			}
			rss[i] = matched
		}
	}
	return nil
}
