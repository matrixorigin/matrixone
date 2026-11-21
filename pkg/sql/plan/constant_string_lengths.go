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

package plan

import (
	"context"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// foldConstantStringLengths reduces exact length expressions before the
// speculative evaluator admits string allocations. It only changes the proof
// copy; expressions that may warn, fail, or have unknown lengths are retained.
func foldConstantStringLengths(expr *plan.Expr, ctx context.Context) {
	if expr == nil || ctx.Err() != nil {
		return
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	if (id == function.LENGTH || id == function.LENGTH_UTF8 || id == function.BIT_LENGTH) &&
		len(fn.Args) == 1 && expr.Typ.Id == int32(types.T_int64) &&
		function.GetFunctionIsProvenPureFoldableById(fn.Func.Obj) {
		arg := fn.Args[0]
		characters := id == function.LENGTH_UTF8 &&
			!stringExprUsesBytes(arg)
		measure, ok := measureConstantString(ctx, arg, characters)
		if ok && (characters || measure.exactBytes) {
			length := measure.bytes
			switch {
			case characters:
				length = measure.characters
			case id == function.BIT_LENGTH:
				length *= 8
			}
			expr.Expr = &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: length}}}
			return
		}
	}
	for _, arg := range fn.Args {
		foldConstantStringLengths(arg, ctx)
	}
}

type constantStringMeasure struct {
	bytes, characters int64
	// bytes is an upper bound when a mixed-width slice gives us only its rune count.
	exactBytes bool
	runeWidth  int64
}

// measureConstantString computes exact sizes for producers whose result never
// needs to be built to find its length. In character mode, valid UTF-8 ensures
// rune counts compose across string boundaries.
func measureConstantString(ctx context.Context, expr *plan.Expr, characters bool) (constantStringMeasure, bool) {
	if expr == nil || ctx.Err() != nil || !types.T(expr.Typ.Id).IsMySQLString() {
		return constantStringMeasure{}, false
	}
	if lit := expr.GetLit(); lit != nil {
		value, ok := lit.Value.(*plan.Literal_Sval)
		if lit.Isnull || lit.Src != nil || !ok || len(value.Sval) > maxSpeculativeFoldLiteralBytes {
			return constantStringMeasure{}, false
		}
		result := constantStringMeasure{bytes: int64(len(value.Sval)), exactBytes: true}
		if characters {
			if lit.IsBin {
				return constantStringMeasure{}, false
			}
			for rest := value.Sval; len(rest) > 0; {
				r, width := utf8.DecodeRuneInString(rest)
				if r == utf8.RuneError && width == 1 {
					return constantStringMeasure{}, false
				}
				if result.characters == 0 {
					result.runeWidth = int64(width)
				} else if result.runeWidth != int64(width) {
					result.runeWidth = 0
				}
				result.characters++
				rest = rest[width:]
			}
		}
		return result, true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || !function.GetFunctionIsProvenPureFoldableById(fn.Func.Obj) {
		return constantStringMeasure{}, false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	switch id {
	case function.SPACE:
		limit := min(int64(types.MaxBlobLen), mpool.MaxAllocationSize())
		if len(fn.Args) == 1 {
			if n, ok := speculativeConstantInt64(fn.Args[0]); ok && n >= 0 && n <= limit {
				return constantStringMeasure{bytes: n, characters: n, exactBytes: true, runeWidth: 1}, true
			}
		}
	case function.REPEAT:
		if len(fn.Args) != 2 {
			break
		}
		source, ok := measureConstantString(ctx, fn.Args[0], characters)
		if !ok {
			break
		}
		n, ok := speculativeConstantInt64(fn.Args[1])
		if !ok || n < 0 {
			break
		}
		limit := min(function.StringFunctionResultByteLimit(types.T(expr.Typ.Id)), mpool.MaxAllocationSize())
		if source.bytes == 0 || n == 0 {
			return constantStringMeasure{exactBytes: true}, true
		}
		if n <= limit/source.bytes {
			source.bytes *= n
			source.characters *= n
			return source, true
		}
	case function.REVERSE:
		if len(fn.Args) != 1 {
			break
		}
		if stringExprUsesBytes(fn.Args[0]) {
			// Byte reversal can break UTF-8, but it preserves byte length.
			if !characters {
				return measureConstantString(ctx, fn.Args[0], false)
			}
		} else {
			// Text reversal re-encodes runes. Even a byte-length observer
			// needs valid UTF-8 before assuming that the byte length is stable.
			return measureConstantString(ctx, fn.Args[0], true)
		}
	case function.LEFT, function.RIGHT, function.SUBSTRING:
		return measureConstantStringSlice(ctx, id, fn.Args, characters)
	case function.CONCAT:
		if len(fn.Args) == 0 {
			break
		}
		// Use the supported large string range, even when CONCAT can
		// admit a larger allocation in its normal execution path.
		limit := min(int64(types.MaxBlobLen), mpool.MaxAllocationSize())
		result := constantStringMeasure{exactBytes: true}
		for _, arg := range fn.Args {
			item, ok := measureConstantString(ctx, arg, characters)
			if !ok || item.bytes > limit-result.bytes {
				return constantStringMeasure{}, false
			}
			result.bytes += item.bytes
			result.exactBytes = result.exactBytes && item.exactBytes
			if item.characters > 0 {
				if result.characters == 0 {
					result.runeWidth = item.runeWidth
				} else if result.runeWidth != item.runeWidth {
					result.runeWidth = 0
				}
				result.characters += item.characters
			}
		}
		return result, true
	}
	return constantStringMeasure{}, false
}

// measureConstantStringSlice applies the executor's interval to a proved
// input. Slicing mixed-width text gives an exact character count even when
// finding the exact byte count would require scanning the resulting string.
func measureConstantStringSlice(ctx context.Context, id int32, args []*plan.Expr, characters bool) (constantStringMeasure, bool) {
	if len(args) != 2 && (id != function.SUBSTRING || len(args) != 3) {
		return constantStringMeasure{}, false
	}
	binary := stringExprUsesBytes(args[0])
	if binary && characters {
		// A byte slice can break UTF-8 boundaries before a text observer.
		return constantStringMeasure{}, false
	}
	input, ok := measureConstantString(ctx, args[0], !binary)
	if !ok || binary && !input.exactBytes {
		return constantStringMeasure{}, false
	}
	count, ok := speculativeConstantInt64(args[1])
	if !ok {
		return constantStringMeasure{}, false
	}
	total := input.characters
	if binary {
		total = input.bytes
	}
	var size int64
	if id == function.SUBSTRING {
		var length int64
		if len(args) == 3 {
			length, ok = speculativeConstantInt64(args[2])
			if !ok {
				return constantStringMeasure{}, false
			}
		}
		start, end := function.StringSliceBounds(int(total), count, length, len(args) == 3)
		size = int64(end - start)
	} else {
		// LEFT and RIGHT only differ in which end they select.
		size = min(total, max(int64(0), count))
	}
	switch {
	case size == 0:
		return constantStringMeasure{exactBytes: true}, true
	case binary:
		return constantStringMeasure{bytes: size, exactBytes: true}, true
	case size == total:
		return input, true
	case input.runeWidth != 0:
		return constantStringMeasure{
			bytes: size * input.runeWidth, characters: size, exactBytes: true, runeWidth: input.runeWidth,
		}, true
	default:
		return constantStringMeasure{bytes: min(input.bytes, size*utf8.UTFMax), characters: size}, true
	}
}
