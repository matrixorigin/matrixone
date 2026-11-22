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
	"sort"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

const (
	maxStringLengthProofDepth = 128
	maxStringLengthProofWork  = 1 << 20
)

// foldConstantStringLengths changes only the proof copy. The original SQL
// still evaluates the original expressions and owns their diagnostics.
func foldConstantStringLengths(expr *plan.Expr, ctx context.Context) {
	proof := stringLengthProof{ctx: ctx, workLeft: maxStringLengthProofWork}
	proof.fold(expr, maxStringLengthProofDepth)
}

type stringLengthProof struct {
	ctx      context.Context
	workLeft int
}

func (p *stringLengthProof) spend(work int) bool {
	if work > p.workLeft || p.ctx.Err() != nil {
		p.workLeft = 0
		return false
	}
	p.workLeft -= work
	return true
}

func (p *stringLengthProof) fold(expr *plan.Expr, depth int) {
	if expr == nil || depth == 0 || !p.spend(1) {
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
		text := id == function.LENGTH_UTF8 && !stringExprUsesBytes(arg)
		if measure, ok := p.measure(arg, text, depth-1); ok && p.ctx.Err() == nil {
			length := measure.bytes
			switch {
			case text:
				length = measure.characters
			case id == function.BIT_LENGTH:
				length *= 8
			}
			expr.Expr = &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: length}}}
			return
		}
	}
	if depth == 1 {
		return
	}
	for _, arg := range fn.Args {
		if p.workLeft == 0 || p.ctx.Err() != nil {
			return
		}
		p.fold(arg, depth-1)
	}
}

// A text measurement stores a byte prefix for every rune boundary
// symbolically. A nil prefix denotes a uniform rune width. A byte-only
// measurement exposes no rune boundaries.
type constantStringMeasure struct {
	bytes, characters int64
	prefix            func(int64) (int64, bool)
}

func (p *stringLengthProof) byteOffset(m constantStringMeasure, characters int64) (int64, bool) {
	switch characters {
	case 0:
		return 0, true
	case m.characters:
		return m.bytes, true
	}
	if !p.spend(1) {
		return 0, false
	}
	if m.prefix == nil {
		return characters * (m.bytes / m.characters), true
	}
	return m.prefix(characters)
}

func (p *stringLengthProof) measure(expr *plan.Expr, text bool, depth int) (constantStringMeasure, bool) {
	if expr == nil || depth == 0 || !p.spend(1) || !types.T(expr.Typ.Id).IsMySQLString() ||
		text && stringExprUsesBytes(expr) {
		return constantStringMeasure{}, false
	}
	if lit := expr.GetLit(); lit != nil {
		value, ok := lit.Value.(*plan.Literal_Sval)
		if lit.Isnull || lit.Src != nil || !ok || len(value.Sval) > maxSpeculativeFoldLiteralBytes {
			return constantStringMeasure{}, false
		}
		return p.literal(value.Sval, text)
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || !function.GetFunctionIsProvenPureFoldableById(fn.Func.Obj) {
		return constantStringMeasure{}, false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	args := fn.Args
	switch id {
	case function.SPACE:
		limit := min(int64(types.MaxBlobLen), mpool.MaxAllocationSize())
		if len(args) == 1 {
			if n, ok := speculativeConstantInt64Within(args[0], depth-1, p.spend); ok && n >= 0 && n <= limit {
				return constantStringMeasure{bytes: n, characters: n}, true
			}
		}
	case function.REPEAT:
		if len(args) == 2 {
			limit := min(function.StringFunctionResultByteLimit(types.T(expr.Typ.Id)), mpool.MaxAllocationSize())
			return p.repeat(args[0], args[1], text, depth-1, limit)
		}
	case function.LOWER, function.UPPER:
		if len(args) == 1 {
			if stringExprUsesBytes(args[0]) {
				return p.measure(args[0], text, depth-1)
			}
			// The case mapper is one rune at a time. A valid UTF-8
			// source with one byte per rune stays ASCII after mapping.
			source, ok := p.measure(args[0], true, depth-1)
			if ok && source.bytes == source.characters {
				return source, true
			}
		}
	case function.REVERSE:
		if len(args) == 1 {
			return p.reverse(args[0], text, depth-1)
		}
	case function.LEFT, function.RIGHT, function.SUBSTRING:
		return p.slice(id, args, text, depth-1)
	case function.CONCAT:
		return p.concat(args, text, depth-1)
	}
	return constantStringMeasure{}, false
}

func (p *stringLengthProof) literal(s string, text bool) (constantStringMeasure, bool) {
	result := constantStringMeasure{bytes: int64(len(s))}
	if !text {
		return result, true
	}
	if !p.spend(len(s)) {
		return constantStringMeasure{}, false
	}
	var width int
	for rest := s; rest != ""; {
		r, n := utf8.DecodeRuneInString(rest)
		if r == utf8.RuneError && n == 1 {
			return constantStringMeasure{}, false
		}
		if result.characters == 0 {
			width = n
		} else if width != n {
			width = 0
		}
		result.characters++
		rest = rest[n:]
	}
	if width == 0 && result.characters > 0 {
		result.prefix = func(k int64) (int64, bool) {
			if !p.spend(len(s)) {
				return 0, false
			}
			for offset := range s {
				if k == 0 {
					return int64(offset), true
				}
				k--
			}
			return 0, false
		}
	}
	return result, true
}

func (p *stringLengthProof) repeat(value, count *plan.Expr, text bool, depth int, limit int64) (constantStringMeasure, bool) {
	source, ok := p.measure(value, text, depth)
	if !ok {
		return constantStringMeasure{}, false
	}
	n, ok := speculativeConstantInt64Within(count, depth, p.spend)
	if !ok || n < 0 {
		return constantStringMeasure{}, false
	}
	if source.bytes == 0 || n == 0 {
		return constantStringMeasure{}, true
	}
	if n > limit/source.bytes {
		return constantStringMeasure{}, false
	}
	result := constantStringMeasure{bytes: source.bytes * n}
	if text {
		result.characters = source.characters * n
		if source.prefix != nil {
			result.prefix = func(k int64) (int64, bool) {
				offset, ok := p.byteOffset(source, k%source.characters)
				return k/source.characters*source.bytes + offset, ok
			}
		}
	}
	return result, true
}

func (p *stringLengthProof) reverse(value *plan.Expr, text bool, depth int) (constantStringMeasure, bool) {
	if stringExprUsesBytes(value) {
		// Byte reversal preserves byte length but can break UTF-8.
		if text {
			return constantStringMeasure{}, false
		}
		return p.measure(value, false, depth)
	}
	// Text reversal re-encodes runes, so even byte length requires valid UTF-8.
	source, ok := p.measure(value, true, depth)
	if !ok || !text || source.prefix == nil {
		return source, ok
	}
	result := source
	result.prefix = func(k int64) (int64, bool) {
		offset, ok := p.byteOffset(source, source.characters-k)
		return source.bytes - offset, ok
	}
	return result, true
}

type constantStringSpan struct {
	endByte, endCharacter int64
	measure               constantStringMeasure
}

func (p *stringLengthProof) concat(args []*plan.Expr, text bool, depth int) (constantStringMeasure, bool) {
	if len(args) == 0 || !p.spend(len(args)) {
		return constantStringMeasure{}, false
	}
	limit := min(int64(types.MaxBlobLen), mpool.MaxAllocationSize())
	var result constantStringMeasure
	var spans []constantStringSpan
	if text {
		spans = make([]constantStringSpan, 0, min(len(args), 8))
	}
	for _, arg := range args {
		item, ok := p.measure(arg, text, depth)
		if !ok || item.bytes > limit-result.bytes {
			return constantStringMeasure{}, false
		}
		result.bytes += item.bytes
		if text && item.characters != 0 {
			// Charge for the span rather than sizing storage from unchecked arity.
			if !p.spend(32) {
				return constantStringMeasure{}, false
			}
			result.characters += item.characters
			spans = append(spans, constantStringSpan{
				endByte: result.bytes, endCharacter: result.characters, measure: item,
			})
		}
	}
	if len(spans) != 0 {
		result.prefix = func(k int64) (int64, bool) {
			i := sort.Search(len(spans), func(i int) bool { return spans[i].endCharacter >= k })
			span := spans[i]
			item := span.measure
			offset, ok := p.byteOffset(item, k-(span.endCharacter-item.characters))
			return span.endByte - item.bytes + offset, ok
		}
	}
	return result, true
}

func (p *stringLengthProof) slice(id int32, args []*plan.Expr, text bool, depth int) (constantStringMeasure, bool) {
	if len(args) != 2 && (id != function.SUBSTRING || len(args) != 3) {
		return constantStringMeasure{}, false
	}
	binary := stringExprUsesBytes(args[0])
	if binary && text {
		return constantStringMeasure{}, false
	}
	input, ok := p.measure(args[0], !binary, depth)
	if !ok {
		return constantStringMeasure{}, false
	}
	count, ok := speculativeConstantInt64Within(args[1], depth, p.spend)
	if !ok {
		return constantStringMeasure{}, false
	}
	total := input.characters
	if binary {
		total = input.bytes
	}
	var start, end int64
	switch id {
	case function.SUBSTRING:
		var length int64
		if len(args) == 3 {
			length, ok = speculativeConstantInt64Within(args[2], depth, p.spend)
			if !ok {
				return constantStringMeasure{}, false
			}
		}
		lo, hi := function.StringSliceBounds(int(total), count, length, len(args) == 3)
		start, end = int64(lo), int64(hi)
	case function.LEFT:
		end = min(total, max(int64(0), count))
	case function.RIGHT:
		start, end = total-min(total, max(int64(0), count)), total
	}
	switch {
	case start == end:
		return constantStringMeasure{}, true
	case binary:
		return constantStringMeasure{bytes: end - start}, true
	case start == 0 && end == total:
		return input, true
	}
	lo, ok := p.byteOffset(input, start)
	if !ok {
		return constantStringMeasure{}, false
	}
	hi, ok := p.byteOffset(input, end)
	if !ok {
		return constantStringMeasure{}, false
	}
	result := constantStringMeasure{bytes: hi - lo, characters: end - start}
	if text && input.prefix != nil {
		result.prefix = func(k int64) (int64, bool) {
			offset, ok := p.byteOffset(input, start+k)
			return offset - lo, ok
		}
	}
	return result, true
}
