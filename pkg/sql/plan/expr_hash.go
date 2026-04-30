// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"hash/fnv"
	"math"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// exprStructuralHash returns a 64-bit identity hash for a plan.Expr derived
// from its structural shape. Intended as a bucket key for deduping conjuncts
// in passes like applyDistributivity. Collisions must be resolved by the
// caller with exprStructuralEqual; two exprs with the same hash are not
// guaranteed to be equal.
//
// Compared to proto-based Marshal/String, this avoids allocating a byte
// buffer proportional to expr size. A query with deeply nested IN / OR trees
// (hundreds of conjuncts) saw ~50% CPU in marshalling before this path.
func exprStructuralHash(expr *plan.Expr) uint64 {
	h := fnv.New64a()
	hashExprInto(h, expr)
	return h.Sum64()
}

type writeByter interface {
	Write([]byte) (int, error)
}

func writeByte(h writeByter, b byte) {
	var tmp [1]byte
	tmp[0] = b
	_, _ = h.Write(tmp[:])
}

func writeUint32(h writeByter, v uint32) {
	var tmp [4]byte
	tmp[0] = byte(v)
	tmp[1] = byte(v >> 8)
	tmp[2] = byte(v >> 16)
	tmp[3] = byte(v >> 24)
	_, _ = h.Write(tmp[:])
}

func writeUint64(h writeByter, v uint64) {
	var tmp [8]byte
	for i := 0; i < 8; i++ {
		tmp[i] = byte(v >> (8 * i))
	}
	_, _ = h.Write(tmp[:])
}

const (
	tagNil uint8 = iota
	tagLit
	tagCol
	tagFn
	tagList
	tagOther
)

func hashExprInto(h writeByter, expr *plan.Expr) {
	if expr == nil {
		writeByte(h, tagNil)
		return
	}
	// Incorporate the Typ so that e.g. int64(5) and varchar("5") differ.
	writeUint32(h, uint32(expr.Typ.Id))
	writeUint32(h, uint32(expr.Typ.Width))
	writeUint32(h, uint32(expr.Typ.Scale))

	switch v := expr.Expr.(type) {
	case *plan.Expr_Lit:
		writeByte(h, tagLit)
		hashLitInto(h, v.Lit)
	case *plan.Expr_Col:
		writeByte(h, tagCol)
		if v.Col != nil {
			writeUint32(h, uint32(v.Col.RelPos))
			writeUint32(h, uint32(v.Col.ColPos))
		}
	case *plan.Expr_F:
		writeByte(h, tagFn)
		if v.F != nil {
			if v.F.Func != nil {
				_, _ = h.Write([]byte(v.F.Func.ObjName))
				writeByte(h, 0)
			}
			for _, a := range v.F.Args {
				hashExprInto(h, a)
			}
		}
	case *plan.Expr_List:
		writeByte(h, tagList)
		if v.List != nil {
			for _, e := range v.List.List {
				hashExprInto(h, e)
			}
		}
	default:
		// Uncommon variants (Sub, Vec, Max, ...) — fall back to the proto
		// binary marshaller so the bucket is still correct, just slower.
		writeByte(h, tagOther)
		if b, err := expr.Marshal(); err == nil {
			_, _ = h.Write(b)
		}
	}
}

func hashLitInto(h writeByter, lit *plan.Literal) {
	if lit == nil {
		writeByte(h, 0)
		return
	}
	if lit.Isnull {
		writeByte(h, 1)
		return
	}
	writeByte(h, 2)
	switch v := lit.Value.(type) {
	case *plan.Literal_I8Val:
		writeByte(h, 1)
		writeUint32(h, uint32(v.I8Val))
	case *plan.Literal_I16Val:
		writeByte(h, 2)
		writeUint32(h, uint32(v.I16Val))
	case *plan.Literal_I32Val:
		writeByte(h, 3)
		writeUint32(h, uint32(v.I32Val))
	case *plan.Literal_I64Val:
		writeByte(h, 4)
		writeUint64(h, uint64(v.I64Val))
	case *plan.Literal_U8Val:
		writeByte(h, 5)
		writeUint32(h, v.U8Val)
	case *plan.Literal_U16Val:
		writeByte(h, 6)
		writeUint32(h, v.U16Val)
	case *plan.Literal_U32Val:
		writeByte(h, 7)
		writeUint32(h, v.U32Val)
	case *plan.Literal_U64Val:
		writeByte(h, 8)
		writeUint64(h, v.U64Val)
	case *plan.Literal_Dval:
		writeByte(h, 9)
		writeUint64(h, math.Float64bits(v.Dval))
	case *plan.Literal_Fval:
		writeByte(h, 10)
		writeUint32(h, math.Float32bits(v.Fval))
	case *plan.Literal_Sval:
		writeByte(h, 11)
		_, _ = h.Write([]byte(v.Sval))
		writeByte(h, 0)
	case *plan.Literal_Bval:
		writeByte(h, 12)
		if v.Bval {
			writeByte(h, 1)
		} else {
			writeByte(h, 0)
		}
	case *plan.Literal_Dateval:
		writeByte(h, 13)
		writeUint32(h, uint32(v.Dateval))
	case *plan.Literal_Timeval:
		writeByte(h, 14)
		writeUint64(h, uint64(v.Timeval))
	case *plan.Literal_Datetimeval:
		writeByte(h, 15)
		writeUint64(h, uint64(v.Datetimeval))
	case *plan.Literal_Timestampval:
		writeByte(h, 16)
		writeUint64(h, uint64(v.Timestampval))
	case *plan.Literal_EnumVal:
		writeByte(h, 17)
		writeUint32(h, v.EnumVal)
	case *plan.Literal_Jsonval:
		writeByte(h, 18)
		_, _ = h.Write([]byte(v.Jsonval))
		writeByte(h, 0)
	default:
		// Uncommon literal variants — fall back to marshal.
		writeByte(h, 0xff)
		if b, err := lit.Marshal(); err == nil {
			_, _ = h.Write(b)
		}
	}
}

// exprStructuralEqual is the collision resolver for exprStructuralHash. It
// walks two exprs in lockstep and compares the branches the hash covers
// directly; uncommon variants fall through to proto binary equality.
func exprStructuralEqual(a, b *plan.Expr) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Typ.Id != b.Typ.Id || a.Typ.Width != b.Typ.Width || a.Typ.Scale != b.Typ.Scale {
		return false
	}
	switch av := a.Expr.(type) {
	case *plan.Expr_Lit:
		bv, ok := b.Expr.(*plan.Expr_Lit)
		if !ok {
			return false
		}
		return literalEqual(av.Lit, bv.Lit)
	case *plan.Expr_Col:
		bv, ok := b.Expr.(*plan.Expr_Col)
		if !ok {
			return false
		}
		if av.Col == nil || bv.Col == nil {
			return av.Col == bv.Col
		}
		return av.Col.RelPos == bv.Col.RelPos && av.Col.ColPos == bv.Col.ColPos
	case *plan.Expr_F:
		bv, ok := b.Expr.(*plan.Expr_F)
		if !ok {
			return false
		}
		if av.F == nil || bv.F == nil {
			return av.F == bv.F
		}
		if (av.F.Func == nil) != (bv.F.Func == nil) {
			return false
		}
		if av.F.Func != nil && av.F.Func.ObjName != bv.F.Func.ObjName {
			return false
		}
		if len(av.F.Args) != len(bv.F.Args) {
			return false
		}
		for i := range av.F.Args {
			if !exprStructuralEqual(av.F.Args[i], bv.F.Args[i]) {
				return false
			}
		}
		return true
	case *plan.Expr_List:
		bv, ok := b.Expr.(*plan.Expr_List)
		if !ok {
			return false
		}
		if av.List == nil || bv.List == nil {
			return av.List == bv.List
		}
		if len(av.List.List) != len(bv.List.List) {
			return false
		}
		for i := range av.List.List {
			if !exprStructuralEqual(av.List.List[i], bv.List.List[i]) {
				return false
			}
		}
		return true
	default:
		// Fallback: compare proto bytes.
		ab, aerr := a.Marshal()
		bb, berr := b.Marshal()
		if aerr != nil || berr != nil {
			return false
		}
		if len(ab) != len(bb) {
			return false
		}
		for i := range ab {
			if ab[i] != bb[i] {
				return false
			}
		}
		return true
	}
}

func literalEqual(a, b *plan.Literal) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Isnull != b.Isnull {
		return false
	}
	if a.Isnull {
		return true
	}
	switch av := a.Value.(type) {
	case *plan.Literal_I8Val:
		bv, ok := b.Value.(*plan.Literal_I8Val)
		return ok && av.I8Val == bv.I8Val
	case *plan.Literal_I16Val:
		bv, ok := b.Value.(*plan.Literal_I16Val)
		return ok && av.I16Val == bv.I16Val
	case *plan.Literal_I32Val:
		bv, ok := b.Value.(*plan.Literal_I32Val)
		return ok && av.I32Val == bv.I32Val
	case *plan.Literal_I64Val:
		bv, ok := b.Value.(*plan.Literal_I64Val)
		return ok && av.I64Val == bv.I64Val
	case *plan.Literal_U8Val:
		bv, ok := b.Value.(*plan.Literal_U8Val)
		return ok && av.U8Val == bv.U8Val
	case *plan.Literal_U16Val:
		bv, ok := b.Value.(*plan.Literal_U16Val)
		return ok && av.U16Val == bv.U16Val
	case *plan.Literal_U32Val:
		bv, ok := b.Value.(*plan.Literal_U32Val)
		return ok && av.U32Val == bv.U32Val
	case *plan.Literal_U64Val:
		bv, ok := b.Value.(*plan.Literal_U64Val)
		return ok && av.U64Val == bv.U64Val
	case *plan.Literal_Dval:
		bv, ok := b.Value.(*plan.Literal_Dval)
		return ok && av.Dval == bv.Dval
	case *plan.Literal_Fval:
		bv, ok := b.Value.(*plan.Literal_Fval)
		return ok && av.Fval == bv.Fval
	case *plan.Literal_Sval:
		bv, ok := b.Value.(*plan.Literal_Sval)
		return ok && av.Sval == bv.Sval
	case *plan.Literal_Bval:
		bv, ok := b.Value.(*plan.Literal_Bval)
		return ok && av.Bval == bv.Bval
	case *plan.Literal_Dateval:
		bv, ok := b.Value.(*plan.Literal_Dateval)
		return ok && av.Dateval == bv.Dateval
	case *plan.Literal_Timeval:
		bv, ok := b.Value.(*plan.Literal_Timeval)
		return ok && av.Timeval == bv.Timeval
	case *plan.Literal_Datetimeval:
		bv, ok := b.Value.(*plan.Literal_Datetimeval)
		return ok && av.Datetimeval == bv.Datetimeval
	case *plan.Literal_Timestampval:
		bv, ok := b.Value.(*plan.Literal_Timestampval)
		return ok && av.Timestampval == bv.Timestampval
	case *plan.Literal_EnumVal:
		bv, ok := b.Value.(*plan.Literal_EnumVal)
		return ok && av.EnumVal == bv.EnumVal
	case *plan.Literal_Jsonval:
		bv, ok := b.Value.(*plan.Literal_Jsonval)
		return ok && av.Jsonval == bv.Jsonval
	default:
		// Uncommon literal variant — binary fallback.
		ab, aerr := a.Marshal()
		bb, berr := b.Marshal()
		if aerr != nil || berr != nil {
			return false
		}
		if len(ab) != len(bb) {
			return false
		}
		for i := range ab {
			if ab[i] != bb[i] {
				return false
			}
		}
		return true
	}
}
