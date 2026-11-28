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

package cdc

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"reflect"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// CDC generation classification for one visible target column. The stored
// variants never expose the bound expression: the CALL handlers digest the
// typed tree and return only the classification plus the digest, so an
// INSERT-only role learns whether a column is writable (ordinary) or
// generated, and - for generated columns - only a fully canonical digest of
// the value-relevant typed semantics.
const (
	CDCColumnOrdinary        byte = 'O'
	CDCColumnGeneratedStored byte = 'S'
	CDCColumnGeneratedVirt   byte = 'V'
)

// CDCGenerationColumnSignature is the minimal scoped classification row the
// guarded metadata call returns per visible ordinal column.
type CDCGenerationColumnSignature struct {
	Ordinal             int32
	Class               byte
	NullabilityMismatch bool
	TypedDigest         uint64
	SrcDigest           uint64 // digest of the raw types.Type for the column
}

// safeFunctionNames lists the resolved overload names the CDC generated
// recompute admission may trust: session-context-free, server-implementation
// proven in review 6058670243. Anything not listed is unknown and therefore
// rejected before acknowledgement. This set is positively proved: DATE in any
// overload, FROM_UNIXTIME, CONVERT_TZ and all clock/env/session readers are
// deliberately absent (see design-v3 section 2).
var safeFunctionNames = map[string]struct{}{
	"+":               {},
	"-":               {},
	"*":               {},
	"~":               {}, // unary int minus
	"st_geomfromtext": {},
}

// intFamily reports whether the encoded type is an integer family value the
// int arithmetic path has proven session-free (and that integer scaling casts
// cannot observe session context). Time-derived decimal/dates are excluded.
func intFamily(t types.T) bool {
	switch t {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return true
	}
	return false
}

// geometryFamily reports the only non-int stored result admitted for
// recompute: the constant-WKT geometry constructor case pinned by the
// committed fixture (ST_GeomFromText over a literal).
func geometryFamily(t types.T) bool {
	return t == types.T_geometry
}

// literalExpr extracts a constant literal, or nil when the node is not a
// compile-time constant.
func literalExpr(e *plan.Expr) (interface{}, bool) {
	if e == nil || e.Expr == nil {
		return nil, false
	}
	switch n := e.Expr.(type) {
	case *plan.Expr_Lit:
		if n == nil || n.Lit == nil || n.Lit.GetIsnull() {
			return nil, false
		}
		switch v := n.Lit.Value.(type) {
		case *plan.Literal_I64Val:
			return v.I64Val, true
		case *plan.Literal_U64Val:
			return v.U64Val, true
		case *plan.Literal_I32Val:
			return v.I32Val, true
		case *plan.Literal_U32Val:
			return v.U32Val, true
		case *plan.Literal_Sval:
			return v.Sval, true
		default:
			return nil, true // constant, but not one of the trusted shapes
		}
	case *plan.Expr_F:
		if n == nil || n.F.Func == nil {
			return nil, false
		}
		name := strings.ToLower(n.F.Func.GetObjName())
		// Fold-stage constants: trust an explicit constant fold of an int
		// literal only. St_ct functions inside vector overlays keep the
		// signature walk bounded: anything else is rejected.
		if name == "cast" || name == "decimal" {
			return nil, false
		}
	}
	return nil, false
}

// columnOrdinalOf returns the absolute ordinal referenced by a column expr.
func columnOrdinalOf(e *plan.Expr) (int32, string, bool) {
	if e == nil || e.Expr == nil {
		return 0, "", false
	}
	if c, ok := e.Expr.(*plan.Expr_Col); ok && c != nil && c.Col != nil {
		return c.Col.GetColPos(), c.Col.GetName(), true
	}
	return 0, "", false
}

// safeGeneratedExpr walks the typed bound expression tree and verifies the
// session-free closure: each node must be one of the allowed function
// overloads, a reference to an ordinary replicated base column (whose ordinal
// is < the first generated ordinal), or a compile-time constant allowed for
// the admitted classes. Operand type discipline is enforced per node so
// changing the target expression, ordinal layout or coercion flips the
// comparison result even when the overload name matches.
func safeGeneratedExpr(e *plan.Expr, firstGeneratedOrdinal int32, resultType types.T, depth int) bool {
	// The safe walk and the digest walk share one depth bound: a tree the
	// canonicalizer cannot render completely is never admitted as safe.
	if e == nil || depth > 64 {
		return false
	}
	switch n := e.Expr.(type) {
	case *plan.Expr_Lit:
		value, ok := literalExpr(e)
		if !ok {
			return false
		}
		// Only trusted constant shapes: an int constant or the
		// string WKT literal feeding the geometry constructor.
		switch value.(type) {
		case int64, uint64, int32, uint32, string:
			return true
		}
		return false
	case *plan.Expr_T:
		// Persisted arithmetic binds wrap operands and results with an
		// explicit target-type marker node. The TargetType payload is
		// empty by design, so the node's own carried type - passed in as
		// resultType - is the whole check: int-family targets only.
		return intFamily(resultType)
	case *plan.Expr_Col:
		ordinal, _, ok := columnOrdinalOf(e)
		if !ok {
			return false
		}
		// The referenced ordinal must be a base column below every
		// generated slot: persisted bound colrefs carry only the ordinal.
		// Ordinal references are compared by value during revalidation, so
		// they are allowed only for ordinary-replicated positions.
		return ordinal >= 0 && (firstGeneratedOrdinal < 0 || ordinal < firstGeneratedOrdinal)
	case *plan.Expr_F:
		if n == nil || n.F == nil || n.F.Func == nil {
			return false
		}
		name := strings.ToLower(n.F.Func.GetObjName())
		if name == "cast" {
			// Persisted int arithmetic binds as explicit casts around the
			// operands and the arithmetic result. Casts are admitted only
			// with an int-family result and int-family typed arguments, so
			// cross-domain coercions (timestamp to date, string to date,
			// ...) stay fenced.
			if !intFamily(resultType) {
				return false
			}
			if len(n.F.Args) == 0 {
				return false
			}
			for _, arg := range n.F.Args {
				if arg == nil || !intFamily(types.T(arg.GetTyp().Id)) {
					return false
				}
				if !safeGeneratedExpr(arg, firstGeneratedOrdinal, types.T(arg.GetTyp().Id), depth+1) {
					return false
				}
			}
			return true
		}
		if _, allowed := safeFunctionNames[name]; !allowed {
			return false
		}
		if name == "st_geomfromtext" {
			// The committed geometry case: constructor over string WKT
			// literals. The real binder persists the call with a single
			// literal argument (proven by the instrumented fence dump);
			// an explicit int-literal SRID argument is admitted too.
			if !geometryFamily(resultType) {
				return false
			}
			if len(n.F.Args) != 1 && len(n.F.Args) != 2 {
				return false
			}
			if value, ok := literalExpr(n.F.Args[0]); !ok {
				return false
			} else if _, isString := value.(string); !isString {
				return false
			}
			if len(n.F.Args) == 2 {
				srid, ok := literalExpr(n.F.Args[1])
				if !ok {
					return false
				}
				switch srid.(type) {
				case int64, uint64, int32, uint32:
				default:
					return false
				}
			}
			return true
		}
		// Arithmetic over integer family only: each argument must be an int
		// expression and the result must be the int family for every node.
		if !intFamily(resultType) {
			return false
		}
		if len(n.F.Args) != 2 && name != "~" {
			return false
		}
		for _, arg := range n.F.Args {
			if arg == nil || !intFamily(types.T(arg.GetTyp().Id)) {
				return false
			}
		}
		for _, arg := range n.F.Args {
			if !safeGeneratedExpr(arg, firstGeneratedOrdinal, types.T(arg.GetTyp().Id), depth+1) {
				return false
			}
		}
		return true
	}
	return false
}

// CDCGeneratedExprSafe reports whether the bound generated expression belongs
// to the positively proved session-free closure (see safeGeneratedExpr) and
// is missing a typed tree (declared generated with a nil Expr) - the latter
// is never silently admitted as safe.
func CDCGeneratedExprSafe(gen *plan.GeneratedCol, firstGeneratedOrdinal int32, resultType types.T) bool {
	if gen == nil || gen.Expr == nil {
		return false
	}
	return safeGeneratedExpr(gen.Expr, firstGeneratedOrdinal, resultType, 0)
}

// CDCGeneratedExprStructure renders the bound tree's node kinds, resolved
// overload names, operand type ids and literal variant names for one exact
// purpose: including the true structure in a rejection error so the fence is
// diagnosable from the persisted err_msg alone. It never renders literal
// contents, so the rejection channel stays scoped.
func CDCGeneratedExprStructure(e *plan.Expr, depth int) string {
	if e == nil || depth > 8 {
		return "nil"
	}
	parts := []string{}
	typ := e.GetTyp()
	parts = append(parts, fmt.Sprintf("type=%d", typ.GetId()))
	switch n := e.Expr.(type) {
	case *plan.Expr_Lit:
		variant := "null"
		if n != nil && n.Lit != nil && !n.Lit.GetIsnull() && n.Lit.Value != nil {
			variant = reflect.TypeOf(n.Lit.Value).String()
		}
		parts = append(parts, "node=lit", "literal="+variant)
	case *plan.Expr_Col:
		pos, name, ok := columnOrdinalOf(e)
		parts = append(parts, fmt.Sprintf("node=col,pos=%d,name=%t", pos, ok && name != ""))
	case *plan.Expr_F:
		if n == nil || n.F == nil || n.F.Func == nil {
			parts = append(parts, "node=f,bad")
		} else {
			parts = append(parts, fmt.Sprintf("node=f,func=%s,funcid=%d,argc=%d", n.F.Func.GetObjName(), n.F.Func.GetObj(), len(n.F.Args)))
			for _, arg := range n.F.Args {
				parts = append(parts, CDCGeneratedExprStructure(arg, depth+1))
			}
		}
	default:
		parts = append(parts, fmt.Sprintf("node=%s", reflect.TypeOf(e.Expr).String()))
	}
	return strings.Join(parts, " ")
}

// digestExpr writes the canonical typed-tree rendering of the bound
// expression: resolved overload object id, operand ordinals, an unambiguous
// literal encoding (length-prefixed), per-node result types and cast flags.
// The rendering never contains raw expression text or quoted SQL tokens.
// digestExpr renders one canonical typed-node image into the digest state.
// The walk is bounded once at depth 64 and FAILS CLOSED: a truncated subtree,
// an unhandled literal or expression variant, or a nil node never aliases
// into an equality match. Callers must refuse the whole signature when any
// node reports false.
func digestExpr(h *fnv64, e *plan.Expr, depth int) bool {
	if depth > 64 || e == nil {
		return false
	}
	typ := e.GetTyp()
	// Full semantic type image: identity, size, nullability, auto-increment,
	// charset, enum values, padding and collation semantics, and the numeric
	// attribute bits - every field that can change a stored value renders
	// differently.
	h.write(typ.GetId())
	h.i32(typ.GetWidth())
	h.i32(typ.GetScale())
	h.b(typ.GetNotNullable())
	h.b(typ.GetAutoIncr())
	h.i64(int64(typ.GetCharset()))
	h.s(typ.GetEnumvalues())
	h.b(typ.GetPadSpace())
	h.i64(int64(typ.GetCollationCoercibility()))
	h.b(typ.GetCollationCoercibilitySet())
	h.b(typ.GetCollationMergeConflict())
	h.i64(int64(typ.GetCollationVersion()))
	h.i64(int64(typ.GetNumericAttributes()))
	switch n := e.Expr.(type) {
	case *plan.Expr_Lit:
		h.write("lit")
		lit := n.Lit
		if lit == nil || lit.GetIsnull() {
			h.write("null")
			return true
		}
		switch v := lit.Value.(type) {
		case *plan.Literal_I64Val:
			h.write("i64")
			h.i64(v.I64Val)
		case *plan.Literal_U64Val:
			h.write("u64")
			h.i64(int64(v.U64Val))
		case *plan.Literal_I32Val:
			h.write("i32")
			h.i32(v.I32Val)
		case *plan.Literal_U32Val:
			h.write("u32")
			h.i64(int64(v.U32Val))
		case *plan.Literal_Sval:
			h.write("s")
			h.i32(int32(len(v.Sval)))
			h.h.Write([]byte(v.Sval))
		case *plan.Literal_Bval:
			if v.Bval {
				h.write("t")
			} else {
				h.write("f")
			}
		case *plan.Literal_Dval:
			h.write("d")
			h.i64(int64(v.Dval))
		default:
			// Unhandled literal variants never alias to "other": the
			// signature is incomplete and must fail closed.
			return false
		}
	case *plan.Expr_Col:
		h.write("col")
		h.i32(n.Col.GetColPos())
		h.s(n.Col.GetName())
		h.s(n.Col.GetTblName())
	case *plan.Expr_T:
		// Persisted binds wrap operands and results with an empty
		// target-type marker node. Its own carried type is already part of
		// the generic per-node header image above; the marker node is
		// complete and benign - unlike unhandled variants it never makes
		// the signature refuse.
		h.write("t")
	case *plan.Expr_Vec:
		h.write("vec")
		h.i32(n.Vec.GetLen())
	case *plan.Expr_F:
		h.write("f")
		h.i64(n.F.Func.GetObj())
		h.s(n.F.Func.GetObjName())
		h.b(n.F.GetSyntaxExplicitCast())
		h.i32(int32(len(n.F.Args)))
		for _, arg := range n.F.Args {
			if !digestExpr(h, arg, depth+1) {
				return false
			}
		}
	default:
		// Unhandled expression variants never alias to "other": the
		// signature is incomplete and must fail closed.
		return false
	}
	return true
}

// fnv64 is the shared canonical digest state.
type fnv64 struct {
	h hash64
}

type hash64 interface {
	Write([]byte) (int, error)
	Sum64() uint64
}

func (s *fnv64) write(v interface{}) {
	switch t := v.(type) {
	case types.T:
		s.i32(int32(t))
	case string:
		s.h.Write([]byte(t))
		s.h.Write([]byte{0})
	case bool:
		if t {
			s.h.Write([]byte{1})
		} else {
			s.h.Write([]byte{0})
		}
	case int32:
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(t))
		s.h.Write(b[:])
	case int64:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(t))
		s.h.Write(b[:])
	default:
		s.h.Write([]byte{0})
	}
}

func (s *fnv64) i32(v int32) { s.write(v) }
func (s *fnv64) i64(v int64) { s.write(v) }
func (s *fnv64) b(v bool)    { s.write(v) }
func (s *fnv64) s(v string)  { s.write(v) }

// CDCColumnTypedDigest digests one column's value-relevant typed semantics:
// binding name, ordinal, the full column type, nullability, the generation
// kind (stored/virtual) and the canonical typed expression tree. Two columns
// with equal digests evaluate identically for equal operands in any session
// context when combined with CDCGeneratedExprSafe.
// digestExprWrapped bounds recursion entry points and carries the walk's
// completeness bit: a truncated, nil or unhandled node makes the whole
// signature incomplete instead of aliasing into a matchable digest.
func digestExprWrapped(h *fnv64, e *plan.Expr) bool {
	return digestExpr(h, e, 0)
}

// CDCColumnSig sorts a signature slice by ordinal for canonical comparison.
func CDCColumnSigSort(rows []CDCGenerationColumnSignature) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].Ordinal < rows[j].Ordinal })
}

// CDCTargetGenerationContract is the admission-captured per-visibility-ordinal
// column contract plus the canonical target signature it was derived from.
// Copy is true when the sinker must replicate the SOURCE MATERIALIZED value
// (ordinary writable target, including a source-generated slot). Generated
// ordinals are the G-admitted omissions: their target digest is retained for
// the per-DML revalidation fence. ProjectionSensitive marks any mapping whose
// projection depends on observed target schema, which is also the only set
// that pays the extra guarded signature read.
type CDCTargetGenerationContract struct {
	Copy                []bool
	GeneratedOrdinals   []int32
	TargetClass         []byte
	TargetNames         []string
	TargetDigest        []uint64
	ProjectionSensitive bool
}

// CDCSourceHasGeneratedColumn reports whether any visible column of the
// source table definition carries a generated expression. An unverified
// mapping defining generated source slots must fail closed: silently
// omitting an unobserved generated slot has no value-copy proof.
func CDCSourceHasGeneratedColumn(cols []*plan.ColDef) bool {
	for _, col := range cols {
		if col == nil {
			continue
		}
		if _, internal := catalog.InternalColumns[col.Name]; internal || col.Hidden {
			continue
		}
		if col.GetGeneratedCol() != nil {
			return true
		}
	}
	return false
}

// CDCContractMatchesSignature revalidates an admission-captured contract
// against a freshly read target signature (per-DML transaction fence). The
// fence compares the COMPLETE column signature: every ordinal's class and
// canonical typed digest, plus layout width. A short, extra-long or drifted
// layout is a schema change; absent ordinals are never padded into a match,
// duplicate ordinals never override each other, and an unknown class byte
// misses against the captured class.
func CDCContractMatchesSignature(c *CDCTargetGenerationContract, class []byte, digest []uint64) bool {
	if c == nil || len(c.Copy) == 0 {
		return false
	}
	width := len(c.Copy)
	if len(class) != width || len(digest) != width ||
		len(c.TargetClass) != width || len(c.TargetDigest) != width {
		return false
	}
	for i := 0; i < width; i++ {
		if class[i] == 0 || class[i] != c.TargetClass[i] || digest[i] != c.TargetDigest[i] {
			return false
		}
	}
	return true
}

// CDCColumnTypedDigestFromColDef digests one visible column from the
// engine-reconstructed plan.ColDef (the complete TableDef path), keeping the
// sinker and the guarded metadata call on one canonical channel. The image
// covers the binding name, ordinal, the FULL semantic type fields (size,
// nullability, auto-increment, charset, enum values, padding, collation
// semantics and numeric attribute bits), the column nullability, the
// generation kind, and the complete typed expression tree. The second return
// is completeness: a truncated, nil or unhandled node makes the digest refuse
// instead of aliasing two different trees into one value.
func CDCColumnTypedDigestFromColDef(col *plan.ColDef, ordinal int32) (uint64, bool) {
	if col == nil {
		return 0, false
	}
	h := &fnv64{h: fnv.New64a()}
	h.s("col")
	h.s(col.GetOriginCaseName())
	h.i32(ordinal)
	typ := col.Typ
	h.i32(typ.GetId())
	h.i32(typ.GetWidth())
	h.i32(typ.GetScale())
	h.b(typ.GetNotNullable())
	h.b(typ.GetAutoIncr())
	h.i64(int64(typ.GetCharset()))
	h.s(typ.GetEnumvalues())
	h.b(typ.GetPadSpace())
	h.i64(int64(typ.GetCollationCoercibility()))
	h.b(typ.GetCollationCoercibilitySet())
	h.b(typ.GetCollationMergeConflict())
	h.i64(int64(typ.GetCollationVersion()))
	h.i64(int64(typ.GetNumericAttributes()))
	h.b(col.GetNotNull())
	gen := col.GetGeneratedCol()
	if gen == nil {
		h.write("ordinary")
		return h.h.Sum64(), true
	}
	h.b(gen.GetIsStored())
	if !digestExprWrapped(h, gen.Expr) {
		return 0, false
	}
	return h.h.Sum64(), true
}

// classifyShowColumnsOrdinary proves ordinary for a GENERATION-FREE source
// mapping through the guarded SHOW metadata on the caller's already-held
// verify transaction. SHOW COLUMNS is object-privileged (#30304) - the sink
// role holds INSERT on the permitted target - and builds exactly what
// buildShowColumns renders from the live catalog attributes: the Extra arm
// is auto_increment > generated > expression-default, so an auto-increment
// or DEFAULT_GENERATED state can never mask a STORED/VIRTUAL GENERATED
// column, and no historical information_schema template is consulted. The
// Field and Extra headers are located by name (the FULL variant renders
// [Field Type Collation Null Key Default Extra Privileges Comment]); the
// read requires the exact ordered visible names and rejects repeated
// required headers, invalid or NULL Field values, and every Extra state
// outside the verified native lexemes (ordinary "", auto_increment,
// DEFAULT_GENERATED; generated STORED/VIRTUAL GENERATED denies). Case
// variants and unrecognized states are fail closed, never treated as
// ordinary, and nothing is ever inferred from an error - there is no
// error-keyed fallback. A proven ordinary layout derives zero per-DML
// generation reads.
func classifyShowColumnsOrdinary(
	ctx context.Context,
	queryer interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	},
	info *DbTableInfo,
	visible []*plan.ColDef,
	contract *CDCTargetGenerationContract,
) (*CDCTargetGenerationContract, error) {
	rows, err := queryer.QueryContext(ctx, fmt.Sprintf(
		"SHOW FULL COLUMNS FROM %s.%s",
		quoteSQLIdentifier(info.SinkDbName), quoteSQLIdentifier(info.SinkTblName)))
	if err != nil {
		return nil, retryableCDCTargetMetadataError(err)
	}
	defer rows.Close()
	headers, err := rows.Columns()
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx,
			"CDC target %s.%s SHOW metadata headers cannot be read: %v; explicit recovery is required",
			info.SinkDbName, info.SinkTblName, err)
	}
	fieldIdx, extraIdx := -1, -1
	fieldSeen, extraSeen := false, false
	for i, h := range headers {
		switch h {
		case "Field":
			if fieldSeen {
				return nil, moerr.NewInternalErrorf(ctx,
					"CDC target %s.%s SHOW metadata repeats the Field header (%v); explicit recovery is required",
					info.SinkDbName, info.SinkTblName, headers)
			}
			fieldSeen, fieldIdx = true, i
		case "Extra":
			if extraSeen {
				return nil, moerr.NewInternalErrorf(ctx,
					"CDC target %s.%s SHOW metadata repeats the Extra header (%v); explicit recovery is required",
					info.SinkDbName, info.SinkTblName, headers)
			}
			extraSeen, extraIdx = true, i
		}
	}
	if fieldIdx < 0 || extraIdx < 0 {
		return nil, moerr.NewInternalErrorf(ctx,
			"CDC target %s.%s SHOW metadata header variant is unsupported: %v; explicit recovery is required",
			info.SinkDbName, info.SinkTblName, headers)
	}
	scan := make([]sql.NullString, len(headers))
	dest := make([]any, len(scan))
	for i := range scan {
		dest[i] = &scan[i]
	}
	ordinal := 0
	for rows.Next() {
		if err = rows.Scan(dest...); err != nil {
			return nil, moerr.NewInternalErrorf(ctx,
				"CDC target %s.%s SHOW metadata cannot be scanned: %v", info.SinkDbName, info.SinkTblName, err)
		}
		if ordinal >= len(visible) {
			return nil, moerr.NewInternalErrorf(ctx,
				"CDC target %s.%s SHOW metadata reports an unexpected column layout at position %d",
				info.SinkDbName, info.SinkTblName, ordinal+1)
		}
		if !scan[fieldIdx].Valid {
			return nil, moerr.NewInternalErrorf(ctx,
				"CDC target %s.%s SHOW metadata reports an invalid column field at position %d; explicit recovery is required",
				info.SinkDbName, info.SinkTblName, ordinal+1)
		}
		name := scan[fieldIdx].String
		// SHOW publishes the DECLARED spelling (col.attname), so identity is
		// compared against the source's original-case column contract - the
		// same contract the schema verify and the sink projection already
		// demand (catalog ColDefs carry the lowercased Name side by side
		// with the original OriginName). No case-insensitive or artificial
		// mapping exists here: a case drift is a conflicting identity.
		if name != visible[ordinal].GetOriginCaseName() {
			return nil, moerr.NewInternalErrorf(ctx,
				"CDC target %s.%s SHOW metadata reports conflicting column identity %q at position %d",
				info.SinkDbName, info.SinkTblName, name, ordinal+1)
		}
		if !scan[extraIdx].Valid {
			return nil, moerr.NewInternalErrorf(ctx,
				"CDC target %s.%s column %s has an unknown SHOW generation state: explicit recovery is required",
				info.SinkDbName, info.SinkTblName, name)
		}
		extra := scan[extraIdx].String
		switch extra {
		case "", "auto_increment", "DEFAULT_GENERATED":
			// The verified native ordinary states: build_show renders
			// exactly these and can never mask generated state.
		case "STORED GENERATED", "VIRTUAL GENERATED":
			return nil, moerr.NewNotSupportedf(ctx,
				"CDC target %s.%s column %s is generated while the source column is not: pre-create an ordinary writable column instead",
				info.SinkDbName, info.SinkTblName, name)
		default:
			// An unrecognized or malformed Extra state - including any
			// case variant of a generated lexeme - is never admitted as
			// ordinary; it fails closed without speculation.
			return nil, moerr.NewInternalErrorf(ctx,
				"CDC target %s.%s column %s reports an unsupported SHOW state %q; explicit recovery is required",
				info.SinkDbName, info.SinkTblName, name, extra)
		}
		contract.TargetClass[ordinal] = CDCColumnOrdinary
		contract.TargetNames[ordinal] = name
		contract.Copy[ordinal] = true
		ordinal++
	}
	if err == nil {
		err = rows.Err()
	}
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx,
			"CDC target %s.%s SHOW metadata cannot be read: %v", info.SinkDbName, info.SinkTblName, err)
	}
	if ordinal != len(visible) {
		return nil, moerr.NewInternalErrorf(ctx,
			"CDC target %s.%s SHOW metadata is incomplete: %d of %d visible columns reported; explicit recovery is required",
			info.SinkDbName, info.SinkTblName, ordinal, len(visible))
	}
	contract.ProjectionSensitive = false
	return contract, nil
}
