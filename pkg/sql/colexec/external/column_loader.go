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

package external

import (
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// columnLoader turns the fields of one column of an external scan into
// values. Everything that depends on the column's type and the scan's
// settings is decided once, when the loader is built; loading a field does no
// type dispatch. Text is parsed into the column type by the shared parser
// (vector.TextAppender, types.ParseFunc); what stays here is what LOAD DATA
// and external tables add around it: trimming and NULL rules, MySQL's
// non-strict adjustments, the quoting rules for integer and YEAR text, raw
// BIT bytes, ENUM and SET members, and geometry.
type columnLoader struct {
	key     columnLoaderKey
	colIdx  int32
	colName string
	colId   uint64
	// id is the column's type, which decides the field rules; typ is the
	// vector's, which the value is parsed into (a staged column's vector is
	// VARCHAR)
	id       types.T
	typ      types.Type
	typeName string // the SQL type in error messages

	trim, isSet, enumOrSet, isBit, esqlTemporal  bool
	emptyNumericAsZero, numericPrefix, stageText bool
	// staged non-strict temporal text that does not parse is replaced by
	// zeroText, the column type's zero value
	stagedTemporalCheck func(string) (any, error)
	zeroText            string
	truncateWidth       int32 // non-strict CHAR/VARCHAR truncation, 0 if none
	bitWidth            int32
	strictWidth         int32 // strict CHAR/VARCHAR "Data too long", 0 if none

	appendValue func(l *columnLoader, vec *vector.Vector, field csvparser.Field, param *ExternalParam, proc *process.Process, mp *mpool.MPool) error
	// the shared parse-and-append of the column type, for quoted and
	// unquoted text (which differ for YEAR: see types.TextParseOptions)
	parsed, parsedUnquoted vector.TextAppendFunc
	// roundInteger: integer text that is not a plain integer is rounded half
	// away from zero (unquoted external CSV, JSONLINE files); otherwise a
	// quoted value must be a plain integer, and an unquoted one may be a float
	roundInteger, unsigned bool
	// wide: a 64-bit integer column, whose float-form text the shared rule
	// cannot convert exactly past 2^53 (see truncateWideIntegerText)
	wide bool
	// keepParseErrors: a vector's parse errors say what is wrong with it
	// (malformed, wrong dimension), so they are returned as they are
	keepParseErrors bool
}

// columnLoaderKey is what a columnLoader is built from.
type columnLoaderKey struct {
	colName              string
	colIndex, fieldIndex int32
	typ                  types.Type
	externType           int32
	format               string
	local, strictSQL     bool
	parallelLoad         bool
	emptyNumericAsZero   bool
	esqlTemporalUTC      bool
	timeZone             *time.Location
}

func newColumnLoaderKey(attr plan.ExternAttr, typ types.Type, param *ExternalParam, proc *process.Process) columnLoaderKey {
	key := columnLoaderKey{
		colName:            attr.ColName,
		colIndex:           attr.ColIndex,
		fieldIndex:         attr.ColFieldIndex,
		typ:                typ,
		parallelLoad:       param.ParallelLoad,
		emptyNumericAsZero: param.LoadEmptyNumericAsZero,
		esqlTemporalUTC:    param.ESQLTemporalUTC,
		strictSQL:          param.StrictSqlMode,
		timeZone:           sessionTimeZone(proc),
	}
	if param.Extern != nil {
		key.externType = param.Extern.ExternType
		key.format = param.Extern.Format
		key.local = param.Extern.Local
	}
	return key
}

func sessionTimeZone(proc *process.Process) *time.Location {
	if proc != nil {
		if tz := proc.GetSessionInfo().TimeZone; tz != nil {
			return tz
		}
	}
	return time.Local
}

// columnLoaders returns the loaders of param.Attrs, in order, for the
// vectors of bat. They are resolved once per scan: Prepare clears them, and
// another batch or attribute list rebuilds them.
func (param *ExternalParam) columnLoaders(bat *batch.Batch, proc *process.Process) []*columnLoader {
	if param.colLoadersBat == bat && len(param.colLoaders) == len(param.Attrs) &&
		(len(param.Attrs) == 0 || param.colLoadersAttrs == &param.Attrs[0]) {
		return param.colLoaders
	}
	loaders := make([]*columnLoader, len(param.Attrs))
	for i, attr := range param.Attrs {
		loaders[i] = newColumnLoader(newColumnLoaderKey(attr, *bat.Vecs[attr.ColIndex].GetType(), param, proc), param)
	}
	param.colLoaders, param.colLoadersBat = loaders, bat
	if len(param.Attrs) > 0 {
		param.colLoadersAttrs = &param.Attrs[0]
	}
	return loaders
}

func newColumnLoader(key columnLoaderKey, param *ExternalParam) *columnLoader {
	col := param.Cols[key.colIndex]
	typ := key.typ
	id := types.T(col.Typ.Id)
	l := &columnLoader{
		id:       id,
		key:      key,
		colIdx:   key.colIndex,
		colName:  key.colName,
		colId:    col.ColId,
		typ:      typ,
		typeName: sqlTypeName(typ),
	}
	// SET is stored as uint64 but is a logical string type: it must not get
	// the numeric empty-to-zero fill or numeric-prefix rewriting ("0" may be a
	// member name).
	l.isSet = id == types.T_uint64 && len(col.Typ.Enumvalues) > 0
	l.enumOrSet = id == types.T_enum || l.isSet
	l.isBit = id == types.T_bit
	l.bitWidth = col.Typ.Width
	// Text types keep their whitespace. T_bit carries raw bytes (parsed
	// byte-by-byte, not as text), so whitespace bytes are data too (a
	// whitespace-only bit value would otherwise even become NULL).
	l.trim = !l.enumOrSet && id != types.T_char && id != types.T_varchar && id != types.T_json &&
		id != types.T_binary && id != types.T_varbinary && id != types.T_blob && id != types.T_text &&
		id != types.T_datalink && !l.isBit
	// ES|QL CSV renders dates as ISO 8601 UTC ("2026-01-15T10:20:30.123Z");
	// MO's temporal parsers reject the trailing 'Z' and interpret zone-less
	// text in the session time zone, so both ESQL paths (foreign table and
	// schema-mode esql_tvf) rewrite the value as session-zone wall clock,
	// preserving the UTC instant.
	l.esqlTemporal = param.ESQLTemporalUTC && id.IsDateRelate()
	l.emptyNumericAsZero = !l.isSet && shouldLoadEmptyNumericAsZero(param, id)

	nonStrict := shouldApplyLoadDataNonStrictAdjustments(param)
	l.stageText = (param.ParallelLoad && !isDirectParallelLoadType(id)) || loadTimeTextInput(param, id) ||
		loadAttributedDecimalTextInput(param, &col.Typ)
	isString := id == types.T_char || id == types.T_varchar
	opts := types.TextParseOptions{Loc: key.timeZone, AllowTruncation: typ.Oid.IsDecimal()}
	if nonStrict {
		// Attributed DECIMAL targets stage raw trimmed field bytes unmodified
		// (raw-field preservation boundary): the source sign and every
		// truncation/invalid-input diagnostic belong to the shared admission
		// boundary. Unattributed types keep today's rewrite.
		l.numericPrefix = isLoadNumericAdjustedValueType(id) && !l.isSet && !isAttributedDecimalStagingTarget(&col.Typ)
		// MySQL non-strict LOAD: invalid temporal text is the zero value. A
		// staged column carries text, so the text is replaced.
		opts.ZeroInvalidTemporal = true
		if zero, ok := types.ZeroTemporalText(id); ok && l.stageText {
			l.stagedTemporalCheck, _ = types.ParseAnyFunc(types.New(id, col.Typ.Width, col.Typ.Scale), types.TextParseOptions{Loc: key.timeZone})
			l.zeroText = zero
		}
		if isString {
			l.truncateWidth = col.Typ.Width
		}
	}
	// In strict SQL mode (non-LOCAL load), an over-width CHAR/VARCHAR value is
	// rejected instead of silently truncated, matching strict assignment casts
	// (cast_strict) and MySQL's "Data too long" behavior. LOCAL / non-strict
	// loads keep the lenient (truncate) behavior.
	if checkLineStrict(param) && isString && col.Typ.Width > 0 {
		l.strictWidth = col.Typ.Width
	}

	// roundInteger classifies when integer assignment may use the exact
	// decimal half-away rounding helper: unquoted external CSV text, and
	// JSONLINE file records — external SELECT and directly decoded LOAD alike —
	// whose scalar text never carries CSV quotes. Parallel staged LOAD keeps
	// its varchar staging and cast projection, CSV LOAD keeps the truncation
	// policy, and Kafka carries its own extern type so it stays excluded.
	l.roundInteger = (key.externType == int32(plan.ExternType_EXTERNAL_TB) && key.format == tree.CSV) ||
		(key.format == tree.JSONLINE && (key.externType == int32(plan.ExternType_EXTERNAL_TB) ||
			key.externType == int32(plan.ExternType_LOAD)))
	l.unsigned = id.IsUnsignedInt()
	l.wide = id == types.T_int64 || id == types.T_uint64
	l.keepParseErrors = id.IsArray()

	l.parsed, _ = vector.TextAppender(typ, opts)
	opts.NumericYear = true
	l.parsedUnquoted, _ = vector.TextAppender(typ, opts)
	switch {
	case l.isSet:
		members := col.Typ.Enumvalues
		l.appendValue = func(_ *columnLoader, vec *vector.Vector, field csvparser.Field, _ *ExternalParam, _ *process.Process, mp *mpool.MPool) error {
			d, err := types.ParseSet(members, field.Val)
			if err != nil {
				return err
			}
			return vector.AppendFixed(vec, d, false, mp)
		}
	case id == types.T_enum:
		members := col.Typ.Enumvalues
		l.appendValue = func(_ *columnLoader, vec *vector.Vector, field csvparser.Field, _ *ExternalParam, _ *process.Process, mp *mpool.MPool) error {
			v, err := types.ParseEnum(members, field.Val)
			if err != nil {
				return err
			}
			return vector.AppendFixed(vec, v, false, mp)
		}
	case l.isBit:
		l.appendValue = appendBitField
	case id.IsInteger():
		l.appendValue = appendIntegerField
	case id == types.T_json:
		if key.format == tree.CSV {
			l.appendValue = appendCSVJSONField
		} else {
			l.appendValue = appendRawField
		}
	case id == types.T_char || id == types.T_varchar || id == types.T_binary || id == types.T_varbinary ||
		id == types.T_blob || id == types.T_text || id == types.T_datalink:
		l.appendValue = func(_ *columnLoader, vec *vector.Vector, field csvparser.Field, _ *ExternalParam, _ *process.Process, mp *mpool.MPool) error {
			return appendFieldText(vec, field.Val, mp)
		}
	case id == types.T_geometry || id == types.T_geometry32:
		// The field is (E)WKT text such as "POINT (-87.6 41.8)". Normalize it
		// to the stored bare-WKB form (float32 for GEOMETRY32) and enforce the
		// column's declared subtype, exactly like cast_geometry_to_subtype does
		// for an INSERT, so an external read and an INSERT store identical
		// bytes. The column subtype is carried in Typ.Scale as a geo.Subtype.
		subtype := ""
		if s := geo.Subtype(col.Typ.Scale); s != geo.GENERIC {
			subtype = strings.ToUpper(s.String())
		}
		is32 := id == types.T_geometry32
		l.appendValue = func(l *columnLoader, vec *vector.Vector, field csvparser.Field, param *ExternalParam, proc *process.Process, mp *mpool.MPool) error {
			wkb, err := function.NormalizeGeometryForStorage(proc, []byte(field.Val), subtype, is32)
			if err != nil {
				return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not a valid geometry for column %d: %v", field.Val, l.colIdx, err)
			}
			return vector.AppendBytes(vec, wkb, false, mp)
		}
	default:
		l.appendValue = appendParsedField
	}
	return l
}

// sqlTypeName is the SQL name of typ for messages, as SHOW COLUMNS writes it.
func sqlTypeName(typ types.Type) string {
	if name, ok := types.FormatSQLColumnType(typ); ok {
		return name
	}
	return typ.DescString()
}

// getColData appends the field of attr in line to its vector, with a loader
// resolved for this call. Row loops use the scan's loaders (columnLoaders).
func getColData(bat *batch.Batch, line []csvparser.Field, rowIdx int, param *ExternalParam, mp *mpool.MPool, attr plan.ExternAttr, proc *process.Process) error {
	vec := bat.Vecs[attr.ColIndex]
	l := newColumnLoader(newColumnLoaderKey(attr, *vec.GetType(), param, proc), param)
	return l.load(vec, line, rowIdx, param, proc, mp)
}

func (l *columnLoader) load(vec *vector.Vector, line []csvparser.Field, rowIdx int, param *ExternalParam, proc *process.Process, mp *mpool.MPool) error {
	field := getFieldFromLine(line, l.colName, l.colId, param, l.key.fieldIndex)
	emptyIsNull := false
	if l.enumOrSet {
		field.Val, emptyIsNull = normalizeEnumSetCSVValue(l.id, param.Cols[l.colIdx].Typ.Enumvalues, field.Val)
	} else if l.trim {
		field.Val = strings.TrimSpace(field.Val)
	}
	if l.esqlTemporal {
		field.Val = normalizeISO8601Zulu(field.Val, l.key.timeZone)
	}
	mappedNull := getNullFlag(param.Extern.NullMap, l.colName, field.Val)
	isNullOrEmpty := field.IsNull || mappedNull || emptyIsNull
	if len(field.Val) == 0 && !field.IsNull && !mappedNull && l.emptyNumericAsZero {
		field.Val = "0"
		isNullOrEmpty = false
	} else if l.trim && len(field.Val) == 0 && !isNullOrEmpty {
		isNullOrEmpty = true
	}
	if isNullOrEmpty {
		return vector.AppendBytes(vec, nil, true, mp)
	}

	// A JSON NUMBER field for a BIT column is the exact uint64 the JSONLINE
	// writer emitted, not a raw-byte CSV payload. Convert it after the raw
	// NULLIF decision above and before both consumers: the staged varchar
	// handoff below and the direct type construction below read the field as
	// big-endian bytes, so canonical value bytes reach either consumer as the
	// exact BIT value. Tokens that are not canonical nonnegative uint64
	// integers, or that exceed the BIT width, keep the original text and with
	// it today's conversion values and errors.
	if l.isBit && field.IsJSONNumber {
		if encoded, ok := jsonNumberBitLexeme(field.Val, l.bitWidth); ok {
			field.Val = encoded
		}
	}

	if l.numericPrefix && !field.HasStringQuote {
		field.Val = loadDataNonStrictNumericPrefix(field.Val)
	}
	if l.stagedTemporalCheck != nil {
		if _, err := l.stagedTemporalCheck(field.Val); err != nil {
			field.Val = l.zeroText
		}
	}
	if l.truncateWidth > 0 {
		field.Val = truncateLoadDataStringValue(field.Val, l.truncateWidth)
	}
	// Uses rune count, like strToStr.
	if l.strictWidth > 0 && utf8.RuneCountInString(field.Val) > int(l.strictWidth) {
		return moerr.NewInternalErrorf(param.Ctx, "Data too long for column '%s' at row %d", l.colName, rowIdx+1)
	}
	if l.stageText {
		return appendFieldText(vec, field.Val, mp)
	}
	return l.appendValue(l, vec, field, param, proc, mp)
}

// appendParsed appends text parsed into the column type. A parse failure is
// reported as "the input value '%v' is not <type> type for column %d"; a
// low-precision float out of range and a malformed vector or one of the wrong
// dimension keep their own error.
func (l *columnLoader) appendParsed(vec *vector.Vector, text, input string, quoted bool, param *ExternalParam, mp *mpool.MPool) error {
	parse := l.parsedUnquoted
	if quoted {
		parse = l.parsed
	}
	if parse == nil {
		return moerr.NewInternalErrorf(param.Ctx, "the value type %s is not support now", l.id.String())
	}
	if err := parse(vec, text, mp); err != nil {
		if l.keepParseErrors || moerr.IsMoErrCode(err, moerr.ErrOutOfRange) {
			return err
		}
		return l.valueError(param, input)
	}
	return nil
}

func (l *columnLoader) valueError(param *ExternalParam, input string) error {
	return moerr.NewInternalErrorf(param.Ctx, "the input value '%v' is not %s type for column %d", input, l.typeName, l.colIdx)
}

func appendParsedField(l *columnLoader, vec *vector.Vector, field csvparser.Field, param *ExternalParam, _ *process.Process, mp *mpool.MPool) error {
	return l.appendParsed(vec, field.Val, field.Val, field.HasStringQuote, param, mp)
}

func appendRawField(_ *columnLoader, vec *vector.Vector, field csvparser.Field, _ *ExternalParam, _ *process.Process, mp *mpool.MPool) error {
	return vector.AppendBytes(vec, []byte(field.Val), false, mp)
}

// appendCSVJSONField parses a CSV JSON field, which may carry the quotes of
// its CSV cell.
func appendCSVJSONField(l *columnLoader, vec *vector.Vector, field csvparser.Field, param *ExternalParam, _ *process.Process, mp *mpool.MPool) error {
	text := strings.Trim(field.Val, "\"")
	return l.appendParsed(vec, text, text, field.HasStringQuote, param, mp)
}

// appendIntegerField applies the quoting rules of integer text: a quoted
// value must be a plain integer; unquoted text that is not one is rounded
// half away from zero where roundInteger says so, and otherwise parsed by
// the shared rule (a float is truncated).
func appendIntegerField(l *columnLoader, vec *vector.Vector, field csvparser.Field, param *ExternalParam, _ *process.Process, mp *mpool.MPool) error {
	text := field.Val
	if !field.HasStringQuote && !l.roundInteger {
		// the shared rule, the usual LOAD DATA case; a wide column first
		// truncates float-form text the rule would refuse for precision
		if l.wide && !isIntegerText(text) {
			if exact, ok := truncateWideIntegerText(text, l.unsigned); ok {
				text = exact
			}
		}
		return l.appendParsed(vec, text, field.Val, false, param, mp)
	}
	if !isIntegerText(text) {
		if field.HasStringQuote {
			return l.valueError(param, field.Val)
		}
		if l.roundInteger {
			rounded, err := roundIntegerText(text, l.unsigned)
			if err != nil {
				return l.valueError(param, field.Val)
			}
			text = rounded
		}
	}
	return l.appendParsed(vec, text, field.Val, field.HasStringQuote, param, mp)
}

// truncateWideIntegerText converts decimal or exponent text whose magnitude
// is at least 2^53 to the integer text of its value truncated toward zero,
// for an int64 (or, when unsigned, uint64) column. The shared rule converts
// such text through float64, which cannot hold every integer past 2^53, so
// it refuses it; this conversion is exact. As in the shared rule, the bounds
// are checked before truncation. ok is false when text is below 2^53 (left
// to the shared rule), not a number, or out of range; the caller then hands
// the text to the shared rule, which reports it.
func truncateWideIntegerText(text string, unsigned bool) (string, bool) {
	f, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || math.Abs(f) < 0x1p53 {
		return "", false
	}
	// |f| >= 2^53 bounds the value, so the exact value costs no more than the
	// length of text to compute, however large its exponent.
	r, ok := new(big.Rat).SetString(text)
	if !ok {
		return "", false
	}
	lo, hi := minInt64Rat, maxInt64Rat
	if unsigned {
		lo, hi = zeroRat, maxUint64Rat
	}
	if r.Cmp(lo) < 0 || r.Cmp(hi) > 0 {
		return "", false
	}
	return new(big.Int).Quo(r.Num(), r.Denom()).String(), true
}

var (
	zeroRat      = new(big.Rat)
	minInt64Rat  = new(big.Rat).SetInt64(math.MinInt64)
	maxInt64Rat  = new(big.Rat).SetInt64(math.MaxInt64)
	maxUint64Rat = new(big.Rat).SetUint64(math.MaxUint64)
)

// isIntegerText reports whether s is an optionally signed decimal integer.
func isIntegerText(s string) bool {
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		s = s[1:]
	}
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// roundIntegerText rounds decimal text half away from zero to an integer.
// For an unsigned target a negative value that rounds to zero is accepted
// only when it is a zero ("-0", "-0.0"), not a negative fraction.
func roundIntegerText(text string, unsigned bool) (string, error) {
	integer, err := function.ParseDecimalInteger(text)
	if err != nil {
		return "", err
	}
	if unsigned && integer == "-0" {
		mantissa := text
		if exponent := strings.IndexAny(mantissa, "eE"); exponent >= 0 {
			mantissa = mantissa[:exponent]
		}
		if strings.Trim(mantissa, "-0.") != "" {
			// a negative fraction: out of range for an unsigned column
			return "", strconv.ErrRange
		}
		integer = "0"
	}
	return integer, nil
}

// appendBitField reads a BIT field as its raw bytes, big-endian, as MySQL's
// LOAD DATA does.
func appendBitField(l *columnLoader, vec *vector.Vector, field csvparser.Field, param *ExternalParam, _ *process.Process, mp *mpool.MPool) error {
	if len(field.Val) > 8 {
		return moerr.NewInternalErrorf(param.Ctx, "data too long, len(val) = %v", len(field.Val))
	}
	val, ok := bitFieldValue(field.Val, l.bitWidth)
	if !ok {
		return moerr.NewInternalErrorf(param.Ctx, "data too long, type width = %d, val = %b", l.bitWidth, val)
	}
	return vector.AppendFixed(vec, val, false, mp)
}

// bitFieldValue is the value of raw BIT bytes, big-endian; ok is false when
// it has more than 8 bytes or does not fit in width bits.
func bitFieldValue(raw string, width int32) (val uint64, ok bool) {
	if len(raw) > 8 {
		return 0, false
	}
	for i := 0; i < len(raw); i++ {
		val = (val << 8) | uint64(raw[i])
	}
	return val, val <= uint64(1<<width-1)
}
