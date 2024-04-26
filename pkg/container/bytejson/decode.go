// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bytejson

import (
	"bytes"
	"fmt"
	"io"
	"math/bits"
	"regexp"
	"strconv"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
)

// call specifies which Decoder method was invoked.
type call uint8

const (
	readCall call = iota
	peekCall
)

const unexpectedFmt = "unexpected token %s"

// ErrUnexpectedEOF means that EOF was encountered in the middle of the input.
var ErrUnexpectedEOF = io.ErrUnexpectedEOF

// Decoder is a token-based JSON decoder.
type Decoder struct {
	// lastCall is last method called, either readCall or peekCall.
	// Initial value is readCall.
	lastCall call

	// lastToken contains the last read token.
	lastToken Token

	// lastErr contains the last read error.
	lastErr error

	// openStack is a stack containing ObjectOpen and ArrayOpen values. The
	// top of stack represents the object or the array the current value is
	// directly located in.
	openStack []Kind

	// orig is used in reporting line and column.
	orig []byte
	// in contains the unconsumed input.
	in []byte
}

// NewDecoder returns a Decoder to read the given []byte.
func NewDecoder(b []byte) *Decoder {
	return &Decoder{orig: b, in: b}
}

// Peek looks ahead and returns the next token kind without advancing a read.
func (d *Decoder) Peek() (Token, error) {
	defer func() { d.lastCall = peekCall }()
	if d.lastCall == readCall {
		d.lastToken, d.lastErr = d.Read()
	}
	return d.lastToken, d.lastErr
}

// Read returns the next JSON token.
// It will return an error if there is no valid token.
func (d *Decoder) Read() (Token, error) {
	const scalar = KdNull | KdBool | KdNumber | KdString

	defer func() { d.lastCall = readCall }()
	if d.lastCall == peekCall {
		return d.lastToken, d.lastErr
	}

	tok, err := d.parseNext()
	if err != nil {
		return Token{}, err
	}

	switch tok.kind {
	case KdEOF:
		if len(d.openStack) != 0 ||
			d.lastToken.kind&scalar|KdObjectClose|KdArrayClose == 0 {
			return Token{}, ErrUnexpectedEOF
		}

	case KdNull:
		if !d.isValueNext() {
			return Token{}, d.newSyntaxError(tok.pos, unexpectedFmt, tok.RawString())
		}

	case KdBool, KdNumber:
		if !d.isValueNext() {
			return Token{}, d.newSyntaxError(tok.pos, unexpectedFmt, tok.RawString())
		}

	case KdString:
		if d.isValueNext() {
			break
		}
		// This string token should only be for a field name.
		if d.lastToken.kind&(KdObjectOpen|comma) == 0 {
			return Token{}, d.newSyntaxError(tok.pos, unexpectedFmt, tok.RawString())
		}
		if len(d.in) == 0 {
			return Token{}, ErrUnexpectedEOF
		}
		if c := d.in[0]; c != ':' {
			return Token{}, d.newSyntaxError(d.currPos(), `unexpected character %s, missing ":" after field name`, string(c))
		}
		tok.kind = KdName
		d.consume(1)

	case KdObjectOpen, KdArrayOpen:
		if !d.isValueNext() {
			return Token{}, d.newSyntaxError(tok.pos, unexpectedFmt, tok.RawString())
		}
		d.openStack = append(d.openStack, tok.kind)

	case KdObjectClose:
		if len(d.openStack) == 0 ||
			d.lastToken.kind&(KdName|comma) != 0 ||
			d.openStack[len(d.openStack)-1] != KdObjectOpen {
			return Token{}, d.newSyntaxError(tok.pos, unexpectedFmt, tok.RawString())
		}
		d.openStack = d.openStack[:len(d.openStack)-1]

	case KdArrayClose:
		if len(d.openStack) == 0 ||
			d.lastToken.kind == comma ||
			d.openStack[len(d.openStack)-1] != KdArrayOpen {
			return Token{}, d.newSyntaxError(tok.pos, unexpectedFmt, tok.RawString())
		}
		d.openStack = d.openStack[:len(d.openStack)-1]

	case comma:
		if len(d.openStack) == 0 ||
			d.lastToken.kind&(scalar|KdObjectClose|KdArrayClose) == 0 {
			return Token{}, d.newSyntaxError(tok.pos, unexpectedFmt, tok.RawString())
		}
	}

	// Update d.lastToken only after validating token to be in the right sequence.
	d.lastToken = tok

	if d.lastToken.kind == comma {
		return d.Read()
	}
	return tok, nil
}

// Any sequence that looks like a non-delimiter (for error reporting).
var errRegexp = regexp.MustCompile(`^([-+._a-zA-Z0-9]{1,32}|.)`)

// parseNext parses for the next JSON token. It returns a Token object for
// different types, except for Name. It does not handle whether the next token
// is in a valid sequence or not.
func (d *Decoder) parseNext() (Token, error) {
	// Trim leading spaces.
	d.consume(0)

	in := d.in
	if len(in) == 0 {
		return d.consumeToken(KdEOF, 0), nil
	}

	switch in[0] {
	case 'n':
		if n := matchWithDelim("null", in); n != 0 {
			return d.consumeToken(KdNull, n), nil
		}

	case 't':
		if n := matchWithDelim("true", in); n != 0 {
			return d.consumeBoolToken(true, n), nil
		}

	case 'f':
		if n := matchWithDelim("false", in); n != 0 {
			return d.consumeBoolToken(false, n), nil
		}

	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		if n, ok := parseNumber(in); ok {
			return d.consumeToken(KdNumber, n), nil
		}

	case '"':
		s, n, err := d.parseString(in)
		if err != nil {
			return Token{}, err
		}
		return d.consumeStringToken(s, n), nil

	case '{':
		return d.consumeToken(KdObjectOpen, 1), nil

	case '}':
		return d.consumeToken(KdObjectClose, 1), nil

	case '[':
		return d.consumeToken(KdArrayOpen, 1), nil

	case ']':
		return d.consumeToken(KdArrayClose, 1), nil

	case ',':
		return d.consumeToken(comma, 1), nil
	}
	return Token{}, d.newSyntaxError(d.currPos(), "invalid value %s", errRegexp.Find(in))
}

// newSyntaxError returns an error with line and column information useful for
// syntax errors.
func (d *Decoder) newSyntaxError(pos int, f string, x ...interface{}) error {
	e := moerr.NewSyntaxErrorNoCtx(f, x...)
	line, column := d.Position(pos)
	return moerr.NewSyntaxErrorNoCtx("syntax error (line %d:%d): %v", line, column, e)
}

// Position returns line and column number of given index of the original input.
// It will panic if index is out of range.
func (d *Decoder) Position(idx int) (line int, column int) {
	b := d.orig[:idx]
	line = bytes.Count(b, []byte("\n")) + 1
	if i := bytes.LastIndexByte(b, '\n'); i >= 0 {
		b = b[i+1:]
	}
	column = utf8.RuneCount(b) + 1 // ignore multi-rune characters
	return line, column
}

// currPos returns the current index position of d.in from d.orig.
func (d *Decoder) currPos() int {
	return len(d.orig) - len(d.in)
}

// matchWithDelim matches s with the input b and verifies that the match
// terminates with a delimiter of some form (e.g., r"[^-+_.a-zA-Z0-9]").
// As a special case, EOF is considered a delimiter. It returns the length of s
// if there is a match, else 0.
func matchWithDelim(s string, b []byte) int {
	if !bytes.HasPrefix(b, []byte(s)) {
		return 0
	}

	n := len(s)
	if n < len(b) && isNotDelim(b[n]) {
		return 0
	}
	return n
}

// isNotDelim returns true if given byte is a not delimiter character.
func isNotDelim(c byte) bool {
	return (c == '-' || c == '+' || c == '.' || c == '_' ||
		('a' <= c && c <= 'z') ||
		('A' <= c && c <= 'Z') ||
		('0' <= c && c <= '9'))
}

// consume consumes n bytes of input and any subsequent whitespace.
func (d *Decoder) consume(n int) {
	d.in = d.in[n:]
	for len(d.in) > 0 {
		switch d.in[0] {
		case ' ', '\n', '\r', '\t':
			d.in = d.in[1:]
		default:
			return
		}
	}
}

// isValueNext returns true if next type should be a JSON value: Null,
// Number, String or Bool.
func (d *Decoder) isValueNext() bool {
	if len(d.openStack) == 0 {
		return d.lastToken.kind == 0
	}

	start := d.openStack[len(d.openStack)-1]
	switch start {
	case KdObjectOpen:
		return d.lastToken.kind&KdName != 0
	case KdArrayOpen:
		return d.lastToken.kind&(KdArrayOpen|comma) != 0
	}
	panic(fmt.Sprintf(
		"unreachable logic in Decoder.isValueNext, lastToken.kind: %v, openStack: %v",
		d.lastToken.kind, start))
}

// consumeToken constructs a Token for given Kind with raw value derived from
// current d.in and given size, and consumes the given size-length of it.
func (d *Decoder) consumeToken(kind Kind, size int) Token {
	tok := Token{
		kind: kind,
		raw:  d.in[:size],
		pos:  len(d.orig) - len(d.in),
	}
	d.consume(size)
	return tok
}

// consumeBoolToken constructs a Token for a Bool kind with raw value derived from
// current d.in and given size.
func (d *Decoder) consumeBoolToken(b bool, size int) Token {
	tok := Token{
		kind: KdBool,
		raw:  d.in[:size],
		pos:  len(d.orig) - len(d.in),
		boo:  b,
	}
	d.consume(size)
	return tok
}

// consumeStringToken constructs a Token for a String kind with raw value derived
// from current d.in and given size.
func (d *Decoder) consumeStringToken(s string, size int) Token {
	tok := Token{
		kind: KdString,
		raw:  d.in[:size],
		pos:  len(d.orig) - len(d.in),
		str:  s,
	}
	d.consume(size)
	return tok
}

// Clone returns a copy of the Decoder for use in reading ahead the next JSON
// object, array or other values without affecting current Decoder.
func (d *Decoder) Clone() *Decoder {
	ret := *d
	ret.openStack = append([]Kind(nil), ret.openStack...)
	return &ret
}

// parseNumber reads the given []byte for a valid JSON number. If it is valid,
// it returns the number of bytes.  Parsing logic follows the definition in
// https://tools.ietf.org/html/rfc7159#section-6, and is based off
// encoding/json.isValidNumber function.
func parseNumber(input []byte) (int, bool) {
	var n int

	s := input
	if len(s) == 0 {
		return 0, false
	}

	// Optional -
	if s[0] == '-' {
		s = s[1:]
		n++
		if len(s) == 0 {
			return 0, false
		}
	}

	// Digits
	switch {
	case s[0] == '0':
		s = s[1:]
		n++

	case '1' <= s[0] && s[0] <= '9':
		s = s[1:]
		n++
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
			n++
		}

	default:
		return 0, false
	}

	// . followed by 1 or more digits.
	if len(s) >= 2 && s[0] == '.' && '0' <= s[1] && s[1] <= '9' {
		s = s[2:]
		n += 2
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
			n++
		}
	}

	// e or E followed by an optional - or + and
	// 1 or more digits.
	if len(s) >= 2 && (s[0] == 'e' || s[0] == 'E') {
		s = s[1:]
		n++
		if s[0] == '+' || s[0] == '-' {
			s = s[1:]
			n++
			if len(s) == 0 {
				return 0, false
			}
		}
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
			n++
		}
	}

	// Check that next byte is a delimiter or it is at the end.
	if n < len(input) && isNotDelim(input[n]) {
		return 0, false
	}

	return n, true
}

// numberParts is the result of parsing out a valid JSON number. It contains
// the parts of a number. The parts are used for integer conversion.
type numberParts struct {
	neg  bool
	intp []byte
	frac []byte
	exp  []byte
}

// parseNumber constructs numberParts from given []byte. The logic here is
// similar to consumeNumber above with the difference of having to construct
// numberParts. The slice fields in numberParts are subslices of the input.
func parseNumberParts(input []byte) (numberParts, bool) {
	var neg bool
	var intp []byte
	var frac []byte
	var exp []byte

	s := input
	if len(s) == 0 {
		return numberParts{}, false
	}

	// Optional -
	if s[0] == '-' {
		neg = true
		s = s[1:]
		if len(s) == 0 {
			return numberParts{}, false
		}
	}

	// Digits
	switch {
	case s[0] == '0':
		// Skip first 0 and no need to store.
		s = s[1:]

	case '1' <= s[0] && s[0] <= '9':
		intp = s
		n := 1
		s = s[1:]
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
			n++
		}
		intp = intp[:n]

	default:
		return numberParts{}, false
	}

	// . followed by 1 or more digits.
	if len(s) >= 2 && s[0] == '.' && '0' <= s[1] && s[1] <= '9' {
		frac = s[1:]
		n := 1
		s = s[2:]
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
			n++
		}
		frac = frac[:n]
	}

	// e or E followed by an optional - or + and
	// 1 or more digits.
	if len(s) >= 2 && (s[0] == 'e' || s[0] == 'E') {
		s = s[1:]
		exp = s
		n := 0
		if s[0] == '+' || s[0] == '-' {
			s = s[1:]
			n++
			if len(s) == 0 {
				return numberParts{}, false
			}
		}
		for len(s) > 0 && '0' <= s[0] && s[0] <= '9' {
			s = s[1:]
			n++
		}
		exp = exp[:n]
	}

	return numberParts{
		neg:  neg,
		intp: intp,
		frac: bytes.TrimRight(frac, "0"), // Remove unnecessary 0s to the right.
		exp:  exp,
	}, true
}

// normalizeToIntString returns an integer string in normal form without the
// E-notation for given numberParts. It will return false if it is not an
// integer or if the exponent exceeds than max/min int value.
func normalizeToIntString(n numberParts) (string, bool) {
	intpSize := len(n.intp)
	fracSize := len(n.frac)

	if intpSize == 0 && fracSize == 0 {
		return "0", true
	}

	var exp int
	if len(n.exp) > 0 {
		i, err := strconv.ParseInt(string(n.exp), 10, 32)
		if err != nil {
			return "", false
		}
		exp = int(i)
	}

	var num []byte
	if exp >= 0 {
		// For positive E, shift fraction digits into integer part and also pad
		// with zeroes as needed.

		// If there are more digits in fraction than the E value, then the
		// number is not an integer.
		if fracSize > exp {
			return "", false
		}

		// Make sure resulting digits are within max value limit to avoid
		// unnecessarily constructing a large byte slice that may simply fail
		// later on.
		const maxDigits = 20 // Max uint64 value has 20 decimal digits.
		if intpSize+exp > maxDigits {
			return "", false
		}

		// Set cap to make a copy of integer part when appended.
		num = n.intp[:len(n.intp):len(n.intp)]
		num = append(num, n.frac...)
		for i := 0; i < exp-fracSize; i++ {
			num = append(num, '0')
		}
	} else {
		// For negative E, shift digits in integer part out.

		// If there are fractions, then the number is not an integer.
		if fracSize > 0 {
			return "", false
		}

		// index is where the decimal point will be after adjusting for negative
		// exponent.
		index := intpSize + exp
		if index < 0 {
			return "", false
		}

		num = n.intp
		// If any of the digits being shifted to the right of the decimal point
		// is non-zero, then the number is not an integer.
		for i := index; i < intpSize; i++ {
			if num[i] != '0' {
				return "", false
			}
		}
		num = num[:index]
	}

	if n.neg {
		return "-" + string(num), true
	}
	return string(num), true
}

func (d *Decoder) parseString(in []byte) (string, int, error) {
	in0 := in
	if len(in) == 0 {
		return "", 0, ErrUnexpectedEOF
	}
	if in[0] != '"' {
		return "", 0, d.newSyntaxError(d.currPos(), "invalid character %q at start of string", in[0])
	}
	in = in[1:]
	i := indexNeedEscapeInBytes(in)
	in, out := in[i:], in[:i:i] // set cap to prevent mutations
	for len(in) > 0 {
		switch r, n := utf8.DecodeRune(in); {
		case r == utf8.RuneError && n == 1:
			return "", 0, d.newSyntaxError(d.currPos(), "invalid UTF-8 in string")
		case r < ' ':
			return "", 0, d.newSyntaxError(d.currPos(), "invalid character %q in string", r)
		case r == '"':
			in = in[1:]
			n := len(in0) - len(in)
			return string(out), n, nil
		case r == '\\':
			if len(in) < 2 {
				return "", 0, ErrUnexpectedEOF
			}
			switch r := in[1]; r {
			case '"', '\\', '/':
				in, out = in[2:], append(out, r)
			case 'b':
				in, out = in[2:], append(out, '\b')
			case 'f':
				in, out = in[2:], append(out, '\f')
			case 'n':
				in, out = in[2:], append(out, '\n')
			case 'r':
				in, out = in[2:], append(out, '\r')
			case 't':
				in, out = in[2:], append(out, '\t')
			case 'u':
				if len(in) < 6 {
					return "", 0, ErrUnexpectedEOF
				}
				v, err := strconv.ParseUint(string(in[2:6]), 16, 16)
				if err != nil {
					return "", 0, d.newSyntaxError(d.currPos(), "invalid escape code %q in string", in[:6])
				}
				in = in[6:]

				r := rune(v)
				if utf16.IsSurrogate(r) {
					if len(in) < 6 {
						return "", 0, ErrUnexpectedEOF
					}
					v, err := strconv.ParseUint(string(in[2:6]), 16, 16)
					r = utf16.DecodeRune(r, rune(v))
					if in[0] != '\\' || in[1] != 'u' ||
						r == unicode.ReplacementChar || err != nil {
						return "", 0, d.newSyntaxError(d.currPos(), "invalid escape code %q in string", in[:6])
					}
					in = in[6:]
				}
				out = append(out, string(r)...)
			default:
				return "", 0, d.newSyntaxError(d.currPos(), "invalid escape code %q in string", in[:2])
			}
		default:
			i := indexNeedEscapeInBytes(in[n:])
			in, out = in[n+i:], append(out, in[:n+i]...)
		}
	}
	return "", 0, ErrUnexpectedEOF
}

// indexNeedEscapeInBytes returns the index of the character that needs
// escaping. If no characters need escaping, this returns the input length.
func indexNeedEscapeInBytes(b []byte) int {
	return indexNeedEscapeInString(util.UnsafeBytesToString(b))
}

// Kind represents a token kind expressible in the JSON format.
type Kind uint16

const (
	KdInvalid Kind = (1 << iota) / 2
	KdEOF
	KdNull
	KdBool
	KdNumber
	KdString
	KdName
	KdObjectOpen
	KdObjectClose
	KdArrayOpen
	KdArrayClose

	// comma is only for parsing in between tokens and
	// does not need to be exported.
	comma
)

func (k Kind) String() string {
	switch k {
	case KdEOF:
		return "eof"
	case KdNull:
		return "null"
	case KdBool:
		return "bool"
	case KdNumber:
		return "number"
	case KdString:
		return "string"
	case KdObjectOpen:
		return "{"
	case KdObjectClose:
		return "}"
	case KdName:
		return "name"
	case KdArrayOpen:
		return "["
	case KdArrayClose:
		return "]"
	case comma:
		return ","
	}
	return "<invalid>"
}

// Token provides a parsed token kind and value.
//
// Values are provided by the difference accessor methods. The accessor methods
// Name, Bool, and ParsedString will panic if called on the wrong kind. There
// are different accessor methods for the Number kind for converting to the
// appropriate Go numeric type and those methods have the ok return value.
type Token struct {
	// Token kind.
	kind Kind
	// pos provides the position of the token in the original input.
	pos int
	// raw bytes of the serialized token.
	// This is a subslice into the original input.
	raw []byte
	// boo is parsed boolean value.
	boo bool
	// str is parsed string value.
	str string
}

// Kind returns the token kind.
func (t Token) Kind() Kind {
	return t.kind
}

// RawString returns the read value in string.
func (t Token) RawString() string {
	return string(t.raw)
}

// Pos returns the token position from the input.
func (t Token) Pos() int {
	return t.pos
}

// Name returns the object name if token is Name, else it panics.
func (t Token) Name() string {
	if t.kind == KdName {
		return t.str
	}
	panic(fmt.Sprintf("Token is not a Name: %v", t.RawString()))
}

// Bool returns the bool value if token kind is Bool, else it panics.
func (t Token) Bool() bool {
	if t.kind == KdBool {
		return t.boo
	}
	panic(fmt.Sprintf("Token is not a Bool: %v", t.RawString()))
}

// ParsedString returns the string value for a JSON string token or the read
// value in string if token is not a string.
func (t Token) ParsedString() string {
	if t.kind == KdString {
		return t.str
	}
	panic(fmt.Sprintf("Token is not a String: %v", t.RawString()))
}

// Float returns the floating-point number if token kind is Number.
//
// The floating-point precision is specified by the bitSize parameter: 32 for
// float32 or 64 for float64. If bitSize=32, the result still has type float64,
// but it will be convertible to float32 without changing its value. It will
// return false if the number exceeds the floating point limits for given
// bitSize.
func (t Token) Float(bitSize int) (float64, bool) {
	if t.kind != KdNumber {
		return 0, false
	}
	f, err := strconv.ParseFloat(t.RawString(), bitSize)
	if err != nil {
		return 0, false
	}
	return f, true
}

// Int returns the signed integer number if token is Number.
//
// The given bitSize specifies the integer type that the result must fit into.
// It returns false if the number is not an integer value or if the result
// exceeds the limits for given bitSize.
func (t Token) Int(bitSize int) (int64, bool) {
	s, ok := t.getIntStr()
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, bitSize)
	if err != nil {
		return 0, false
	}
	return n, true
}

// Uint returns the signed integer number if token is Number.
//
// The given bitSize specifies the unsigned integer type that the result must
// fit into. It returns false if the number is not an unsigned integer value
// or if the result exceeds the limits for given bitSize.
func (t Token) Uint(bitSize int) (uint64, bool) {
	s, ok := t.getIntStr()
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(s, 10, bitSize)
	if err != nil {
		return 0, false
	}
	return n, true
}

func (t Token) getIntStr() (string, bool) {
	if t.kind != KdNumber {
		return "", false
	}
	parts, ok := parseNumberParts(t.raw)
	if !ok {
		return "", false
	}
	return normalizeToIntString(parts)
}

// TokenEquals returns true if given Tokens are equal, else false.
func TokenEquals(x, y Token) bool {
	return x.kind == y.kind &&
		x.pos == y.pos &&
		bytes.Equal(x.raw, y.raw) &&
		x.boo == y.boo &&
		x.str == y.str
}

// indexNeedEscapeInString returns the index of the character that needs
// escaping. If no characters need escaping, this returns the input length.
func indexNeedEscapeInString(s string) int {
	for i, r := range s {
		if r < ' ' || r == '\\' || r == '"' || r == utf8.RuneError {
			return i
		}
	}
	return len(s)
}

func appendString(out []byte, in string) ([]byte, error) {
	out = append(out, '"')
	i := indexNeedEscapeInString(in)
	in, out = in[i:], append(out, in[:i]...)
	for len(in) > 0 {
		switch r, n := utf8.DecodeRuneInString(in); {
		case r == utf8.RuneError && n == 1:
			return out, moerr.NewInvalidInputNoCtx("invalid UTF-8")
		case r < ' ' || r == '"' || r == '\\':
			out = append(out, '\\')
			switch r {
			case '"', '\\':
				out = append(out, byte(r))
			case '\b':
				out = append(out, 'b')
			case '\f':
				out = append(out, 'f')
			case '\n':
				out = append(out, 'n')
			case '\r':
				out = append(out, 'r')
			case '\t':
				out = append(out, 't')
			default:
				out = append(out, 'u')
				out = append(out, "0000"[1+(bits.Len32(uint32(r))-1)/4:]...)
				out = strconv.AppendUint(out, uint64(r), 16)
			}
			in = in[n:]
		default:
			i := indexNeedEscapeInString(in[n:])
			in, out = in[n+i:], append(out, in[:n+i]...)
		}
	}
	out = append(out, '"')
	return out, nil
}
