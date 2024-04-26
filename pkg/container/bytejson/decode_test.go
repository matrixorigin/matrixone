// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bytejson_test

import (
	"fmt"
	"testing"

	json "github.com/matrixorigin/matrixone/pkg/container/bytejson"
)

func BenchmarkFloat(b *testing.B) {
	input := []byte(`1.797693134862315708145274237317043567981e+308`)
	for i := 0; i < b.N; i++ {
		dec := json.NewDecoder(input)
		val, err := dec.Read()
		if err != nil {
			b.Fatal(err)
		}
		if _, ok := val.Float(64); !ok {
			b.Fatal("not a float")
		}
	}
}

func BenchmarkInt(b *testing.B) {
	input := []byte(`922337203.6854775807e+10`)
	for i := 0; i < b.N; i++ {
		dec := json.NewDecoder(input)
		val, err := dec.Read()
		if err != nil {
			b.Fatal(err)
		}
		if _, ok := val.Int(64); !ok {
			b.Fatal("not an int64")
		}
	}
}

func BenchmarkString(b *testing.B) {
	input := []byte(`"abcdefghijklmnopqrstuvwxyz0123456789\\n\\t"`)
	for i := 0; i < b.N; i++ {
		dec := json.NewDecoder(input)
		val, err := dec.Read()
		if err != nil {
			b.Fatal(err)
		}
		_ = val.ParsedString()
	}
}

func BenchmarkBool(b *testing.B) {
	input := []byte(`true`)
	for i := 0; i < b.N; i++ {
		dec := json.NewDecoder(input)
		val, err := dec.Read()
		if err != nil {
			b.Fatal(err)
		}
		_ = val.Bool()
	}
}

type R struct {
	// E is expected error substring from calling Decoder.Read if set.
	E string
	// V is one of the checker implementations that validates the token value.
	V checker
	// P is expected Token.Pos() if set > 0.
	P int
	// RS is expected result from Token.RawString() if not empty.
	RS string
}

// checker defines API for Token validation.
type checker interface {
	// check checks and expects for token API call to return and compare
	// against implementation-stored value. Returns empty string if success,
	// else returns error message describing the error.
	check(json.Token) string
}

// checkers that checks the token kind only.
var (
	EOF         = kindOnly{json.KdEOF}
	Null        = kindOnly{json.KdNull}
	ObjectOpen  = kindOnly{json.KdObjectOpen}
	ObjectClose = kindOnly{json.KdObjectClose}
	ArrayOpen   = kindOnly{json.KdArrayOpen}
	ArrayClose  = kindOnly{json.KdArrayClose}
)

type kindOnly struct {
	want json.Kind
}

func (x kindOnly) check(tok json.Token) string {
	if got := tok.Kind(); got != x.want {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, x.want)
	}
	return ""
}

type Name struct {
	val string
}

func (x Name) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdName {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdName)
	}

	if got := tok.Name(); got != x.val {
		return fmt.Sprintf("Token.Name(): got %v, want %v", got, x.val)
	}
	return ""
}

type Bool struct {
	val bool
}

func (x Bool) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdBool {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdBool)
	}

	if got := tok.Bool(); got != x.val {
		return fmt.Sprintf("Token.Bool(): got %v, want %v", got, x.val)
	}
	return ""
}

type Str struct {
	val string
}

func (x Str) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdString {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdString)
	}

	if got := tok.ParsedString(); got != x.val {
		return fmt.Sprintf("Token.ParsedString(): got %v, want %v", got, x.val)
	}
	return ""
}

type F64 struct {
	val float64
}

func (x F64) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdNumber {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdNumber)
	}

	got, ok := tok.Float(64)
	if !ok {
		return fmt.Sprintf("Token.Float(64): returned not ok")
	}
	if got != x.val {
		return fmt.Sprintf("Token.Float(64): got %v, want %v", got, x.val)
	}
	return ""
}

type F32 struct {
	val float32
}

func (x F32) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdNumber {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdNumber)
	}

	got, ok := tok.Float(32)
	if !ok {
		return fmt.Sprintf("Token.Float(32): returned not ok")
	}
	if float32(got) != x.val {
		return fmt.Sprintf("Token.Float(32): got %v, want %v", got, x.val)
	}
	return ""
}

// NotF64 is a checker to validate a Number token where Token.Float(64) returns not ok.
var NotF64 = xf64{}

type xf64 struct{}

func (x xf64) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdNumber {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdNumber)
	}

	_, ok := tok.Float(64)
	if ok {
		return fmt.Sprintf("Token.Float(64): returned ok")
	}
	return ""
}

// NotF32 is a checker to validate a Number token where Token.Float(32) returns not ok.
var NotF32 = xf32{}

type xf32 struct{}

func (x xf32) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdNumber {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdNumber)
	}

	_, ok := tok.Float(32)
	if ok {
		return fmt.Sprintf("Token.Float(32): returned ok")
	}
	return ""
}

type I64 struct {
	val int64
}

func (x I64) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdNumber {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdNumber)
	}

	got, ok := tok.Int(64)
	if !ok {
		return fmt.Sprintf("Token.Int(64): returned not ok")
	}
	if got != x.val {
		return fmt.Sprintf("Token.Int(64): got %v, want %v", got, x.val)
	}
	return ""
}

type I32 struct {
	val int32
}

func (x I32) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdNumber {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdNumber)
	}

	got, ok := tok.Int(32)
	if !ok {
		return fmt.Sprintf("Token.Int(32): returned not ok")
	}
	if int32(got) != x.val {
		return fmt.Sprintf("Token.Int(32): got %v, want %v", got, x.val)
	}
	return ""
}

// NotI64 is a checker to validate a Number token where Token.Int(64) returns not ok.
var NotI64 = xi64{}

type xi64 struct{}

func (x xi64) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdNumber {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdNumber)
	}

	_, ok := tok.Int(64)
	if ok {
		return fmt.Sprintf("Token.Int(64): returned ok")
	}
	return ""
}

// NotI32 is a checker to validate a Number token where Token.Int(32) returns not ok.
var NotI32 = xi32{}

type xi32 struct{}

func (x xi32) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdNumber {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdNumber)
	}

	_, ok := tok.Int(32)
	if ok {
		return fmt.Sprintf("Token.Int(32): returned ok")
	}
	return ""
}

type Ui64 struct {
	val uint64
}

func (x Ui64) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdNumber {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdNumber)
	}

	got, ok := tok.Uint(64)
	if !ok {
		return fmt.Sprintf("Token.Uint(64): returned not ok")
	}
	if got != x.val {
		return fmt.Sprintf("Token.Uint(64): got %v, want %v", got, x.val)
	}
	return ""
}

type Ui32 struct {
	val uint32
}

func (x Ui32) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdNumber {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdNumber)
	}

	got, ok := tok.Uint(32)
	if !ok {
		return fmt.Sprintf("Token.Uint(32): returned not ok")
	}
	if uint32(got) != x.val {
		return fmt.Sprintf("Token.Uint(32): got %v, want %v", got, x.val)
	}
	return ""
}

// NotUi64 is a checker to validate a Number token where Token.Uint(64) returns not ok.
var NotUi64 = xui64{}

type xui64 struct{}

func (x xui64) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdNumber {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdNumber)
	}

	_, ok := tok.Uint(64)
	if ok {
		return fmt.Sprintf("Token.Uint(64): returned ok")
	}
	return ""
}

// NotI32 is a checker to validate a Number token where Token.Uint(32) returns not ok.
var NotUi32 = xui32{}

type xui32 struct{}

func (x xui32) check(tok json.Token) string {
	if got := tok.Kind(); got != json.KdNumber {
		return fmt.Sprintf("Token.Kind(): got %v, want %v", got, json.KdNumber)
	}

	_, ok := tok.Uint(32)
	if ok {
		return fmt.Sprintf("Token.Uint(32): returned ok")
	}
	return ""
}

var errEOF = json.ErrUnexpectedEOF.Error()

func TestDecoder(t *testing.T) {
}

func checkToken(t *testing.T, tok json.Token, idx int, r R, in string) {
}

func errorf(t *testing.T, in string, fmtStr string, args ...interface{}) {
}

func TestClone(t *testing.T) {
}

func compareDecoders(t *testing.T, d1 *json.Decoder, d2 *json.Decoder) {
}
