// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build ignore
// +build ignore

package main

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"os"
	"strings"
	"text/template"
)

// lrt is a structure to generate binary-operator files.
type lrt struct {
	LTYP types.T
	RTYP types.T
	RET  types.T
}

// tr is a structure to generate unary-operator files.
type tr struct {
	LTYP types.T
	RET  types.T
}

var (
	// numerics contains all number type in mo. used in some generate function.
	numerics = []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
	}

	// chars contains all string types in mo. they're different to numerics because they can't
	// do plus, minus, div and some operations directly. And in some operation, such as cast op.
	// string type can not use numerics' template but should do special treatment.
	chars = []types.T{
		types.T_char, types.T_varchar,
	}

	// dates contains all time-related types in mo. It has its own compute and express logic, so
	// it is different to other types and can not use the same template as others.
	dates = []types.T{
		types.T_date, types.T_datetime, types.T_timestamp,
	}
)

// map of file name and its generate function.
var filesAndFunctions map[string]func() error = map[string]func() error{
	// binary operators.
	"plus.go":  GeneratePlus,
	"minus.go": GenerateMinus,
	"mult.go":  GenerateMult,
	"div.go":   GenerateDiv,
	"mod.go":   GenerateMod,
	"and.go":   GenerateAnd,
	"or.go":    GenerateOr,
	"like.go":  GenerateLike,
	// unary operators.
	"unaryops.go": GenerateUnaryOperators,
	// binary comparison operators.
	"eq.go": GenerateEqual,
	"ge.go": GenerateGreatEqual,
	"gt.go": GenerateGreatThan,
	"le.go": GenerateLessEqual,
	"lt.go": GenerateLessThan,
	"ne.go": GenerateNotEqual,

	"cast.go": GenerateCast,
}

func main() {
	// do generate work for overload/files.go
	var err error
	for name, f := range filesAndFunctions {
		if f == nil { // no function to generate it.
			continue
		}
		os.Remove(name) // remove old files to avoid make-build error
		if err = f(); err != nil {
			panic(err)
		}
	}
}

// GeneratePlus makes plus.go
func GeneratePlus() error {
	// read template file
	plusTemplate, err := os.ReadFile("plus.template")
	if err != nil {
		return err
	}
	tempText := string(plusTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("plus").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		Field1 []lrt
		Field2 []lrt
		Field3 []lrt
	}

	var pTs = pts{
		[]lrt{ // same type
			{types.T_int8, types.T_int8, types.T_int8},
			{types.T_int16, types.T_int16, types.T_int16},
			{types.T_int32, types.T_int32, types.T_int32},
			{types.T_int64, types.T_int64, types.T_int64},
			{types.T_uint8, types.T_uint8, types.T_uint8},
			{types.T_uint16, types.T_uint16, types.T_uint16},
			{types.T_uint32, types.T_uint32, types.T_uint32},
			{types.T_uint64, types.T_uint64, types.T_uint64},
			{types.T_float32, types.T_float32, types.T_float32},
			{types.T_float64, types.T_float64, types.T_float64},
		},
		[]lrt{ // same family : low - high - high
			{types.T_int8, types.T_int64, types.T_int64},
			{types.T_int16, types.T_int64, types.T_int64},
			{types.T_int32, types.T_int64, types.T_int64},
			{types.T_int8, types.T_int32, types.T_int32},
			{types.T_int16, types.T_int32, types.T_int32},
			{types.T_int8, types.T_int16, types.T_int16},
			{types.T_float32, types.T_float64, types.T_float64},
			{types.T_uint8, types.T_uint64, types.T_uint64},
			{types.T_uint16, types.T_uint64, types.T_uint64},
			{types.T_uint32, types.T_uint64, types.T_uint64},
			{types.T_uint8, types.T_uint32, types.T_uint32},
			{types.T_uint16, types.T_uint32, types.T_uint32},
			{types.T_uint8, types.T_uint16, types.T_uint16},
		},
		[]lrt{ // same family : high - low - high
			{types.T_int64, types.T_int8, types.T_int64},
			{types.T_int64, types.T_int16, types.T_int64},
			{types.T_int64, types.T_int32, types.T_int64},
			{types.T_int32, types.T_int16, types.T_int32},
			{types.T_int32, types.T_int8, types.T_int32},
			{types.T_int16, types.T_int8, types.T_int16},
			{types.T_float64, types.T_float32, types.T_float64},
			{types.T_uint64, types.T_uint8, types.T_uint64},
			{types.T_uint64, types.T_uint16, types.T_uint64},
			{types.T_uint64, types.T_uint32, types.T_uint64},
			{types.T_uint32, types.T_uint16, types.T_uint32},
			{types.T_uint32, types.T_uint8, types.T_uint32},
			{types.T_uint16, types.T_uint8, types.T_uint16},
		},
	}

	file, err := os.OpenFile("plus.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "plus", pTs)
	file.Close()
	return err
}

// GenerateMinus makes minus.go
func GenerateMinus() error {
	// read template file
	minusTemplate, err := os.ReadFile("minus.template")
	if err != nil {
		return err
	}
	tempText := string(minusTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("minus").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		Field1 []lrt
		Field2 []lrt
		Field3 []lrt
	}

	var pTs = pts{
		[]lrt{ // same type
			{types.T_int8, types.T_int8, types.T_int8},
			{types.T_int16, types.T_int16, types.T_int16},
			{types.T_int32, types.T_int32, types.T_int32},
			{types.T_int64, types.T_int64, types.T_int64},
			{types.T_uint8, types.T_uint8, types.T_uint8},
			{types.T_uint16, types.T_uint16, types.T_uint16},
			{types.T_uint32, types.T_uint32, types.T_uint32},
			{types.T_uint64, types.T_uint64, types.T_uint64},
			{types.T_float32, types.T_float32, types.T_float32},
			{types.T_float64, types.T_float64, types.T_float64},
		},
		[]lrt{ // low - high - high
			{types.T_int32, types.T_int64, types.T_int64},
			{types.T_int16, types.T_int64, types.T_int64},
			{types.T_int8, types.T_int64, types.T_int64},
			{types.T_int16, types.T_int32, types.T_int32},
			{types.T_int8, types.T_int32, types.T_int32},
			{types.T_int8, types.T_int16, types.T_int16},
			{types.T_float32, types.T_float64, types.T_float64},
			{types.T_uint32, types.T_uint64, types.T_uint64},
			{types.T_uint16, types.T_uint64, types.T_uint64},
			{types.T_uint8, types.T_uint64, types.T_uint64},
			{types.T_uint16, types.T_uint32, types.T_uint32},
			{types.T_uint8, types.T_uint32, types.T_uint32},
			{types.T_uint8, types.T_uint16, types.T_uint16},
		},
		[]lrt{ // high - low - high
			{types.T_int64, types.T_int32, types.T_int64},
			{types.T_int64, types.T_int16, types.T_int64},
			{types.T_int64, types.T_int8, types.T_int64},
			{types.T_int32, types.T_int16, types.T_int32},
			{types.T_int32, types.T_int8, types.T_int32},
			{types.T_int16, types.T_int8, types.T_int16},
			{types.T_float64, types.T_float32, types.T_float64},
			{types.T_uint64, types.T_uint32, types.T_uint64},
			{types.T_uint64, types.T_uint16, types.T_uint64},
			{types.T_uint64, types.T_uint8, types.T_uint64},
			{types.T_uint32, types.T_uint16, types.T_uint32},
			{types.T_uint32, types.T_uint8, types.T_uint32},
			{types.T_uint16, types.T_uint8, types.T_uint16},
		},
	}

	file, err := os.OpenFile("minus.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "minus", pTs)
	file.Close()
	return err
}

// GenerateMult makes mult.go
func GenerateMult() error {
	// read template file
	multTemplate, err := os.ReadFile("mult.template")
	if err != nil {
		return err
	}
	tempText := string(multTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("mult").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		Field1 []lrt
		Field2 []lrt
		Field3 []lrt
	}

	var pTs = pts{
		[]lrt{ // same type
			{types.T_int8, types.T_int8, types.T_int8},
			{types.T_int16, types.T_int16, types.T_int16},
			{types.T_int32, types.T_int32, types.T_int32},
			{types.T_int64, types.T_int64, types.T_int64},
			{types.T_uint8, types.T_uint8, types.T_uint8},
			{types.T_uint16, types.T_uint16, types.T_uint16},
			{types.T_uint32, types.T_uint32, types.T_uint32},
			{types.T_uint64, types.T_uint64, types.T_uint64},
			{types.T_float32, types.T_float32, types.T_float32},
			{types.T_float64, types.T_float64, types.T_float64},
		},
		[]lrt{ // low - high - high
			{types.T_int32, types.T_int64, types.T_int64},
			{types.T_int16, types.T_int64, types.T_int64},
			{types.T_int8, types.T_int64, types.T_int64},
			{types.T_int16, types.T_int32, types.T_int32},
			{types.T_int8, types.T_int32, types.T_int32},
			{types.T_int8, types.T_int16, types.T_int16},
			{types.T_float32, types.T_float64, types.T_float64},
			{types.T_uint32, types.T_uint64, types.T_uint64},
			{types.T_uint16, types.T_uint64, types.T_uint64},
			{types.T_uint8, types.T_uint64, types.T_uint64},
			{types.T_uint16, types.T_uint32, types.T_uint32},
			{types.T_uint8, types.T_uint32, types.T_uint32},
			{types.T_uint8, types.T_uint16, types.T_uint16},
		},
		[]lrt{ // high - low - high
			{types.T_int64, types.T_int32, types.T_int64},
			{types.T_int64, types.T_int16, types.T_int64},
			{types.T_int64, types.T_int8, types.T_int64},
			{types.T_int32, types.T_int16, types.T_int32},
			{types.T_int32, types.T_int8, types.T_int32},
			{types.T_int16, types.T_int8, types.T_int16},
			{types.T_float64, types.T_float32, types.T_float64},
			{types.T_uint64, types.T_uint32, types.T_uint64},
			{types.T_uint64, types.T_uint16, types.T_uint64},
			{types.T_uint64, types.T_uint8, types.T_uint64},
			{types.T_uint32, types.T_uint16, types.T_uint32},
			{types.T_uint32, types.T_uint8, types.T_uint32},
			{types.T_uint16, types.T_uint8, types.T_uint16},
		},
	}

	file, err := os.OpenFile("mult.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "mult", pTs)
	file.Close()
	return err
}

// GenerateDiv makes div.go
func GenerateDiv() error {
	// TODO(m-schen): only support div operator between float family now. Because
	// `int div int` should return Decimal, And `uint div uint` shoule return Decimal Unsigned.
	// But we do not support Decimal Type now.
	// Div between int and int, int and float, int and uint, and so on will do type conversion first.
	// Rules is that :
	// 		t / t 			  ==> 	float64 / float64
	// 		t / float32		  ==>	float64 / float64
	//		t / float64 	  ==> 	float64 / float64
	// 		float32 / float64 ==> 	float64 / float64
	// 		float32 / float32 ==> 	float32 / float32
	// 		float64 / float64 ==> 	float64 / float64
	// t contains all int type and uint type.

	// read template file
	divTemplate, err := os.ReadFile("div.template")
	if err != nil {
		return err
	}
	tempText := string(divTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("div").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		Div        []lrt
		IntegerDiv []lrt
	}

	var pTs = pts{
		[]lrt{
			{types.T_float32, types.T_float32, types.T_float32},
			{types.T_float64, types.T_float64, types.T_float64},
		},
		[]lrt{
			{types.T_float32, types.T_float32, types.T_int64},
			{types.T_float64, types.T_float64, types.T_int64},
		},
	}

	file, err := os.OpenFile("div.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "div", pTs)
	file.Close()
	return err
}

// GenerateMod makes mod.go
func GenerateMod() error {
	// read template file
	modTemplate, err := os.ReadFile("mod.template")
	if err != nil {
		return err
	}
	tempText := string(modTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("mod").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		Field1 []lrt
		Field2 []lrt
		Field3 []lrt
	}

	var pTs = pts{
		[]lrt{ // same type
			{types.T_int8, types.T_int8, types.T_int8},
			{types.T_int16, types.T_int16, types.T_int16},
			{types.T_int32, types.T_int32, types.T_int32},
			{types.T_int64, types.T_int64, types.T_int64},
			{types.T_uint8, types.T_uint8, types.T_uint8},
			{types.T_uint16, types.T_uint16, types.T_uint16},
			{types.T_uint32, types.T_uint32, types.T_uint32},
			{types.T_uint64, types.T_uint64, types.T_uint64},
			{types.T_float32, types.T_float32, types.T_float32},
			{types.T_float64, types.T_float64, types.T_float64},
		},
		[]lrt{},
		[]lrt{},
	}

	file, err := os.OpenFile("mod.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "mod", pTs)
	file.Close()
	return err
}

// GenerateNot makes unaryops.go
func GenerateUnaryOperators() error {
	// read template file
	unaryTemplate, err := os.ReadFile("unaryops.template")
	if err != nil {
		return err
	}
	tempText := string(unaryTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("unaryops").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		Minus []tr
		Not   []tr
	}

	var pTs pts = pts{
		Minus: []tr{
			{types.T_int8, types.T_int8},
			{types.T_int16, types.T_int16},
			{types.T_int32, types.T_int32},
			{types.T_int64, types.T_int64},
			{types.T_float32, types.T_float32},
			{types.T_float64, types.T_float64},
		},
		Not: nil,
	}
	for _, num := range numerics {
		if num == types.T_int8 { // this already implemented directly in the template
			continue
		}
		pTs.Not = append(pTs.Not, tr{num, types.T_int8})
	}

	file, err := os.OpenFile("unaryops.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "unaryops", pTs)
	file.Close()
	return err
}

// GenerateAnd makes and.go
func GenerateAnd() error {
	modTemplate, err := os.ReadFile("and.template")
	if err != nil {
		return err
	}
	tempText := string(modTemplate)

	temp, err := template.New("and").Parse(tempText)
	if err != nil {
		return err
	}

	file, err := os.OpenFile("and.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "and", struct{}{})
	file.Close()
	return err
}

// GenerateOr makes or.go
func GenerateOr() error {
	modTemplate, err := os.ReadFile("or.template")
	if err != nil {
		return err
	}
	tempText := string(modTemplate)

	temp, err := template.New("or").Parse(tempText)
	if err != nil {
		return err
	}

	file, err := os.OpenFile("or.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "or", struct{}{})
	file.Close()
	return err
}

// GenerateCast makes mod.go
func GenerateCast() error {
	// read template file
	modTemplate, err := os.ReadFile("cast.template")
	if err != nil {
		return err
	}
	tempText := string(modTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("cast").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		SameType    []lrt
		SameType2   []lrt
		LeftToRight []lrt
		Specials1   []lrt // left type is T_char or T_varchar
		Specials2   []lrt // right type is T_char or T_varchar
		Specials3   []lrt // conversion between char and varchar
		Specials4   []lrt // cast ints to decimal128
	}

	var pTs = pts{
		nil,
		nil,
		nil,
		nil,
		nil,
		[]lrt{
			{types.T_char, types.T_char, types.T_char},
			{types.T_varchar, types.T_varchar, types.T_varchar},
			{types.T_char, types.T_varchar, types.T_varchar},
			{types.T_varchar, types.T_char, types.T_char},
		},
		[]lrt{},
	}
	// init source data
	for _, typ1 := range numerics {
		for _, typ2 := range numerics {
			if typ1 == typ2 {
				pTs.SameType = append(pTs.SameType, lrt{typ1, typ1, typ1})
			} else {
				pTs.LeftToRight = append(pTs.LeftToRight, lrt{typ2, typ1, typ1})
			}
		}
	}
	for _, timeType := range dates {
		pTs.SameType2 = append(pTs.SameType2, lrt{timeType, timeType, timeType})
	}
	for _, numType := range numerics {
		pTs.Specials1 = append(pTs.Specials1, []lrt{
			{types.T_char, numType, numType},
			{types.T_varchar, numType, numType},
		}...)
		pTs.Specials2 = append(pTs.Specials2, []lrt{
			{numType, types.T_char, types.T_char},
			{numType, types.T_varchar, types.T_varchar},
		}...)
	}
	ints := []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	}
	for _, intType := range ints {
		pTs.Specials4 = append(pTs.Specials4, lrt{intType, types.T_decimal128, types.T_decimal128})
	}

	file, err := os.OpenFile("cast.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "cast", pTs)
	file.Close()
	return err
}

// GenerateEqual makes eq.go
func GenerateEqual() error {
	modTemplate, err := os.ReadFile("eq.template")
	if err != nil {
		return err
	}
	tempText := string(modTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("eq").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		Numerics  []lrt
		CharTypes []lrt
	}
	var pTs = pts{
		nil,
		nil,
	}

	for _, p := range numerics {
		pTs.Numerics = append(pTs.Numerics, lrt{p, p, types.T_sel})
	}
	for _, p := range chars {
		pTs.CharTypes = append(pTs.CharTypes, lrt{p, p, types.T_sel})
	}

	file, err := os.OpenFile("eq.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "eq", pTs)
	file.Close()
	return err
}

// GenerateGreatEqual makes ge.go
func GenerateGreatEqual() error {
	modTemplate, err := os.ReadFile("ge.template")
	if err != nil {
		return err
	}
	tempText := string(modTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("ge").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		Numerics  []lrt
		CharTypes []lrt
	}
	var pTs = pts{
		nil,
		nil,
	}

	for _, p := range numerics {
		pTs.Numerics = append(pTs.Numerics, lrt{p, p, types.T_sel})
	}
	for _, p := range chars {
		pTs.CharTypes = append(pTs.CharTypes, lrt{p, p, types.T_sel})
	}

	file, err := os.OpenFile("ge.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "ge", pTs)
	file.Close()
	return err
}

// GenerateGreatThan makes gt.go
func GenerateGreatThan() error {
	modTemplate, err := os.ReadFile("gt.template")
	if err != nil {
		return err
	}
	tempText := string(modTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("gt").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		Numerics  []lrt
		CharTypes []lrt
	}
	var pTs = pts{
		nil,
		nil,
	}

	for _, p := range numerics {
		pTs.Numerics = append(pTs.Numerics, lrt{p, p, types.T_sel})
	}
	for _, p := range chars {
		pTs.CharTypes = append(pTs.CharTypes, lrt{p, p, types.T_sel})
	}

	file, err := os.OpenFile("gt.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "gt", pTs)
	file.Close()
	return err
}

// GenerateLessEqual makes le.go
func GenerateLessEqual() error {
	modTemplate, err := os.ReadFile("le.template")
	if err != nil {
		return err
	}
	tempText := string(modTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("le").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		Numerics  []lrt
		CharTypes []lrt
	}
	var pTs = pts{
		nil,
		nil,
	}

	for _, p := range numerics {
		pTs.Numerics = append(pTs.Numerics, lrt{p, p, types.T_sel})
	}
	for _, p := range chars {
		pTs.CharTypes = append(pTs.CharTypes, lrt{p, p, types.T_sel})
	}

	file, err := os.OpenFile("le.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "le", pTs)
	file.Close()
	return err
}

// GenerateLessThan makes lt.go
func GenerateLessThan() error {
	modTemplate, err := os.ReadFile("lt.template")
	if err != nil {
		return err
	}
	tempText := string(modTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("lt").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		Numerics  []lrt
		CharTypes []lrt
	}
	var pTs = pts{
		nil,
		nil,
	}

	for _, p := range numerics {
		pTs.Numerics = append(pTs.Numerics, lrt{p, p, types.T_sel})
	}
	for _, p := range chars {
		pTs.CharTypes = append(pTs.CharTypes, lrt{p, p, types.T_sel})
	}

	file, err := os.OpenFile("lt.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "lt", pTs)
	file.Close()
	return err
}

// GenerateNotEqual makes ne.go
func GenerateNotEqual() error {
	modTemplate, err := os.ReadFile("ne.template")
	if err != nil {
		return err
	}
	tempText := string(modTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("ne").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		Numerics  []lrt
		CharTypes []lrt
	}
	var pTs = pts{
		nil,
		nil,
	}

	for _, p := range numerics {
		pTs.Numerics = append(pTs.Numerics, lrt{p, p, types.T_sel})
	}
	for _, p := range chars {
		pTs.CharTypes = append(pTs.CharTypes, lrt{p, p, types.T_sel})
	}

	file, err := os.OpenFile("ne.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "ne", pTs)
	file.Close()
	return err
}

// GenerateLike makes like.go
func GenerateLike() error {
	modTemplate, err := os.ReadFile("like.template")
	if err != nil {
		return err
	}
	tempText := string(modTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("like").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		CharTypes []lrt
	}
	var pTs = pts{nil}
	for _, p1 := range chars {
		for _, p2 := range chars {
			pTs.CharTypes = append(pTs.CharTypes, lrt{p1, p2, types.T_sel})
		}
	}

	file, err := os.OpenFile("like.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "like", pTs)
	file.Close()
	return err
}

func rewriteTemplateText(tempText string) string {
	tempText = strings.ReplaceAll(tempText, ".LEFT_TYPE_OID", ".{{.LTYP.OidString}}")
	tempText = strings.ReplaceAll(tempText, ".RIGHT_TYPE_OID", ".{{.RTYP.OidString}}")
	tempText = strings.ReplaceAll(tempText, ".RETURN_TYPE_OID", ".{{.RET.OidString}}")
	tempText = strings.ReplaceAll(tempText, "L_GO_TYPE", "{{.LTYP.GoType}}")
	tempText = strings.ReplaceAll(tempText, "R_GO_TYPE", "{{.RTYP.GoType}}")
	tempText = strings.ReplaceAll(tempText, "RETURN_GO_TYPE", "{{.RET.GoType}}")
	tempText = strings.ReplaceAll(tempText, "{.LTYP}", "{{.LTYP.GoGoType}}")
	tempText = strings.ReplaceAll(tempText, "{.RTYP}", "{{.RTYP.GoGoType}}")
	tempText = strings.ReplaceAll(tempText, "{.RETTYP}", "{{.RET.GoGoType}}")
	tempText = strings.ReplaceAll(tempText, "{.RETURN_TYPE_LEN}", "{{.RET.TypeLen}}")

	return tempText
}
