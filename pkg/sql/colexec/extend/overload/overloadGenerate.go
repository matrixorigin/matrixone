// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build ignore
// +build ignore
package main

import (
	"matrixone/pkg/container/types"
	"os"
	"strings"
	"text/template"
)

type lrt struct {
	LTYP types.T
	RTYP types.T
	RET  types.T
}

var files []string = []string{"plus.go", "minus.go", "mult.go", "div.go", "mod.go"}

func main() {
	// remove old files to avoid make-build error
	for _, file := range files {
		os.Remove(file)
	}
	// do generate work for overload/files.go
	var err error
	err = GeneratePlus()
	if err != nil {
		panic(err)
	}
	err = GenerateMinus()
	if err != nil {
		panic(err)
	}
	err = GenerateMult()
	if err != nil {
		panic(err)
	}
	err = GenerateDiv()
	if err != nil {
		panic(err)
	}
	err = GenerateMod()
	if err != nil {
		panic(err)
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
		[]lrt{ // low - high - high
			{types.T_int8, types.T_int16, types.T_int16},
			{types.T_int8, types.T_int32, types.T_int32},
			{types.T_int8, types.T_int64, types.T_int64},
			{types.T_int16, types.T_int32, types.T_int32},
			{types.T_int16, types.T_int64, types.T_int64},
			{types.T_int32, types.T_int64, types.T_int64},

			{types.T_float32, types.T_float64, types.T_float64},

			{types.T_uint8, types.T_uint16, types.T_uint16},
			{types.T_uint8, types.T_uint32, types.T_uint32},
			{types.T_uint8, types.T_uint64, types.T_uint64},
			{types.T_uint16, types.T_uint32, types.T_uint32},
			{types.T_uint16, types.T_uint64, types.T_uint64},
			{types.T_uint32, types.T_uint64, types.T_uint64},
		},
		[]lrt{ // high - low - high
			{types.T_int16, types.T_int8, types.T_int16},
			{types.T_int32, types.T_int8, types.T_int32},
			{types.T_int64, types.T_int8, types.T_int64},
			{types.T_int32, types.T_int16, types.T_int32},
			{types.T_int64, types.T_int16, types.T_int64},
			{types.T_int64, types.T_int32, types.T_int64},

			{types.T_float64, types.T_float32, types.T_float64},

			{types.T_uint16, types.T_uint8, types.T_uint16},
			{types.T_uint32, types.T_uint8, types.T_uint32},
			{types.T_uint64, types.T_uint8, types.T_uint64},
			{types.T_uint32, types.T_uint16, types.T_uint32},
			{types.T_uint64, types.T_uint32, types.T_uint64},
			{types.T_uint64, types.T_uint32, types.T_uint64},
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
	plusTemplate, err := os.ReadFile("minus.template")
	if err != nil {
		return err
	}
	tempText := string(plusTemplate)
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
			{types.T_int8, types.T_int16, types.T_int16},
			{types.T_int8, types.T_int32, types.T_int32},
			{types.T_int8, types.T_int64, types.T_int64},
			{types.T_int16, types.T_int32, types.T_int32},
			{types.T_int16, types.T_int64, types.T_int64},
			{types.T_int32, types.T_int64, types.T_int64},

			{types.T_float32, types.T_float64, types.T_float64},

			{types.T_uint8, types.T_uint16, types.T_uint16},
			{types.T_uint8, types.T_uint32, types.T_uint32},
			{types.T_uint8, types.T_uint64, types.T_uint64},
			{types.T_uint16, types.T_uint32, types.T_uint32},
			{types.T_uint16, types.T_uint64, types.T_uint64},
			{types.T_uint32, types.T_uint64, types.T_uint64},
		},
		[]lrt{ // high - low - high
			{types.T_int16, types.T_int8, types.T_int16},
			{types.T_int32, types.T_int8, types.T_int32},
			{types.T_int64, types.T_int8, types.T_int64},
			{types.T_int32, types.T_int16, types.T_int32},
			{types.T_int64, types.T_int16, types.T_int64},
			{types.T_int64, types.T_int32, types.T_int64},

			{types.T_float64, types.T_float32, types.T_float64},

			{types.T_uint16, types.T_uint8, types.T_uint16},
			{types.T_uint32, types.T_uint8, types.T_uint32},
			{types.T_uint64, types.T_uint8, types.T_uint64},
			{types.T_uint32, types.T_uint16, types.T_uint32},
			{types.T_uint64, types.T_uint32, types.T_uint64},
			{types.T_uint64, types.T_uint32, types.T_uint64},
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
	plusTemplate, err := os.ReadFile("mult.template")
	if err != nil {
		return err
	}
	tempText := string(plusTemplate)
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

		},
		[]lrt{ // high - low - high

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
	// read template file
	plusTemplate, err := os.ReadFile("div.template")
	if err != nil {
		return err
	}
	tempText := string(plusTemplate)
	tempText = rewriteTemplateText(tempText)

	temp, err := template.New("div").Parse(tempText)
	if err != nil {
		return err
	}

	type pts struct {
		SameType []lrt
		IntInt   []lrt
		HighLow  []lrt
		LowHigh	 []lrt
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
		[]lrt{

		},
		[]lrt{

		},
		[]lrt{

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
	plusTemplate, err := os.ReadFile("mod.template")
	if err != nil {
		return err
	}
	tempText := string(plusTemplate)
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
		[]lrt{

		},
		[]lrt{

		},
	}

	file, err := os.OpenFile("mod.go", os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	err = temp.ExecuteTemplate(file, "mod", pTs)
	file.Close()
	return err
}

func rewriteTemplateText(tempText string) string {
	tempText = strings.ReplaceAll(tempText, ".LEFT_TYPE_OID", ".{{.LTYP.OidString}}")
	tempText = strings.ReplaceAll(tempText, ".RIGHT_TYPE_OID", ".{{.RTYP.OidString}}")
	tempText = strings.ReplaceAll(tempText, ".RETURN_TYPE_OID", ".{{.RET.OidString}}")
	tempText = strings.ReplaceAll(tempText, "L_GO_TYPE", "{{.LTYP.GoType}}")
	tempText = strings.ReplaceAll(tempText, "R_GO_TYPE", "{{.RTYP.GoType}}")
	tempText = strings.ReplaceAll(tempText, "{.LTYP}", "{{.LTYP.GoGoType}}")
	tempText = strings.ReplaceAll(tempText, "{.RTYP}", "{{.RTYP.GoGoType}}")
	tempText = strings.ReplaceAll(tempText, "{.RETTYP}", "{{.RET.GoGoType}}")
	tempText = strings.ReplaceAll(tempText, "{.RETURN_TYPE_LEN}", "{{.RET.TypeLen}}")

	return tempText
}