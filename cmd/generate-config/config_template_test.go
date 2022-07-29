// Copyright 2022 Matrix Origin
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

package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"html/template"
	"log"
	"os"
	"sort"
	"testing"
	"unicode"
)

type par struct {
	Name string `toml:"Name"`
}
type Pars struct {
	Par []par
}

func TestToml(t *testing.T) {
	blob := `
		[[par]]
		Name = "Thunder Road"
		
		[[par]]
		Name = "Stairway to Heaven"
		`

	var favorites Pars
	if _, err := toml.Decode(blob, &favorites); err != nil {
		log.Fatal(err)
	}

	for _, s := range favorites.Par {
		fmt.Printf("%s\n", s.Name)
	}
}

func compareArray(A, B []string) bool {
	if len(A) != len(B) {
		return false
	}

	sort.Strings(A)
	sort.Strings(B)
	for i := 0; i < len(A); i++ {
		if A[i] != B[i] {
			return false
		}
	}
	return true
}

func TestParameter(t *testing.T) {
	blob := `
		[[parameter]]
		name = "autocommit"
		scope = ["global","session"]
		access = ["file"]
		type = "bool"
		domain-type = "set"
		values = ["true"]
		comment = "autocommit"
		update-mode = "dynamic"

		[[parameter]]
		name = "back-log"
		scope = ["global"]
		access = ["file"]
		type = "int64"
		domain-type = "range"
		values = ["-1","1","65535"]
		comment = "back-log"
		update-mode = "fix"
		`
	var results = map[string]parameter{
		"autocommit": {
			Name:       "autocommit",
			Scope:      []string{"global", "session"},
			Access:     []string{"file"},
			DataType:   "bool",
			DomainType: "set",
			Values:     []string{"true"},
			Comment:    "autocommit",
			UpdateMode: "dynamic",
		},
		"back-log": {
			Name:       "back-log",
			Scope:      []string{"global"},
			Access:     []string{"file"},
			DataType:   "int64",
			DomainType: "range",
			Values:     []string{"-1", "1", "65535"},
			Comment:    "back-log",
			UpdateMode: "fix",
		},
	}
	var paras parameters
	if metadata, err := toml.Decode(blob, &paras); err != nil {
		t.Errorf("error:%v \n", err)
	} else if undecoded := metadata.Undecoded(); len(undecoded) > 0 && err == nil {
		for _, item := range undecoded {
			t.Errorf("undecoded %s\n", item.String())
		}
	}

	for _, p := range paras.Parameter {
		par := results[p.Name]
		if p.Name != par.Name ||
			!compareArray(p.Scope, par.Scope) ||
			!compareArray(p.Access, par.Access) ||
			p.DataType != par.DataType ||
			p.DomainType != par.DomainType ||
			!compareArray(p.Values, par.Values) ||
			p.Comment != par.Comment ||
			p.UpdateMode != par.UpdateMode {
			t.Errorf("toml decode parameter failed.")
		}
	}
}

func Test_isAsciiChar(t *testing.T) {
	cases := [][]byte{
		{'a', 'j', 'z', 'A', 'J', 'Z', '_'},
		{'<', '=', '>', '+', '{', '[', '@', 0, ' ', '~'},
	}

	results := []bool{
		true,
		false,
	}

	for i := 0; i < len(results); i++ {
		for _, x := range cases[i] {
			r := isASCIIChar(x)
			if r != results[i] {
				t.Errorf("isAsciiChar failed. %d %c %v %v", i, x, r, results[i])
				return
			}
		}
	}
}

func Test_isAsciiDigit(t *testing.T) {
	cases := [][]byte{
		{'0', '1', '7', '8', '9'},
		{'<', '=', '>', '+', '{', '[', '@', 0, ' ', '~', '/', ':'},
	}

	results := []bool{
		true,
		false,
	}

	for i := 0; i < len(results); i++ {
		for _, x := range cases[i] {
			r := isASCIIDigit(x)
			if r != results[i] {
				t.Errorf("isAsciiDigit failed. %d %c %v %v", i, x, r, results[i])
				return
			}
		}
	}
}

func Test_isGoIdentifier(t *testing.T) {
	cases := [][]string{
		{"a", "j", "z", "_"},
		{"A", "J", "Z"},
		{"<", "=", ">", "+", "{", "[", "@", "0", " ", "~", ""},
		{"a0", "j0", "z0", "_0"},
		{"A0", "J0", "Z0"},
		{"a<", "j<", "z<", "_<"},
		{"A<", "J<", "Z<"},
		{"ap", "jp", "zp", "_p"},
		{"Ap", "Jp", "Zp"},
		{"pA", "pJ", "pZ"},
	}

	results := []bool{
		true,
		false,
		false,
		true,
		false,
		false,
		false,
		true,
		false,
		true,
	}

	for i := 0; i < len(results); i++ {
		for _, x := range cases[i] {
			r := isGoIdentifier(x)
			if r != results[i] {
				t.Errorf("isGoIdentifier failed. %d %s %v %v", i, x, r, results[i])
				return
			}
		}
	}
}

func Test_isGoStructAndInterfaceIdentifier(t *testing.T) {
	cases := [][]string{
		{"a", "j", "z", "_"},
		{"A", "J", "Z"},
		{"<", "=", ">", "+", "{", "[", "@", "0", " ", "~", ""},
		{"a0", "j0", "z0", "_0"},
		{"A0", "J0", "Z0"},
		{"a<", "j<", "z<", "_<"},
		{"A<", "J<", "Z<"},
		{"ap", "jp", "zp", "_p"},
		{"Ap", "Jp", "Zp"},
		{"pA", "pJ", "pZ"},
	}

	results := []bool{
		true,
		true,
		false,
		true,
		true,
		false,
		false,
		true,
		true,
		true,
	}

	for i := 0; i < len(results); i++ {
		for _, x := range cases[i] {
			r := isGoStructAndInterfaceIdentifier(x)
			if r != results[i] {
				t.Errorf("isGoIdentifier failed. %d %s %v %v", i, x, r, results[i])
				return
			}
		}
	}
}

func Test_isSubset(t *testing.T) {
	A := [][]string{
		{"a", "b", "c"},
		{},
		{"a", "a"},
		{"a", "e"},
		{"a", "b", "c", "e"},
		{"a", "b"},
		{"b", "c"},
		{"a", "c"},
		{"a"},
		{"b"},
		{"c"},
	}

	B := [][]string{
		{"a", "b", "c"},
		{"a", "b", "c"},
		{"a", "b", "c"},
		{"a", "b", "c"},
		{"a", "b", "c"},
		{"a", "b", "c"},
		{"a", "b", "c"},
		{"a", "b", "c"},
		{"a", "b", "c"},
		{"a", "b", "c"},
		{"a", "b", "c"},
	}

	results := []bool{
		true,
		false,
		false,
		false,
		false,
		true,
		true,
		true,
		true,
		true,
		true,
	}

	for i := 0; i < len(results); i++ {
		r := isSubset(A[i], B[i])
		if r != results[i] {
			t.Errorf("isSubset failed. %d %s %s %v %v", i, A[i], B[i], r, results[i])
			return
		}
	}
}

func Test_isScope(t *testing.T) {
	A := [][]string{
		{"session", "global", "c"},
		{},
		{"session", "session"},
		{"session", "e"},
		{"session", "global", "c", "e"},
		{"session", "global"},
		{"global", "c"},
		{"session", "c"},
		{"session"},
		{"global"},
		{"c"},
	}

	results := []bool{
		false,
		false,
		false,
		false,
		false,
		true,
		false,
		false,
		true,
		true,
		false,
	}

	for i := 0; i < len(results); i++ {
		r := isScope(A[i])
		if r != results[i] {
			t.Errorf("isScope failed. %d %s %v %v", i, A[i], r, results[i])
			return
		}
	}
}

func Test_isAccess(t *testing.T) {
	A := [][]string{
		{"cmd", "file", "c"},
		{},
		{"cmd", "cmd"},
		{"cmd", "e"},
		{"cmd", "file", "c", "e"},
		{"cmd", "file"},
		{"file", "c"},
		{"cmd", "c"},
		{"cmd"},
		{"file"},
		{"c"},
	}

	results := []bool{
		false,
		false,
		false,
		false,
		false,
		true,
		false,
		false,
		true,
		true,
		false,
	}

	for i := 0; i < len(results); i++ {
		r := isAccess(A[i])
		if r != results[i] {
			t.Errorf("isAccess failed. %d %s %v %v", i, A[i], r, results[i])
			return
		}
	}
}

func Test_isDataType(t *testing.T) {
	A := []string{
		"string", "int64", "float64", "bool",
		"A", "B", "C", "D",
	}

	results := []bool{
		true,
		true,
		true,
		true,
		false,
		false,
		false,
		false,
	}

	for i := 0; i < len(results); i++ {
		r := isDataType(A[i])
		if r != results[i] {
			t.Errorf("isDataType failed. %d %s %v %v", i, A[i], r, results[i])
			return
		}
	}
}

func Test_isDomainType(t *testing.T) {
	A := []string{
		"set", "range",
		"string", "int64", "float64", "bool",
		"A", "B", "C", "D",
	}

	results := []bool{
		true,
		true,
		false,
		false,
		false,
		false,
		false,
		false,
		false,
		false,
	}

	for i := 0; i < len(results); i++ {
		r := isDomainType(A[i])
		if r != results[i] {
			t.Errorf("isDataType failed. %d %s %v %v", i, A[i], r, results[i])
			return
		}
	}
}

type valueCase struct {
	dataType   string
	domainType string
	values     []string
}

func Test_checkValues(t *testing.T) {
	A := []valueCase{
		//string
		{"string", "set", []string{}},
		{"string", "set", []string{"A"}},
		{"string", "set", []string{"A", "A"}},
		{"string", "set", []string{"A", "A", "B", "C"}},
		{"string", "set", []string{"A", "B", "C"}},

		{"string", "range", []string{}},
		{"string", "range", []string{"A"}},
		{"string", "range", []string{"A", "A"}},
		{"string", "range", []string{"A", "A", "B", "C"}},
		{"string", "range", []string{"A", "B", "C"}},

		//int64
		{"int64", "set", []string{}},
		{"int64", "set", []string{"A"}},
		{"int64", "set", []string{"A", "A"}},
		{"int64", "set", []string{"A", "A", "B", "C"}},
		{"int64", "set", []string{"A", "B", "C"}},

		{"int64", "set", []string{"0"}},
		{"int64", "set", []string{"0", "0"}},
		{"int64", "set", []string{"0", "0", "1", "C"}},
		{"int64", "set", []string{"0", "1", "2"}},
		{"int64", "set", []string{"0", "1", "2", "0"}},

		{"int64", "range", []string{"0"}},
		{"int64", "range", []string{"0", "1"}},
		{"int64", "range", []string{"0", "1", "2"}},
		{"int64", "range", []string{"1", "0", "2"}},
		{"int64", "range", []string{"3", "1", "2"}},
		{"int64", "range", []string{"5", "1", "9"}},

		//float64
		{"float64", "set", []string{}},
		{"float64", "set", []string{"A"}},
		{"float64", "set", []string{"A", "A"}},
		{"float64", "set", []string{"A", "A", "B", "C"}},
		{"float64", "set", []string{"A", "B", "C"}},

		{"float64", "set", []string{"0.1"}},
		{"float64", "set", []string{"0.1", "0.1"}},
		{"float64", "set", []string{"0", "0", "1", "C"}},
		{"float64", "set", []string{"0", "1", "2"}},
		{"float64", "set", []string{"0", "1", "2", "0"}},

		{"float64", "range", []string{"0"}},
		{"float64", "range", []string{"0", "1"}},
		{"float64", "range", []string{"0", "1", "2"}},
		{"float64", "range", []string{"1", "0", "2"}},
		{"float64", "range", []string{"3", "1", "2"}},
		{"float64", "range", []string{"5", "1", "9"}},

		//bool
		{"bool", "set", []string{}},
		{"bool", "set", []string{"A"}},
		{"bool", "set", []string{"A", "A"}},
		{"bool", "set", []string{"A", "A", "B", "C"}},
		{"bool", "set", []string{"A", "B", "C"}},

		{"bool", "set", []string{"0.1"}},
		{"bool", "set", []string{"false"}},
		{"bool", "set", []string{"off"}},
		{"bool", "set", []string{"true"}},
		{"bool", "set", []string{"on"}},

		{"bool", "range", []string{"0", "1", "2", "0"}},
		{"bool", "set", []string{"FALSE"}},
		{"bool", "set", []string{"OFF"}},
		{"bool", "set", []string{"TRUE"}},
		{"bool", "set", []string{"ON"}},

		//x
		{"x", "set", []string{"FALSE"}},
	}

	results := []bool{
		//string
		true,
		true,
		false,
		false,
		true,

		false,
		false,
		false,
		false,
		false,

		//int64
		true,
		false,
		false,
		false,
		false,

		true,
		false,
		false,
		true,
		false,

		false,
		false,
		false,
		true,
		false,
		true,

		//float64
		true,
		false,
		false,
		false,
		false,

		true,
		false,
		false,
		true,
		false,

		false,
		false,
		false,
		true,
		false,
		true,

		//bool
		true,
		false,
		false,
		false,
		false,

		false,
		true,
		true,
		true,
		true,

		false,
		true,
		true,
		true,
		true,

		//x
		false,
	}

	for i := 0; i < len(results); i++ {
		r := checkValues(A[i].dataType, A[i].domainType, A[i].values)
		if r != results[i] {
			t.Errorf("checkValues failed. %d %s %v %v", i, A[i], r, results[i])
			return
		}
	}
}

func Test_hasDuplicateValue(t *testing.T) {
	type args struct {
		x []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"t1", args{[]string{"A", "B", "A"}}, true},
		{"t2", args{[]string{"A", "B", "C"}}, false},
		{"t3", args{[]string{"A"}}, false},
		{"t4", args{[]string{"A", "A"}}, true},
		{"t5", args{[]string{}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasDuplicateValueString(tt.args.x); got != tt.want {
				t.Errorf("hasDuplicateValueString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hasDuplicateValueInt64(t *testing.T) {
	type args struct {
		x []int64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"t1", args{[]int64{0, 1, 0}}, true},
		{"t2", args{[]int64{0, 1, 2}}, false},
		{"t3", args{[]int64{0}}, false},
		{"t4", args{[]int64{0, 0}}, true},
		{"t5", args{[]int64{}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasDuplicateValueInt64(tt.args.x); got != tt.want {
				t.Errorf("hasDuplicateValueInt64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_hasDuplicateValueFloat64(t *testing.T) {
	type args struct {
		x []float64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"t1", args{[]float64{0, 1, 0}}, true},
		{"t2", args{[]float64{0, 1, 2}}, false},
		{"t3", args{[]float64{0}}, false},
		{"t4", args{[]float64{0, 0}}, true},
		{"t5", args{[]float64{}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasDuplicateValueFloat64(tt.args.x); got != tt.want {
				t.Errorf("hasDuplicateValueFloat64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parameters_LoadParametersDefinition(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"t1", args{"test/t1.toml"}, false},
		{"t2", args{"test/t2.toml"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := &parameters{}
			if err := params.LoadParametersDefinitionFromFile(tt.args.filename); (err != nil) != tt.wantErr {
				t.Errorf("LoadParametersDefinitionFromFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_parameters_LoadParametersDefinitionFromString(t *testing.T) {
	t1 := `
		parameter-struct-name = "AllParameters"
		config-struct-name = "configuration"
		operation-file-name = "parameters"
		config-file-name = "config"

		[[parameter]]
		name = "autocommit"
		scope = ["global","session"]
		access = ["file"]
		type = "bool"
		domain-type = "set"
		values = ["true"]
		comment = "autocommit"
		update-mode = "dynamic"`

	t2 := `
parameter-struct-name = "AllParameters"
config-struct-name = "configuration"
operation-file-name = "parameters"
config-file-name = "config"

		[[parameter]]
		name = "autocommit@"
		scope = ["global","session"]
		access = ["file"]
		type = "bool"
		domain-type = "set"
		values = ["true"]
		comment = "autocommit"
		update-mode = "dynamic"`

	t3 := `
parameter-struct-name = "AllParameters"
config-struct-name = "configuration"
operation-file-name = "parameters"
config-file-name = "config"

		[[parameter]]
		name = "autocommit"
		scope = ["global","sesxion"]
		access = ["file"]
		type = "bool"
		domain-type = "set"
		values = ["true"]
		comment = "autocommit"
		update-mode = "dynamic"`

	t4 := `
parameter-struct-name = "AllParameters"
config-struct-name = "configuration"
operation-file-name = "parameters"
config-file-name = "config"

		[[parameter]]
		name = "autocommit"
		scope = ["global","session"]
		access = ["x"]
		type = "bool"
		domain-type = "set"
		values = ["true"]
		comment = "autocommit"
		update-mode = "dynamic"`

	t5 := `
parameter-struct-name = "AllParameters"
config-struct-name = "configuration"
operation-file-name = "parameters"
config-file-name = "config"

		[[parameter]]
		name = "autocommit"
		scope = ["global","session"]
		access = ["file"]
		type = "bl"
		domain-type = "set"
		values = ["true"]
		comment = "autocommit"
		update-mode = "dynamic"`

	t6 := `
parameter-struct-name = "AllParameters"
config-struct-name = "configuration"
operation-file-name = "parameters"
config-file-name = "config"

		[[parameter]]
		name = "autocommit"
		scope = ["global","session"]
		access = ["file"]
		type = "int64"
		domain-type = "set"
		values = ["1"]
		comment = "autocommit"
		update-mode = "dynamic"`

	t7 := `
parameter-struct-name = "AllParameters"
config-struct-name = "configuration"
operation-file-name = "parameters"
config-file-name = "config"

		[[parameter]]
		name = "count"
		scope = ["global","session"]
		access = ["file"]
		type = "int64"
		domain-type = "range"
		values = ["1","0","65535"]
		comment = "count"
		update-mode = "fix"`

	t8 := `
parameter-struct-name = "AllParameters"
config-struct-name = "configuration"
operation-file-name = "parameters"
config-file-name = "config"

		[[parameter]]
		name = "count"
		scope = ["global","session"]
		access = ["file"]
		type = "int64"
		domain-type = "range"
		values = ["1","0","65535"]
		comment = "count"
		update-mode = "fix"
		
		[[parameter]]
		name = "count"
		scope = ["global","session"]
		access = ["file"]
		type = "int64"
		domain-type = "range"
		values = ["1","0","65535"]
		comment = "count"
		update-mode = "fix"
`

	t9 := `
parameter-struct-name = "AllParameters"
config-struct-name = "configuration"
operation-file-name = "parameters"
config-file-name = "config"

		[[parameter]]
		name = "boolset1"
		scope = ["global","session"]
		access = ["file"]
		type = "bool"
		domain-type = "set"
		values = []
		comment = "boolset1"
		update-mode = "dynamic"
`

	t10 := `
parameter-struct-name = "AllParameters"
config-struct-name = "configuration"
operation-file-name = "parameters"
config-file-name = "config"

		[[parameter]]
		name = "int64set1"
		scope = ["global","session"]
		access = ["file"]
		type = "int64"
		domain-type = "set"
		values = []
		comment = "int64set1"
		update-mode = "dynamic"
`

	t11 := `
parameter-struct-name = "AllParameters"
config-struct-name = "configuration"
operation-file-name = "parameters"
config-file-name = "config"

		[[parameter]]
		name = "float64set1"
		scope = ["global","session"]
		access = ["file"]
		type = "float64"
		domain-type = "set"
		values = []
		comment = "float64set1"
		update-mode = "dynamic"
`

	t12 := `
parameter-struct-name = "AllParameters"
config-struct-name = "configuration"
operation-file-name = "parameters"
config-file-name = "config"

		[[parameter]]
		name = "stringset1"
		scope = ["global","session"]
		access = ["file"]
		type = "string"
		domain-type = "set"
		values = []
		comment = "stringset1"
		update-mode = "dynamic"`

	type args struct {
		input string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"t1", args{t1}, false},
		{"t2", args{t2}, true},
		{"t3", args{t3}, true},
		{"t4", args{t4}, true},
		{"t5", args{t5}, true},
		{"t6", args{t6}, false},
		{"t7", args{t7}, false},
		{"t8", args{t8}, true},
		{"t9", args{t9}, false},
		{"t10", args{t10}, false},
		{"t11", args{t11}, false},
		{"t12", args{t12}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := &parameters{}
			if err := params.LoadParametersDefinitionFromString(tt.args.input); (err != nil) != tt.wantErr {
				t.Errorf("LoadParametersDefinitionFromString() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

var tmplStr = `
// Code generated by tool; DO NOT EDIT.
package config

import (
	"fmt"
	"sync"
	"github.com/BurntSushi/toml"
)

//all parameters in the system
type {{.ParameterStructName}} struct{
	//read and write lock
	rwlock	sync.RWMutex
{{range .Parameter}}
	{{ printf "/**\n\tName:\t%s\n\tScope:\t%s\n\tAccess:\t%s\n\tDataType:\t%s\n\tDomainType:\t%s\n\tValues:\t%s\n\tComment:\t%s\n\tUpdateMode:\t%s\n\t*/"  
			.Name .Scope .Access .DataType .DomainType .Values .Comment .UpdateMode
	}}
	{{ printf "%s    %s" .Name .DataType }}
{{end}}

	//parameter name -> parameter definition string
	name2definition map[string]string
}//end {{.ParameterStructName}}

//all parameters can be set in the configuration file.
type {{.ConfigurationStructName}} struct{
	//read and write lock
	rwlock	sync.RWMutex

{{range .Parameter}}
	{{ if ne .UpdateMode "fix"}}
	{{ printf "/**\n\tName:\t%s\n\tScope:\t%s\n\tAccess:\t%s\n\tDataType:\t%s\n\tDomainType:\t%s\n\tValues:\t%s\n\tComment:\t%s\n\tUpdateMode:\t%s\n\t*/"  
			.Name .Scope .Access .DataType .DomainType .Values .Comment .UpdateMode
	}}
	{{ printf "%s    %s  ` + "`toml:" + `\"%s\"` + "`" + `" .CapitalName .DataType .Name }}

	{{end}}
{{end}}

	//parameter name -> updated flag
	name2updatedFlags map[string]bool
}//end {{.ConfigurationStructName}}

/**
prepare something before anything else.
it is unsafe in multi-thread environment.
*/
func (ap *{{.ParameterStructName}}) prepareAnything(){
	if ap.name2definition == nil {
		ap.name2definition = make(map[string]string)
	}
}

/**
set parameter and its string of the definition.
*/
func (ap *{{.ParameterStructName}}) PrepareDefinition(){
	ap.rwlock.Lock()
	defer ap.rwlock.Unlock()

	ap.prepareAnything()
	{{range .Parameter}}
	{{ printf "ap.name2definition[\"%s\"] = \"\tName:\t%s\tScope:\t%s\tAccess:\t%s\tDataType:\t%s\tDomainType:\t%s\tValues:\t%s\tComment:\t%s\tUpdateMode:\t%s\t\"" 
			.Name .Name .Scope .Access .DataType .DomainType .Values .Comment .UpdateMode
	}}
	{{end}}
}

/**
get the definition of the parameter.
*/
func (ap *{{.ParameterStructName}}) GetDefinition(name string)(string,error){
	ap.rwlock.RLock()
	defer ap.rwlock.RUnlock()
	ap.prepareAnything()
	if p,ok := ap.name2definition[name];!ok{
		return "",fmt.Errorf("there is no parameter %s",name)
	}else{
		return p,nil
	}
}

/**
check if there is the parameter
*/
func (ap *{{.ParameterStructName}}) HasParameter(name string)bool{
	ap.rwlock.RLock()
	defer ap.rwlock.RUnlock()
	ap.prepareAnything()
	if _,ok := ap.name2definition[name];!ok{
		return false
	}else{
		return true
	}
}

/**
Load the initial values of all parameters.
*/
func (ap *{{.ParameterStructName}}) LoadInitialValues()error{
	ap.PrepareDefinition()
	var err error
	{{range .Parameter}}
		
		{{if eq .DataType "string"}}
			{{.Name}}choices :=[]{{.DataType}} {
				{{range .Values}}
				{{printf "\"%s\"" .}},
				{{end}}	
			}
			if len({{.Name}}choices) != 0{
				if err = ap.set{{.CapitalName}}( {{.Name}}choices[0] ) ; err != nil{
					return fmt.Errorf("set%s failed.error:%v",{{printf "\"%s\"" .CapitalName}},err)
				}
			}else{
				//empty string
				if err = ap.set{{.CapitalName}}( "" ) ; err != nil{
					return fmt.Errorf("set%s failed.error:%v",{{printf "\"%s\"" .CapitalName}},err)
				}
			}
		{{else}}
			{{.Name}}choices :=[]{{.DataType}} {
				{{range .Values}}
				{{printf "%s" .}},
				{{end}}	
			}
			if len({{.Name}}choices) != 0{
				if err = ap.set{{.CapitalName}}( {{.Name}}choices[0] ) ; err != nil{
					return fmt.Errorf("set%s failed.error:%v",{{printf "\"%s\"" .CapitalName}},err)
				}
			}else{
				{{if eq .DataType "bool"}}
					if err = ap.set{{.CapitalName}}( false ) ; err != nil{
						return fmt.Errorf("set%s failed.error:%v",{{printf "\"%s\"" .CapitalName}},err)
					}	
				{{else if  eq .DataType "int64"}}
					if err = ap.set{{.CapitalName}}( 0 ) ; err != nil{
						return fmt.Errorf("set%s failed.error:%v",{{printf "\"%s\"" .CapitalName}},err)
					}
				{{else if eq .DataType "float64"}}
					if err = ap.set{{.CapitalName}}( 0.0 ) ; err != nil{
						return fmt.Errorf("set%s failed.error:%v",{{printf "\"%s\"" .CapitalName}},err)
					}
				{{end}}
			}
		{{end}}
	{{end}}
	return nil
}

{{with $Params := .}}
{{range $Params.Parameter}}
/**
Get the value of the parameter {{.Name}}
*/
func (ap * {{$Params.ParameterStructName}} ) Get{{.CapitalName}}() {{.DataType}} {
	ap.rwlock.RLock()
	defer ap.rwlock.RUnlock()
	return ap.{{.Name}}
}
{{end}}
{{end}}


{{with $Params := .}}
{{range $Params.Parameter}}
{{ if ne .UpdateMode "fix"}}
/**
Set the value of the parameter {{.Name}}
*/
func (ap * {{$Params.ParameterStructName}} ) Set{{.CapitalName}}(value {{.DataType}})error {
	return  ap.set{{.CapitalName}}(value)
}
{{end}}
{{end}}
{{end}}

{{with $Params := .}}
{{range $Params.Parameter}}
/**
Set the value of the parameter {{.Name}}
*/
func (ap * {{$Params.ParameterStructName}} ) set{{.CapitalName}}(value {{.DataType}})error {
	ap.rwlock.Lock()
	defer ap.rwlock.Unlock()

	{{if eq .DataType "bool"}}
		
		{{if eq .DomainType "set"}}
			choices :=[]{{.DataType}} {
				{{range .Values}}
				{{printf "%s" .}},
				{{end}}	
			}
			if len( choices ) != 0{
				if !isInSliceBool(value, choices){
					return fmt.Errorf("set{{.CapitalName}},the value %t is not in set %v",value,choices)
				}
			}//else means any bool value: true or false
		{{else}}
			return fmt.Errorf("the bool type does not support domainType %s",{{printf "\"%s\"" .DomainType}})
		{{end}}

	{{else if eq .DataType "string"}}

		{{if eq .DomainType "set"}}
			choices :=[]{{.DataType}} {
				{{range .Values}}
				{{printf "\"%s\"" .}},
				{{end}}	
			}
			if len( choices ) != 0{
				if !isInSlice(value, choices){
					return fmt.Errorf("set{{.CapitalName}},the value %s is not in set %v",value,choices)
				}
			}//else means any string
		{{else}}
			return fmt.Errorf("the string type does not support domainType %s",{{printf "\"%s\"" .DomainType}})
		{{end}}

	{{else if eq .DataType "int64"}}

		{{if eq .DomainType "set"}}
			choices :=[]{{.DataType}} {
				{{range .Values}}
				{{printf "%s" .}},
				{{end}}	
			}
			if len( choices ) != 0{
				if !isInSliceInt64(value, choices){
					return fmt.Errorf("set{{.CapitalName}},the value %d is not in set %v",value,choices)
				}
			}//else means any int64
		{{else if eq .DomainType "range"}}
			choices :=[]{{.DataType}} {
				{{range .Values}}
				{{printf "%s" .}},
				{{end}}	
			}
			if !(value >= choices[1] && value <= choices[2]){
				return fmt.Errorf("set{{.CapitalName}},the value %d is not in the range [%d,%d]",value,choices[1],choices[2])
			}
		{{else}}
			return fmt.Errorf("the int64 type does not support domainType %s",{{printf "\"%s\"" .DomainType}})
		{{end}}

	{{else if eq .DataType "float64"}}

		{{if eq .DomainType "set"}}
			choices :=[]{{.DataType}} {
				{{range .Values}}
				{{printf "%s" .}},
				{{end}}	
			}
			if len( choices ) != 0{
				if !isInSliceFloat64(value, choices){
					return fmt.Errorf("set{{.CapitalName}},the value %f is not in set %v",value,choices)
				}
			}//else means any float64
		{{else if eq .DomainType "range"}}
			choices :=[]{{.DataType}} {
				{{range .Values}}
				{{printf "%s" .}},
				{{end}}	
			}
			if !(value >= choices[1] && value <= choices[2]){
				return fmt.Errorf("set{{.CapitalName}},the value %f is not in the range [%f,%f]",value,choices[1],choices[2])
			}
		{{else}}
			return fmt.Errorf("the float64 type does not support domainType %s",{{printf "\"%s\"" .DomainType}})
		{{end}}
	{{end}}

	ap.{{.Name}} = value
	return nil
}
{{end}}
{{end}}

/**
prepare something before anything else.
it is unsafe in multi-thread environment.
*/
func (config *{{.ConfigurationStructName}}) prepareAnything(){
	if config.name2updatedFlags == nil {
		config.name2updatedFlags = make(map[string]bool)
	}
}

/**
reset update flags of configuration items
*/
func (config *{{.ConfigurationStructName}}) resetUpdatedFlags(){
	config.rwlock.Lock()
	defer config.rwlock.Unlock()
	config.prepareAnything()
	{{range .Parameter}}
	{{ if ne .UpdateMode "fix"}}
		{{ printf "config.name2updatedFlags[\"%s\"] = false" .Name}}
	{{end}}
	{{end}}
}

/**
set update flag of configuration item
*/
func (config *{{.ConfigurationStructName}}) setUpdatedFlag(name string,updated bool){
	config.rwlock.Lock()
	defer config.rwlock.Unlock()
	config.prepareAnything()
	config.name2updatedFlags[name] = updated
}

/**
get update flag of configuration item
*/
func (config *{{.ConfigurationStructName}}) getUpdatedFlag(name string)bool{
	config.rwlock.RLock()
	defer config.rwlock.RUnlock()
	config.prepareAnything()
	return config.name2updatedFlags[name]
}

/**
Load parameters' values in the configuration string.
*/
func (config *{{.ConfigurationStructName}}) LoadConfigurationFromString(input string) error {
	config.resetUpdatedFlags()

	metadata, err := toml.Decode(input, config);
	if err != nil {
		return err
	}else if failed := metadata.Undecoded() ; len(failed) > 0 {
		var failedItems []string
		for _, item := range failed {
			failedItems = append(failedItems, item.String())
		}
		return fmt.Errorf("decode failed %s. error:%v",failedItems,err)
	}

	for _,k := range metadata.Keys(){
		config.setUpdatedFlag(k[0],true)
	}

	return nil
}

/**
Load parameters' values in the configuration file.
*/
func (config *{{.ConfigurationStructName}}) LoadConfigurationFromFile(fname string) error {
	config.resetUpdatedFlags()

	metadata, err := toml.DecodeFile(fname, config);
	if err != nil {
		return err
	}else if failed := metadata.Undecoded() ; len(failed) > 0 {
		var failedItems []string
		for _, item := range failed {
			failedItems = append(failedItems, item.String())
		}
		return fmt.Errorf("decode failed %s. error:%v",failedItems,err)
	}

	for _,k := range metadata.Keys(){
		config.setUpdatedFlag(k[0],true)
	}

	return nil
}

/**
Update parameters' values with configuration.
*/
func (ap * {{.ParameterStructName}} ) UpdateParametersWithConfiguration(config *{{.ConfigurationStructName}})error{
	var err error
	{{range .Parameter}}
	{{ if ne .UpdateMode "fix"}}
	if config.getUpdatedFlag("{{.Name}}"){
		if err = ap.set{{.CapitalName}}(config.{{.CapitalName}}); err != nil{
			return fmt.Errorf("update parameter {{.Name}} failed.error:%v",err)
		}
	}
	{{end}}
	{{end}}
	return nil
}
`

func Test_template(t *testing.T) {
	var paras = parameters{
		ParameterStructName:     "AllParameters",
		ConfigurationStructName: "configuration",
		Parameter: []parameter{
			{
				Name:       "boolSet1",
				Scope:      []string{"global", "session"},
				Access:     []string{"file"},
				DataType:   "bool",
				DomainType: "set",
				Values:     []string{"true"},
				Comment:    "boolSet1",
				UpdateMode: "dynamic",
			},
			{
				Name:       "boolRange1",
				Scope:      []string{"global", "session"},
				Access:     []string{"file"},
				DataType:   "bool",
				DomainType: "range",
				Values:     []string{"true"},
				Comment:    "boolRange1",
				UpdateMode: "dynamic",
			},
			{
				Name:       "stringSet1",
				Scope:      []string{"global"},
				Access:     []string{"file"},
				DataType:   "string",
				DomainType: "set",
				Values:     []string{"localhost", "127.0.0.1"},
				Comment:    "stringSet1",
				UpdateMode: "dynamic",
			},
			{
				Name:       "stringRange1",
				Scope:      []string{"global"},
				Access:     []string{"file"},
				DataType:   "string",
				DomainType: "range",
				Values:     []string{"localhost", "127.0.0.1"},
				Comment:    "stringRange1",
				UpdateMode: "dynamic",
			},
			{
				Name:       "int64set1",
				Scope:      []string{"global"},
				Access:     []string{"file"},
				DataType:   "int64",
				DomainType: "set",
				Values:     []string{"-1", "2", "65535"},
				Comment:    "int64set1",
				UpdateMode: "dynamic",
			},
			{
				Name:       "int64range1",
				Scope:      []string{"global"},
				Access:     []string{"file"},
				DataType:   "int64",
				DomainType: "range",
				Values:     []string{"-1", "-1", "65535"},
				Comment:    "int64range1",
				UpdateMode: "dynamic",
			},
			{
				Name:       "int64X1",
				Scope:      []string{"global"},
				Access:     []string{"file"},
				DataType:   "int64",
				DomainType: "X",
				Values:     []string{"-1", "-1", "65535"},
				Comment:    "int64X1",
				UpdateMode: "dynamic",
			},
			{
				Name:       "float64set1",
				Scope:      []string{"global"},
				Access:     []string{"file"},
				DataType:   "float64",
				DomainType: "set",
				Values:     []string{"-1.01", "2.02", "65535.03"},
				Comment:    "float64set1",
				UpdateMode: "dynamic",
			},
			{
				Name:       "float64range1",
				Scope:      []string{"global"},
				Access:     []string{"file"},
				DataType:   "float64",
				DomainType: "range",
				Values:     []string{"-2.02", "-1.01", "65535.03"},
				Comment:    "float64range1",
				UpdateMode: "dynamic",
			},
			{
				Name:       "float64X1",
				Scope:      []string{"global"},
				Access:     []string{"file"},
				DataType:   "float64",
				DomainType: "X",
				Values:     []string{"-1", "-1", "65535"},
				Comment:    "float64X1",
				UpdateMode: "dynamic",
			},
			{
				Name:       "float64set2",
				Scope:      []string{"global"},
				Access:     []string{"file"},
				DataType:   "float64",
				DomainType: "set",
				Values:     []string{"-1", "-1", "65535"},
				Comment:    "float64set2",
				UpdateMode: "fix",
			},
			{
				Name:       "boolSet2",
				Scope:      []string{"global", "session"},
				Access:     []string{"file"},
				DataType:   "bool",
				DomainType: "set",
				Values:     []string{"true"},
				Comment:    "boolSet2",
				UpdateMode: "dynamic",
			},
			{
				Name:       "boolSet3",
				Scope:      []string{"global", "session"},
				Access:     []string{"file"},
				DataType:   "bool",
				DomainType: "set",
				Values:     []string{},
				Comment:    "boolSet3",
				UpdateMode: "dynamic",
			},
		},
	}

	for i := 0; i < len(paras.Parameter); i++ {
		p := paras.Parameter[i].Name
		capName := string(unicode.ToUpper(rune(p[0]))) + p[1:]
		paras.Parameter[i].CapitalName = capName
	}

	tmpl, err := template.New("test").Parse(tmplStr)
	if err != nil {
		panic(err)
	}

	f, err := os.Create("parameters.go")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	err = tmpl.Execute(f, paras)
	if err != nil {
		panic(err)
	}
}

var tomlTmplString = `
# Code generated by tool; DO NOT EDIT.
{{range $index,$param := .Parameter}}
{{ if ne .UpdateMode "fix"}}
	{{ printf "\n#\tName:\t%s\n#\tScope:\t%s\n#\tAccess:\t%s\n#\tDataType:\t%s\n#\tDomainType:\t%s\n#\tValues:\t%s\n#\tComment:\t%s\n#\tUpdateMode:\t%s\n\t"  
			.Name .Scope .Access .DataType .DomainType .Values .Comment .UpdateMode
	}}

	{{- with $count := len .Values -}}
		{{- if ne 0 $count -}}
			{{- if eq $param.DataType "string" -}}
				{{- $param.Name -}} = "{{- index $param.Values 0 -}}"
			{{- else -}}
				{{- $param.Name -}} = {{- index $param.Values 0 -}}
			{{- end -}}
		{{- end -}}
	{{- else -}}
		{{- if eq $param.DataType "string" -}}
			{{- $param.Name -}} = ""
		{{- else if eq $param.DataType "bool" -}}
			{{- $param.Name -}} = false
		{{- else if eq $param.DataType "int64" -}}
			{{- $param.Name -}} = 0
		{{- else if eq $param.DataType "float64" -}}
			{{- $param.Name -}} = 0.0
		{{- end -}}
	{{- end -}}
{{end}}
{{end}}
`

var testOperationTmpl = `
// Code generated by tool; DO NOT EDIT.
package config

import (
	"sync"
	"testing"
)

func Test{{.ParameterStructName}}_LoadInitialValues(t *testing.T) {
	ap := &{{.ParameterStructName}}{}
	if err :=ap.LoadInitialValues(); err!=nil{
		t.Errorf("LoadInitialValues failed. error:%v",err)
	}
}

func is{{.ConfigurationStructName}}Equal(c1,c2 {{.ConfigurationStructName}}) bool {

{{range .Parameter}}
{{ if ne .UpdateMode "fix"}}
	if c1.{{.CapitalName}} != c2.{{.CapitalName}} {
		return false
	}
{{end}}
{{end}}

	return true
}

func Test_{{.ConfigurationStructName}}_LoadConfigurationFromString(t *testing.T) {
	t1 := ` + "`" + `
{{range $index,$param := .Parameter}}
{{ if ne .UpdateMode "fix"}}
	{{- with $count := len .Values -}}
		{{- if ne 0 $count -}}
			{{- if eq $param.DataType "string" -}}
				{{- $param.Name -}} = "{{- index $param.Values 0 -}}"
			{{- else -}}
				{{- $param.Name -}} = {{- index $param.Values 0 -}}
			{{- end -}}
		{{- end -}}
	{{- else -}}
		{{- if eq $param.DataType "string" -}}
			{{- $param.Name -}} = ""
		{{- else if eq $param.DataType "bool" -}}
			{{- $param.Name -}} = false
		{{- else if eq $param.DataType "int64" -}}
			{{- $param.Name -}} = 0
		{{- else if eq $param.DataType "float64" -}}
			{{- $param.Name -}} = 0.0
		{{- end -}}
	{{- end -}}
{{end}}
{{end}}		
` + "`" + `
	t1_config:={{.ConfigurationStructName}}{
		rwlock:            sync.RWMutex{},

{{range $index,$param := .Parameter}}
{{ if ne .UpdateMode "fix"}}
	{{- with $count := len .Values -}}
		{{- if ne 0 $count -}}
			{{- if eq $param.DataType "string" -}}
				{{- $param.CapitalName -}} : "{{- index $param.Values 0 -}}" ,
			{{- else -}}
				{{- $param.CapitalName -}} : {{- index $param.Values 0 -}} ,
			{{- end -}}
		{{- end -}}
	{{- else -}}
		{{- if eq $param.DataType "string" -}}
			{{- $param.CapitalName -}} : "" ,
		{{- else if eq $param.DataType "bool" -}}
			{{- $param.CapitalName -}} : false ,
		{{- else if eq $param.DataType "int64" -}}
			{{- $param.CapitalName -}} : 0 ,
		{{- else if eq $param.DataType "float64" -}}
			{{- $param.CapitalName -}} : 0.0 ,
		{{- end -}}
	{{- end -}}
{{end}}
{{end}}	

		name2updatedFlags: nil,
	}

	type args struct {
		input string
		config {{.ConfigurationStructName}}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		wantErr2 bool
		wantErr3 bool
	}{
		{"t1",args{t1,t1_config},false,false,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ap := &{{.ParameterStructName}}{}
			if err := ap.LoadInitialValues(); err != nil{
				t.Errorf("LoadInitialValues failed.error %v",err)
			}
			config := &{{.ConfigurationStructName}}{}
			if err := config.LoadConfigurationFromString(tt.args.input); (err != nil) != tt.wantErr {
				t.Errorf("LoadConfigurationFromString() error = %v, wantErr %v", err, tt.wantErr)
			}else if err != nil{
				return
			}

			if err := ap.UpdateParametersWithConfiguration(config); (err != nil) != tt.wantErr2{
				t.Errorf("UpdateParametersWithConfiguration failed. error:%v",err)
			}

			if ( is{{.ConfigurationStructName}}Equal(*config,tt.args.config) != true ) != tt.wantErr3{
				t.Errorf("Configuration are not equal. %v vs %v ",*config,tt.args.config)
				return
			}
		})
	}
}
`

func Test_ParameterDefinitionAndTemplate2(t *testing.T) {
	t1 := `
		parameter-struct-name = "AllParameters"
		config-struct-name = "configuration"
		operation-file-name = "parameters"
		config-file-name = "config"

		[[parameter]]
		name = "boolSet1"
		scope = ["global"]
		access = ["file"]
		type = "bool"
		domain-type = "set"
		values = ["true"]
		comment = "boolSet1"
		update-mode = "dynamic"

		[[parameter]]
		name = "boolSet2"
		scope = ["global"]
		access = ["file"]
		type = "bool"
		domain-type = "set"
		values = ["false"]
		comment = "boolSet2"
		update-mode = "hotload"

		[[parameter]]
		name = "boolSet3"
		scope = ["global"]
		access = ["file"]
		type = "bool"
		domain-type = "set"
		values = []
		comment = "boolSet3"
		update-mode = "dynamic"

		[[parameter]]
		name = "stringSet1"
		scope = ["global"]
		access = ["file"]
		type = "string"
		domain-type = "set"
		values = ["ss1","ss2","ss3"]
		comment = "stringSet1"
		update-mode = "dynamic"

		[[parameter]]
		name = "stringSet2"
		scope = ["global"]
		access = ["file"]
		type = "string"
		domain-type = "set"
		values = []
		comment = "stringSet2"
		update-mode = "dynamic"

		[[parameter]]
		name = "int64set1"
		scope = ["global"]
		access = ["file"]
		type = "int64"
		domain-type = "set"
		values = ["1","2","3","4","5","6"]
		comment = "int64Set1"
		update-mode = "dynamic"

		[[parameter]]
		name = "int64set2"
		scope = ["global"]
		access = ["file"]
		type = "int64"
		domain-type = "set"
		values = ["1","3","5","7"]
		comment = "int64Set2"
		update-mode = "fix"

		[[parameter]]
		name = "int64set3"
		scope = ["global"]
		access = ["file"]
		type = "int64"
		domain-type = "set"
		values = []
		comment = "int64Set3"
		update-mode = "dynamic"

		[[parameter]]
		name = "int64Range1"
		scope = ["global"]
		access = ["file"]
		type = "int64"
		domain-type = "range"
		values = ["1000","0","10000"]
		comment = "int64Range1"
		update-mode = "dynamic"

		[[parameter]]
		name = "float64set1"
		scope = ["global"]
		access = ["file"]
		type = "float64"
		domain-type = "set"
		values = ["1.0","2.","3.","4.00","5","6"]
		comment = "float64Set1"
		update-mode = "dynamic"

		[[parameter]]
		name = "float64set2"
		scope = ["global"]
		access = ["file"]
		type = "float64"
		domain-type = "set"
		values = ["1.001","3.003","5.005","7.007"]
		comment = "float64Set2"
		update-mode = "fix"

		[[parameter]]
		name = "float64set3"
		scope = ["global"]
		access = ["file"]
		type = "float64"
		domain-type = "set"
		values = []
		comment = "float64Set3"
		update-mode = "dynamic"

		[[parameter]]
		name = "float64Range1"
		scope = ["global"]
		access = ["file"]
		type = "float64"
		domain-type = "range"
		values = ["1000.01","0.02","10000.03"]
		comment = "float64Range1"
		update-mode = "dynamic"
`

	t2 := `
		parameter-struct-name = "AllParameters"
		config-struct-name = "configuration"
		operation-file-name = "parameters"
		config-file-name = "config"

		[[parameter]]
		name = "boolRange1"
		scope = ["global","session"]
		access = ["file"]
		type = "bool"
		domain-type = "range"
		values = ["false"]
		comment = "boolRange1"
		update-mode = "dynamic"
`

	t3 := `
		parameter-struct-name = "AllParameters"
		config-struct-name = "configuration"
		operation-file-name = "parameters"
		config-file-name = "config"

		[[parameter]]
		name = "stringRange1"
		scope = ["global"]
		access = ["file"]
		type = "string"
		domain-type = "range"
		values = ["ss1","ss2","ss3"]
		comment = "stringRange1"
		update-mode = "dynamic"
`

	t4 := `		
		parameter-struct-name = "AllParameters"
		config-struct-name = "configuration"
		operation-file-name = "parameters"
		config-file-name = "config"

		[[parameter]]
		name = "int64set3"
		scope = ["global"]
		access = ["file"]
		type = "int64"
		domain-type = "set"
		values = ["1","1"]
		comment = "int64Set3"
		update-mode = "fix"
`

	t5 := `
		parameter-struct-name = "AllParameters"
		config-struct-name = "configuration"
		operation-file-name = "parameters"
		config-file-name = "config"

		[[parameter]]
		name = "int64Range2"
		scope = ["global"]
		access = ["file"]
		type = "int64"
		domain-type = "range"
		values = ["1000","1500","10000"]
		comment = "int64Range2"
		update-mode = "dynamic"
`

	t6 := `
		parameter-struct-name = "AllParameters"
		config-struct-name = "configuration"
		operation-file-name = "parameters"
		config-file-name = "config"

		[[parameter]]
		name = "float64set3"
		scope = ["global"]
		access = ["file"]
		type = "float64"
		domain-type = "set"
		values = ["1.001","1.001"]
		comment = "float64Set3"
		update-mode = "fix"
`

	t7 := `
		parameter-struct-name = "AllParameters"
		config-struct-name = "configuration"
		operation-file-name = "parameters"
		config-file-name = "config"

		[[parameter]]
		name = "float64Range2"
		scope = ["global"]
		access = ["file"]
		type = "float64"
		domain-type = "range"
		values = ["1000.001","1500.01","10000.03"]
		comment = "float64Range2"
		update-mode = "dynamic"
`

	t8 := `
		parameter-struct-name = "AllParameters"
		config-struct-name = "configuration"
		operation-file-name = "parameters"
		config-file-name = "config"

		[[parameter]]
		name = "Float64Range2"
		scope = ["global"]
		access = ["file"]
		type = "float64"
		domain-type = "range"
		values = ["1000.001","1500.01","10000.03"]
		comment = "float64Range2"
		update-mode = "dynamic"
`
	type args struct {
		input string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"t1", args{t1}, false},
		{"t2", args{t2}, true},
		{"t3", args{t3}, true},
		{"t4", args{t4}, true},
		{"t5", args{t5}, true},
		{"t6", args{t6}, true},
		{"t7", args{t7}, true},
		{"t8", args{t8}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := &parameters{}
			if err := params.LoadParametersDefinitionFromString(tt.args.input); (err != nil) != tt.wantErr {
				t.Errorf("LoadParametersDefinitionFromString() error = %v, wantErr %v", err, tt.wantErr)
				return
			} else if err != nil {
				return
			}

			tmpl, err := template.New("test").Parse(tmplStr)
			if err != nil {
				panic(err)
			}

			parameterName1 := "parameters.go"
			f, err := os.Create(parameterName1)
			if err != nil {
				panic(err)
			}

			err = tmpl.Execute(f, params)
			if err != nil {
				panic(err)
			}

			tomlTmpl, err := template.New("toml").Parse(tomlTmplString)
			if err != nil {
				panic(err)
			}

			tomlName := "config.toml"
			tomlf, err := os.Create(tomlName)
			if err != nil {
				panic(err)
			}

			err = tomlTmpl.Execute(tomlf, params)
			if err != nil {
				panic(err)
			}

			testTmpl, err := template.New("genTest").Parse(testOperationTmpl)
			if err != nil {
				panic(err)
			}

			parameterName2 := "parameters_test.go"
			testf, err := os.Create(parameterName2)
			if err != nil {
				panic(err)
			}

			err = testTmpl.Execute(testf, params)
			if err != nil {
				panic(err)
			}

			_ = f.Close()
			_ = os.Remove(parameterName1)
			_ = tomlf.Close()
			_ = os.Remove(tomlName)
			_ = testf.Close()
			_ = os.Remove(parameterName2)
		})
	}

}

func Test_String(t *testing.T) {
	fmt.Printf("%s \n", "`toml:\"name\"`")
}

func TestNewConfigurationFileGenerator(t *testing.T) {
	type args struct {
		defFileName string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"t1", args{"test/def1.toml"}, false},
		{"t2", args{"test/def2.toml"}, false},
		{"t3", args{"test/system_vars_def.toml"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gen := NewConfigurationFileGenerator(tt.args.defFileName)
			if err := gen.Generate(); (err != nil) != tt.wantErr {
				t.Errorf("Generator() = %v, want %v", err, tt.wantErr)
			}

			_ = gen.DeleteGeneratedFiles()
		})
	}
}
