// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"unicode"

	"github.com/BurntSushi/toml"
)

/*
	scope options:

	session:

	If you change a session parameter,
		the value remains in effect within your session until you change the variable to a different value
			or the session ends.
		The change has no effect on other sessions.

	global:

	If you change a global parameter, the value is remembered and used to initialize the session value
		for new sessions until you change the parameter to a different value or the server exits.
	The change is visible to any client that accesses the global value.
		However, the change affects the corresponding session value only for clients that connect after the change.
		The global parameter change does not affect the session value for any current client sessions (
		not even the session within which the global value change occurs).
*/
var scopeOptions = []string{"session", "global"}

// the access
var accessOptions = []string{"cmd", "file", "env"}

// the data type
var dataTypeOptions = []string{"string", "int64", "float64", "bool"}

var boolFalseOptions = []string{"false", "off"}
var boolTrueOptions = []string{"true", "on"}

// the domain
var domainTyoeOptions = []string{"set", "range"}

// the updateMode
var updateModeOptions = []string{"dynamic", "fix", "hotload"}

// a unit in configuration template for a configuration item
type parameter struct {
	// the Name
	Name string `toml:"name"`

	// also the Name whose initial character is capital
	CapitalName string

	// the scope (also visibility)
	// where the others can see the value
	Scope []string `toml:"scope"`

	// the access: command line; configure file; environment variable in the shell
	// where the value can be changed
	Access []string `toml:"access"`

	// the data type of the value, like int, string ,bool,float,...
	DataType string `toml:"type"`

	// the domain of the value for validity check: set; range;
	// set: select one among discrete values.
	// range: select a point in a real range
	DomainType string `toml:"domain-type"`

	/**
	The first one is the initial-value.
	for range domain-type
	[initial-value,minimum,maximum]

	for set domain-type
		normal situation
			[initial-value,value2,value3,value4,...,]

		special situation
			[] is empty, you can set any value in the domain of the data type
	*/
	Values []string `toml:"values"`

	// the comments for this parameter
	Comment string `toml:"comment"`

	// the update mode in running
	// dynamic: can be updated in running
	// fix: can not be updated in running
	// hotLoad: can be loaded from the config file and updated in running
	UpdateMode string `toml:"update-mode"`
}

type parameters struct {
	// the name of parameter data structure
	// the parameter structure can be exported.
	// the first character must be capital.
	ParameterStructName string `toml:"parameter-struct-name"`

	// the name of configuration data structure
	// the configuration structure is an internal structure that will not be exported.
	// the first character should not be capital.
	ConfigurationStructName string `toml:"config-struct-name"`

	// the name of operation file which contains all interface code for operating parameters
	// filename extension ".go" eliminated.
	OperationFileName string `toml:"operation-file-name"`

	// the name of configuration file which contains all auto generated parameters.
	// filename extension ".toml" eliminated.
	ConfigurationFileName string `toml:"config-file-name"`

	// the array of parameters
	Parameter []parameter
}

/**
load and analyse parameters definition from the template file
*/
func (params *parameters) LoadParametersDefinitionFromFile(filename string) error {
	pfile, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer pfile.Close()

	fbytes, err := io.ReadAll(pfile)
	if err != nil {
		return err
	}
	return params.LoadParametersDefinitionFromString(string(fbytes))
}

func (params *parameters) LoadParametersDefinitionFromString(input string) error {
	_, err := toml.Decode(input, params)
	if err != nil {
		return err
	}

	// check parameter-struct-name
	if !isExportedGoIdentifier(params.ParameterStructName) {
		return fmt.Errorf("ParameterStructName [%s] is not a valid identifier name within ascii characters", params.ParameterStructName)
	}

	// check config-struct-name
	if !isGoIdentifier(params.ConfigurationStructName) {
		return fmt.Errorf("ConfigurationStructName [%s] is not a valid identifier name within ascii characters", params.ConfigurationStructName)
	}

	// check parameter operation file name
	if !isGoStructAndInterfaceIdentifier(params.OperationFileName) {
		return fmt.Errorf("OperationFileName [%s] is not a valid identifier name within ascii characters", params.OperationFileName)
	}

	// check parameter configuration file name
	if !isGoStructAndInterfaceIdentifier(params.ConfigurationFileName) {
		return fmt.Errorf("ConfigurationFileName [%s] is not a valid identifier name within ascii characters", params.ConfigurationFileName)
	}

	// check parameter
	for _, p := range params.Parameter {
		if !isGoIdentifier(p.Name) {
			return fmt.Errorf("name [%s] is not a valid identifier name within ascii characters", p.Name)
		}

		if !isScope(p.Scope) {
			return fmt.Errorf("scope [%s] is not a valid scope", p.Scope)
		}

		if !isAccess(p.Access) {
			return fmt.Errorf("access [%s] is not a valid access", p.Access)
		}

		if !isDataType(p.DataType) {
			return fmt.Errorf("DataType [%s] is not a valid data type", p.DataType)
		}

		if !isDomainType(p.DomainType) {
			return fmt.Errorf("DomainType [%s] is not a valid domain type", p.DomainType)
		}

		if !checkValues(p.DataType, p.DomainType, p.Values) {
			return fmt.Errorf("values [%s] is not compatible with data type %s and domain type %s", p.Values, p.DataType, p.DomainType)
		}

		if !isUpdateMode(p.UpdateMode) {
			return fmt.Errorf("UpdateMode [%s] is not a valid update mode", p.UpdateMode)
		}
	}

	// parameter name dedup
	var dedup = make(map[string]bool)

	if _, ok := dedup[params.ParameterStructName]; !ok {
		dedup[params.ParameterStructName] = true
	} else {
		return fmt.Errorf("has duplicate parameter struct name %s", params.ParameterStructName)
	}

	if _, ok := dedup[params.ConfigurationStructName]; !ok {
		dedup[params.ConfigurationStructName] = true
	} else {
		return fmt.Errorf("has duplicate configuration struct name %s", params.ConfigurationStructName)
	}

	if _, ok := dedup[params.OperationFileName]; !ok {
		dedup[params.OperationFileName] = true
	} else {
		return fmt.Errorf("has duplicate operation file name %s", params.OperationFileName)
	}

	if _, ok := dedup[params.ConfigurationFileName]; !ok {
		dedup[params.ConfigurationFileName] = true
	} else {
		return fmt.Errorf("has duplicate configuration file name %s", params.ConfigurationFileName)
	}

	for _, p := range params.Parameter {
		if _, ok := dedup[p.Name]; !ok {
			dedup[p.Name] = true
		} else {
			return fmt.Errorf("has duplicate parameter name %s", p.Name)
		}
	}

	// make capital name for the name
	for i := 0; i < len(params.Parameter); i++ {
		p := params.Parameter[i].Name
		capName := string(unicode.ToUpper(rune(p[0]))) + p[1:]
		params.Parameter[i].CapitalName = capName
	}

	return err
}

/**
check if x is a valid lowercase ascii character
*/
func isLowCaseASCIIChar(x byte) bool {
	return x >= 'a' && x <= 'z' || x == '_'
}

/**
check if x is a valid uppercase ascii character
*/
func isUpCaseASCIIChar(x byte) bool {
	return x >= 'A' && x <= 'Z'
}

/**
check if x is a valid ascii character
*/
func isASCIIChar(x byte) bool {
	return x >= 'a' && x <= 'z' || x >= 'A' && x <= 'Z' || x == '_'
}

/**
check if x is a valid ascii digit
*/
func isASCIIDigit(x byte) bool {
	return x >= '0' && x <= '9'
}

/**
check if the string can be a valid identifier in Golang.

In our context, the identifier can be defined as follow:
identifier = low-case-letter { letter | digit }
low-case-letter = "a" ... "z" | "_"
letter     = "a" ... "z" | "A" ... "Z" | "_"
digit      = "0" ... "9"

here,the letters only contain ascii characters.
So,it's a subset of the identifier in Golang.
*/
func isGoIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}

	//the first character is a lowercase ascii character.
	if !isLowCaseASCIIChar(s[0]) {
		return false
	}

	//the rest should ascii character | ascii digit | _
	for i := 1; i < len(s); i++ {
		if !(isASCIIChar(s[i]) || isASCIIDigit(s[i])) {
			return false
		}
	}

	return true
}

/**
check if the string can be a valid identifier in Golang.

In our context, the identifier can be defined as follow:
identifier = up-case-letter { letter | digit }
up-case-letter = "A" ... "Z"
letter     = "a" ... "z" | "A" ... "Z" | "_"
digit      = "0" ... "9"

here,the letters only contain ascii characters.
So,it's a subset of the identifier in Golang.
*/
func isExportedGoIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}

	// the first character is a lowercase ascii character.
	if !isUpCaseASCIIChar(s[0]) {
		return false
	}

	// the rest should ascii character | ascii digit | _
	for i := 1; i < len(s); i++ {
		if !(isASCIIChar(s[i]) || isASCIIDigit(s[i])) {
			return false
		}
	}

	return true
}

/**
check if the string can be a valid struct and interface identifier in Golang.

In our context, the identifier can be defined as follow:
identifier = letter { letter | digit }
letter     = "a" ... "z" | "A" ... "Z" | "_"
digit      = "0" ... "9"

here,the letters only contain ascii characters.
So,it's a subset of the identifier in Golang.
*/
func isGoStructAndInterfaceIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}

	// the first character is a lowercase ascii character.
	if !isASCIIChar(s[0]) {
		return false
	}

	// the rest should ascii character | ascii digit | _
	for i := 1; i < len(s); i++ {
		if !(isASCIIChar(s[i]) || isASCIIDigit(s[i])) {
			return false
		}
	}

	return true
}

/**
check if the scope is valid.
*/
func isScope(sc []string) bool {
	return isSubset(sc, scopeOptions)
}

/**
check if the access is valid.
*/
func isAccess(sc []string) bool {
	return isSubset(sc, accessOptions)
}

/**
check if the data type is valid
*/
func isDataType(x string) bool {
	return isInSlice(x, dataTypeOptions)
}

/**
check if the domain type is valid
*/
func isDomainType(x string) bool {
	return isInSlice(x, domainTyoeOptions)
}

/**
make a float string look like a float64 string.
*/
func looklikeFloat64String(s string) string {
	if i := strings.Index(s, "."); i != -1 {
		if i == len(s)-1 { // . is the last one, append a zero
			return s + "0"
		}
		return s
	} else {
		return s + ".0"
	}
}

/**
check if the values are valid based on dataType, domainType.
*/
func checkValues(dataType string, domainType string, values []string) bool {
	switch dataType {
	case "string":
		switch domainType {
		case "set":
			if len(values) < 1 {
				return true
			} else if len(values) == 1 {
				return true
			} else {
				if hasDuplicateValueString(values) {
					return false
				}
			}
			return true
		case "range":
			return false
		default:
			return false
		}
	case "int64":
		switch domainType {
		case "set":
			if len(values) < 1 {
				return true
			}

			var intArr []int64
			for i := 0; i < len(values); i++ {
				if v, err := strconv.ParseInt(values[i], 10, 64); err != nil {
					return false
				} else {
					intArr = append(intArr, v)
				}
			}

			if len(intArr) == 1 {
				return true
			}

			//first one is the default value.
			//there are no duplicate values in the set.
			if hasDuplicateValueInt64(intArr) {
				return false
			}
			return true
		case "range":
			if len(values) != 3 {
				return false
			}

			var intArr []int64
			for i := 0; i < len(values); i++ {
				if v, err := strconv.ParseInt(values[i], 10, 64); err != nil {
					return false
				} else {
					intArr = append(intArr, v)
				}
			}

			if !(intArr[0] >= intArr[1] && intArr[0] <= intArr[2]) {
				return false
			}
			return true
		}
	case "float64":
		switch domainType {
		case "set":
			if len(values) < 1 {
				return true
			}

			var fArr []float64
			for i := 0; i < len(values); i++ {
				if v, err := strconv.ParseFloat(values[i], 64); err != nil {
					return false
				} else {
					fArr = append(fArr, v)
				}
			}

			if len(fArr) == 1 {
				return true
			}

			if hasDuplicateValueFloat64(fArr) {
				return false
			}

			// for configuration file generation
			for i := 0; i < len(values); i++ {
				values[i] = looklikeFloat64String(values[i])
			}

			return true
		case "range":
			if len(values) != 3 {
				return false
			}

			var fArr []float64
			for i := 0; i < len(values); i++ {
				if v, err := strconv.ParseFloat(values[i], 64); err != nil {
					return false
				} else {
					fArr = append(fArr, v)
				}
			}

			if !(fArr[0] >= fArr[1] && fArr[0] <= fArr[2]) {
				return false
			}
			return true
		}
	case "bool":
		switch domainType {
		case "set":
			if len(values) < 1 {
				return true
			}

			low := strings.ToLower(values[0])
			if !isInSlice(low, boolFalseOptions) && !isInSlice(low, boolTrueOptions) {
				return false
			}
			return true
		case "range":
			return false
		default:

		}
	}

	return false
}

/**
check if the update mode is valid
*/
func isUpdateMode(um string) bool {
	return isInSlice(um, updateModeOptions)
}

/**
check if A is a valid subset of B.
if A has something that is not in B, then return false.
if A has duplicate elements, then return false.
if A has nothing,then return false.
*/
func isSubset(A []string, B []string) bool {
	if len(A) > len(B) {
		return false
	}

	sort.Strings(B)

	// check the elements of A is in B or not
	for _, x := range A {
		if !isInSlice(x, B) {
			return false
		}
	}

	// check if A has duplicate elements
	dedup := map[string]bool{}
	for _, x := range A {
		if _, ok := dedup[x]; ok { // duplicate scope
			return false
		}
		dedup[x] = true
	}

	return len(A) > 0
}

/**
check if x in a slice
*/
func isInSlice(x string, arr []string) bool {
	for _, y := range arr {
		if x == y {
			return true
		}
	}
	return false
}

/**
check if x has duplicate values.
*/
func hasDuplicateValueString(x []string) bool {
	var dedup = make(map[string]bool)
	for _, v := range x {
		if _, ok := dedup[v]; !ok {
			dedup[v] = true
		} else {
			return true
		}
	}
	return false
}

/**
check if x has duplicate values.
*/
func hasDuplicateValueInt64(x []int64) bool {
	var dedup = make(map[int64]bool)
	for _, v := range x {
		if _, ok := dedup[v]; !ok {
			dedup[v] = true
		} else {
			return true
		}
	}
	return false
}

/**
check if x has duplicate values.
*/
func hasDuplicateValueFloat64(x []float64) bool {
	var dedup = make(map[float64]bool)
	for _, v := range x {
		if _, ok := dedup[v]; !ok {
			dedup[v] = true
		} else {
			return true
		}
	}
	return false
}

var defaultParameterTempate = `// Code generated by 'make config'. DO NOT EDIT.
// Edit the definition of the configuration item in the '.toml' file according to your usage.
// instead of changing this file.
package config

import (
	"fmt"
	"sync"
	"github.com/BurntSushi/toml"
	"math"
)

// all parameters in the system
type {{.ParameterStructName}} struct{
	//read and write lock
	rwlock	sync.RWMutex
{{range .Parameter}}
	{{ printf "\n\t/**\n\tName:\t%s\n\tScope:\t%s\n\tAccess:\t%s\n\tDataType:\t%s\n\tDomainType:\t%s\n\tValues:\t%s\n\tComment:\t%s\n\tUpdateMode:\t%s\n\t*/"  
			.Name .Scope .Access .DataType .DomainType .Values .Comment .UpdateMode
	}}
	{{ printf "%s    %s" .Name .DataType }}
{{- end}}

	//parameter name -> parameter definition string
	name2definition map[string]string
}//end {{.ParameterStructName}}

//all parameters can be set in the configuration file.
type {{.ConfigurationStructName}} struct{
	//read and write lock
	rwlock	sync.RWMutex
{{range .Parameter}}
	{{- if ne .UpdateMode "fix"}}
	{{ printf "\n\t/**\n\tName:\t%s\n\tScope:\t%s\n\tAccess:\t%s\n\tDataType:\t%s\n\tDomainType:\t%s\n\tValues:\t%s\n\tComment:\t%s\n\tUpdateMode:\t%s\n\t*/"  
			.Name .Scope .Access .DataType .DomainType .Values .Comment .UpdateMode
	}}
	{{ printf "%s    %s  ` + "`toml:" + `\"%s\"` + "`" + `" .CapitalName .DataType .Name }}
	{{- end}}
{{- end}}

	//parameter name -> updated flag
	name2updatedFlags map[string]bool
}//end {{.ConfigurationStructName}}

/**
check if x in a slice
*/
func (ap *{{.ParameterStructName}}) isInSlice(x string, arr []string) bool{
	for _, y := range arr {
		if x == y {
			return true
		}
	}
	return false
}

/**
check if x in a slice
*/
func (ap *{{.ParameterStructName}}) isInSliceBool(x bool, arr []bool) bool {
	for _, y := range arr {
		if x == y {
			return true
		}
	}
	return false
}

/**
check if x in a slice
*/
func (ap *{{.ParameterStructName}}) isInSliceInt64(x int64, arr []int64) bool {
	for _, y := range arr {
		if x == y {
			return true
		}
	}
	return false
}

/**
check if x in a slice
*/
func (ap *{{.ParameterStructName}}) isInSliceFloat64(x float64, arr []float64) bool {
	for _, y := range arr {
		if math.Abs(x-y) < 0.000001 {
			return true
		}
	}
	return false
}

/**
prepare something before anything else.
This is unsafe in multi-thread environment.
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
	if p,ok := ap.name2definition[name]; !ok {
		return "", fmt.Errorf("there is no parameter %s",name)
	} else {
		return p, nil
	}
}

/**
check if there is the parameter
*/
func (ap *{{.ParameterStructName}}) HasParameter(name string)bool{
	ap.rwlock.RLock()
	defer ap.rwlock.RUnlock()
	ap.prepareAnything()
	if _,ok := ap.name2definition[name]; !ok{
		return false
	} else {
		return true
	}
}

/**
Load the initial values of all parameters.
*/
func (ap *{{.ParameterStructName}}) LoadInitialValues()error{
	ap.PrepareDefinition()
	var err error
	{{range .Parameter }}
	{{if eq .DataType "string"}}
	{{.Name}}choices := []{{.DataType}} {
		{{range .Values -}}
		{{printf "\"%s\"" .}},
		{{- end}} 
	}
	if len({{.Name}}choices) != 0 {
		if err = ap.set{{.CapitalName}}({{.Name}}choices[0]) ; err != nil {
			return fmt.Errorf("set%s failed.error:%v",{{printf "\"%s\"" .CapitalName}},err)
		}
	} else {
		//empty string
		if err = ap.set{{.CapitalName}}("") ; err != nil {
			return fmt.Errorf("set%s failed.error:%v",{{printf "\"%s\"" .CapitalName}},err)
		}
	}{{else}}
	{{.Name}}choices :=[]{{.DataType}} {
		{{range .Values -}}
		{{printf "%s" .}},
		{{- end}}
	}
	if len({{.Name}}choices) != 0 {
		if err = ap.set{{.CapitalName}}({{.Name}}choices[0]) ; err != nil {
			return fmt.Errorf("set%s failed.error:%v",{{printf "\"%s\"" .CapitalName}},err)
		}
	} else { {{if eq .DataType "bool"}}
		if err = ap.set{{.CapitalName}}(false) ; err != nil {
			return fmt.Errorf("set%s failed.error:%v",{{printf "\"%s\"" .CapitalName}},err)
		}
	{{else if eq .DataType "int64"}}
		if err = ap.set{{.CapitalName}}(0) ; err != nil {
			return fmt.Errorf("set%s failed.error:%v",{{printf "\"%s\"" .CapitalName}},err)
		}
	{{else if eq .DataType "float64"}}
		if err = ap.set{{.CapitalName}}(0.0) ; err != nil {
			return fmt.Errorf("set%s failed.error:%v",{{printf "\"%s\"" .CapitalName}},err)
		}
	{{- end -}} }
	{{- end}}
	{{- end}}
	return nil
}
{{with $Params := .}}
{{- range $Params.Parameter}}
/**
Get the value of the parameter {{.Name}}
*/
func (ap * {{$Params.ParameterStructName}} ) Get{{.CapitalName}}() {{.DataType}} {
	ap.rwlock.RLock()
	defer ap.rwlock.RUnlock()
	return ap.{{.Name}}
}
{{end}}
{{end -}}

{{with $Params := .}}
{{- range $Params.Parameter}}
{{- if ne .UpdateMode "fix"}}
/**
Set the value of the parameter {{.Name}}
*/
func (ap * {{$Params.ParameterStructName}} ) Set{{.CapitalName}}(value {{.DataType}})error {
	return  ap.set{{.CapitalName}}(value)
}
{{end -}}
{{end -}}
{{end -}}

{{with $Params := .}}
{{- range $Params.Parameter}}
/**
Set the value of the parameter {{.Name}}
*/
func (ap * {{$Params.ParameterStructName}} ) set{{.CapitalName}}(value {{.DataType}})error {
	ap.rwlock.Lock()
	defer ap.rwlock.Unlock()
	{{if eq .DataType "bool" -}}
	{{if eq .DomainType "set" -}}
		choices :=[]{{.DataType}} {
			{{range .Values -}}
			{{printf "%s" .}},
			{{- end}}	
		}
		if len( choices ) != 0{
			if !ap.isInSliceBool(value, choices){
				return fmt.Errorf("set{{.CapitalName}},the value %t is not in set %v",value,choices)
			}
		}//else means any bool value: true or false
	{{else}}
		return fmt.Errorf("the bool type does not support domainType %s",{{printf "\"%s\"" .DomainType}})
	{{- end}}
	{{else if eq .DataType "string" -}}
	{{if eq .DomainType "set"}}
		choices :=[]{{.DataType}} {
			{{range .Values -}}
			{{printf "\"%s\"" .}},
			{{- end}}	
		}
		if len( choices ) != 0{
			if !ap.isInSlice(value, choices){
				return fmt.Errorf("set{{.CapitalName}},the value %s is not in set %v",value,choices)
			}
		}//else means any string
	{{else}}
		return fmt.Errorf("the string type does not support domainType %s",{{printf "\"%s\"" .DomainType}})
	{{- end}}
	{{else if eq .DataType "int64"}}
	{{if eq .DomainType "set"}}
		choices :=[]{{.DataType}} {
			{{range .Values -}}
			{{printf "%s" .}},
			{{- end}}	
		}
		if len( choices ) != 0{
			if !ap.isInSliceInt64(value, choices){
				return fmt.Errorf("set{{.CapitalName}},the value %d is not in set %v",value,choices)
			}
		}//else means any int64
	{{else if eq .DomainType "range"}}
		choices :=[]{{.DataType}} {
			{{range .Values -}}
			{{printf "%s" .}},
			{{- end}}	
		}
		if !(value >= choices[1] && value <= choices[2]){
			return fmt.Errorf("set{{.CapitalName}},the value %d is not in the range [%d,%d]",value,choices[1],choices[2])
		}
	{{else}}
		return fmt.Errorf("the int64 type does not support domainType %s",{{printf "\"%s\"" .DomainType}})
	{{- end}}
	{{else if eq .DataType "float64"}}
		{{if eq .DomainType "set"}}
			choices :=[]{{.DataType}} {
				{{range .Values -}}
				{{printf "%s" .}},
				{{- end}}	
			}
			if len( choices ) != 0{
				if !ap.isInSliceFloat64(value, choices){
					return fmt.Errorf("set{{.CapitalName}},the value %f is not in set %v",value,choices)
				}
			}//else means any float64
		{{else if eq .DomainType "range"}}
			choices :=[]{{.DataType}} {
				{{range .Values -}}
				{{printf "%s" .}},
				{{- end}}	
			}
			if !(value >= choices[1] && value <= choices[2]){
				return fmt.Errorf("set{{.CapitalName}},the value %f is not in the range [%f,%f]",value,choices[1],choices[2])
			}
		{{else}}
			return fmt.Errorf("the float64 type does not support domainType %s",{{printf "\"%s\"" .DomainType}})
		{{end}}
	{{- end}}
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
	{{- range .Parameter}}
	{{- if ne .UpdateMode "fix"}}
	{{ printf "config.name2updatedFlags[\"%s\"] = false" .Name}}
	{{- end}}
	{{- end}}
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
		//var failedItems []string
		//for _, item := range failed {
		//	failedItems = append(failedItems, item.String())
		//}
		//return fmt.Errorf("decode failed %s. error:%v",failedItems,err)
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
	{{- range .Parameter}}
	{{- if ne .UpdateMode "fix"}}
	if config.getUpdatedFlag("{{.Name}}"){
		if err = ap.set{{.CapitalName}}(config.{{.CapitalName}}); err != nil{
			return fmt.Errorf("update parameter {{.Name}} failed.error:%v",err)
		}
	}
	{{- end}}
	{{- end}}
	return nil
}

/**
Load configuration from file into {{.ConfigurationStructName}}.
Then update items into {{.ParameterStructName}}
*/
func Load{{.ConfigurationStructName}}FromFile(filename string,params *{{.ParameterStructName}}) error{
	config := &{{.ConfigurationStructName}}{}
	if err := config.LoadConfigurationFromFile(filename); err != nil{
		return err
	}

	if err := params.UpdateParametersWithConfiguration(config); err != nil{
		return err
	}
	return nil
}
`

var defaultConfigurationTemplate = `#Start MatrixOne cluster on docker or kubernetes, please refer to this repo [matrixorigin/matrixone-operator](https://github.com/matrixorigin/matrixone-operator)

{{range $index, $param := .Parameter -}}
{{ if ne .UpdateMode "fix" -}}
	{{ printf "\n#\tName:\t%s\n#\tScope:\t%s\n#\tAccess:\t%s\n#\tDataType:\t%s\n#\tDomainType:\t%s\n#\tValues:\t%s\n#\tComment:\t%s\n#\tUpdateMode:\t%s\n\t"  
			.Name .Scope .Access .DataType .DomainType .Values .Comment .UpdateMode
	}}

	{{- with $count := len .Values -}}
		{{- if ne 0 $count -}}
			{{- if eq $param.DataType "string" -}}
				{{- $param.Name}} = "{{index $param.Values 0}}"
			{{- else}}
				{{- $param.Name}} = {{index $param.Values 0}}
			{{- end}}
{{end}}
{{- else}}
		{{- if eq $param.DataType "string" -}}
				{{- $param.Name}} = ""
		{{- else if eq $param.DataType "bool" -}}
				{{- $param.Name}} = false
		{{- else if eq $param.DataType "int64" -}}
				{{- $param.Name}} = 0
		{{- else if eq $param.DataType "float64" -}}
				{{- $param.Name}} = 0.0
		{{- end}}
{{end}}
{{- end}}
{{- end}}
`

var defaultOperationTestTemplate = `// Code generated by 'make config'. DO NOT EDIT.
// Edit the definition of the configuration item in the '.toml' file according to your usage.
// instead of changing this file.
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

func is{{.ConfigurationStructName}}Equal(c1,c2 *{{.ConfigurationStructName}}) bool {

{{range .Parameter -}}
{{ if ne .UpdateMode "fix"}}
	if c1.{{.CapitalName}} != c2.{{.CapitalName}} {
		return false
	}
{{- end}}
{{- end}}

	return true
}

func Test_{{.ConfigurationStructName}}_LoadConfigurationFromString(t *testing.T) {
	t1 := ` + "`" + `
{{range $index,$param := .Parameter -}}
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
	t1_config:=&{{.ConfigurationStructName}}{
		rwlock:            sync.RWMutex{},

{{range $index,$param := .Parameter -}}
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
		config *{{.ConfigurationStructName}}
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

			if ( is{{.ConfigurationStructName}}Equal(config,tt.args.config) != true ) != tt.wantErr3{
				t.Errorf("Configuration are not equal. %v vs %v ",config,tt.args.config)
				return
			}
		})
	}
}
`

// ConfigurationFileGenerator Analyse the template files.
// Generate configuration file, operation interfaces.
type ConfigurationFileGenerator interface {
	/**
	Input: parameter definition file name
	Output:
		1. operation interface and code for parameters
		2. configuraion file for parameters
	*/
	Generate() error

	/**
	Remove all generated files
	*/
	DeleteGeneratedFiles() error
}

type ConfigurationFileGeneratorImpl struct {
	//the name of the parameter definition
	parameterDefinitionFileName string

	//the output directory of the configuration file
	configurationOutputDirectory string

	//the template string for the auto generated parameter operation interfaces and classes.
	parameterTemplate string

	//the template string for the auto generated initial configuration file.
	configurationTemplate string

	//the template string for the auto generated parameter operation interfaces test cases.
	parameterTestCasesTemplate string

	//path to generated files
	pathOfCodeFile     string
	pathOfCodeTestFile string
	pathOfConfigFile   string
}

func (cfgi *ConfigurationFileGeneratorImpl) getOutDirectory() (string, error) {
	var outDir string

	defDir, err := filepath.Abs(filepath.Dir(cfgi.parameterDefinitionFileName))
	if err != nil {
		return "", fmt.Errorf("get the directory of parameter definition file failed.error:%v", err)
	}

	if len(cfgi.configurationOutputDirectory) == 0 {
		outDir = defDir
	} else {
		outDir = cfgi.configurationOutputDirectory
	}
	return outDir, nil
}

func (cfgi *ConfigurationFileGeneratorImpl) getConfigurationFileNameAndCodeFileName(operationFileName, configurationFileName string) error {
	var outDir string
	var err error
	outDir, err = cfgi.getOutDirectory()
	if err != nil {
		return err
	}
	cfgi.pathOfCodeFile = outDir + "/" + operationFileName + ".go"
	cfgi.pathOfConfigFile = outDir + "/" + configurationFileName + ".toml"
	cfgi.pathOfCodeTestFile = outDir + "/" + operationFileName + "_test.go"
	return err
}

func (cfgi *ConfigurationFileGeneratorImpl) Generate() error {
	params := &parameters{}
	if err := params.LoadParametersDefinitionFromFile(cfgi.parameterDefinitionFileName); err != nil {
		return fmt.Errorf("LoadParametersDefinitionFromFile failed.error:%v", err)
	}

	parameterTmpl, err := template.New("MakeParameterTemplate").Parse(cfgi.parameterTemplate)
	if err != nil {
		return fmt.Errorf("make parameter template failed. error:%v", err)
	}

	err = cfgi.getConfigurationFileNameAndCodeFileName(params.OperationFileName, params.ConfigurationFileName)
	if err != nil {
		return err
	}

	f, err := os.Create(cfgi.pathOfCodeFile)
	if err != nil {
		return err
	}
	defer f.Close()

	err = parameterTmpl.Execute(f, params)
	if err != nil {
		return err
	}

	tomlTmpl, err := template.New("MakeConfigurationTemplate").Parse(cfgi.configurationTemplate)
	if err != nil {
		return err
	}

	tomlf, err := os.Create(cfgi.pathOfConfigFile)
	if err != nil {
		return err
	}
	defer tomlf.Close()

	err = tomlTmpl.Execute(tomlf, params)
	if err != nil {
		return err
	}

	testCasesTmpl, err := template.New("MakeTestCasesTemplate").Parse(cfgi.parameterTestCasesTemplate)
	if err != nil {
		return err
	}

	testCasesf, err := os.Create(cfgi.pathOfCodeTestFile)
	if err != nil {
		return err
	}
	defer testCasesf.Close()

	err = testCasesTmpl.Execute(testCasesf, params)
	if err != nil {
		return err
	}

	return nil
}

func (cfgi *ConfigurationFileGeneratorImpl) DeleteGeneratedFiles() error {
	if len(cfgi.pathOfCodeFile) != 0 {
		_ = os.Remove(cfgi.pathOfCodeFile)
	}
	if len(cfgi.pathOfConfigFile) != 0 {
		_ = os.Remove(cfgi.pathOfConfigFile)
	}
	if len(cfgi.pathOfCodeTestFile) != 0 {
		_ = os.Remove(cfgi.pathOfCodeTestFile)
	}
	return nil
}

func NewConfigurationFileGenerator(defFileName string) ConfigurationFileGenerator {
	return &ConfigurationFileGeneratorImpl{
		parameterDefinitionFileName:  defFileName,
		configurationOutputDirectory: "",
		parameterTemplate:            defaultParameterTempate,
		configurationTemplate:        defaultConfigurationTemplate,
		parameterTestCasesTemplate:   defaultOperationTestTemplate,
		pathOfConfigFile:             "",
		pathOfCodeFile:               "",
		pathOfCodeTestFile:           "",
	}
}

func NewConfigurationFileGeneratorWithOutputDirectory(defFileName, outputDirectory string) ConfigurationFileGenerator {
	return &ConfigurationFileGeneratorImpl{
		parameterDefinitionFileName:  defFileName,
		configurationOutputDirectory: outputDirectory,
		parameterTemplate:            defaultParameterTempate,
		configurationTemplate:        defaultConfigurationTemplate,
		parameterTestCasesTemplate:   defaultOperationTestTemplate,
		pathOfConfigFile:             "",
		pathOfCodeFile:               "",
		pathOfCodeTestFile:           "",
	}
}
