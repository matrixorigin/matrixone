// Copyright 2026 Matrix Origin
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

package function

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func xmlFunctionCheck(overloads []overload, inputs []types.Type) checkResult {
	for _, in := range inputs {
		if !in.Oid.IsMySQLString() && in.Oid != types.T_any {
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		}
	}
	return stringDomainFixedTypeMatch(overloads, inputs)
}
func xmlFunctionReturnType(_ []types.Type) types.Type {
	// The admitted 16 MiB output exceeds both VARCHAR and MEDIUMTEXT.
	return types.NewWithCharset(types.T_text, types.MaxLongTextLen, 0, types.CharsetUTF8)
}

func ExtractValue(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return executeXMLFunction(parameters, result, proc, length, selectList, false)
}
func UpdateXML(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return executeXMLFunction(parameters, result, proc, length, selectList, true)
}

func executeXMLFunction(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList, update bool) error {
	doc := vector.GenerateFunctionStrParameter(parameters[0])
	xpath := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[types.Varlena](result)
	var program *xmlXPath
	var warnings process.WarningAccumulator
	warnings.SetWarningRetentionForProcess(proc)
	defer warnings.Reset()
	for row := uint64(0); row < uint64(length); row++ {
		if selectList != nil && (selectList.IgnoreAllRow() || selectList.Contains(row)) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if err := proc.Ctx.Err(); err != nil {
			return err
		}
		if !parameters[1].IsConst() {
			return moerr.NewNotSupported(proc.Ctx, "Only constant XPATH queries are supported")
		}
		path, pathNull := xpath.GetStrValue(row)
		if pathNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if program == nil {
			// Admit borrowed vector bytes before allocating the compiler string.
			if len(path) > xmlXPathLimit {
				return moerr.NewInvalidInput(proc.Ctx, "XPath exceeds 16 KiB")
			}
			var err error
			program, err = compileXMLXPath(proc.Ctx, string(path))
			if err != nil {
				return err
			}
			if update {
				for _, p := range program.paths {
					if p.terminalText() {
						return moerr.NewNotSupported(proc.Ctx, "UpdateXML text() target is unsupported")
					}
				}
			}
		}
		xml, docNull := doc.GetStrValue(row)
		var replacement []byte
		replacementNull := false
		if update {
			replacement, replacementNull = vector.GenerateFunctionStrParameter(parameters[2]).GetStrValue(row)
		}
		if docNull || replacementNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if len(replacement) > xmlInputLimit {
			return moerr.NewInvalidInput(proc.Ctx, "XML replacement exceeds 8 MiB")
		}
		// Conversion copies at most the admitted input; no borrowed vector data
		// survives this row. Source spans keep replacement byte-exact.
		if len(xml) > xmlInputLimit {
			return moerr.NewInvalidInput(proc.Ctx, "XML input exceeds 8 MiB")
		}
		d, err := parseXMLFragment(proc.Ctx, string(xml))
		if errors.Is(err, errXMLMalformed) {
			warnings.Add(1525, "Incorrect XML value: malformed XML fragment")
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if update && program.count {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		ids, err := d.evaluate(program)
		if err != nil {
			return err
		}
		var value string
		if update {
			value = d.source
			if len(ids) == 1 {
				n := d.nodes[ids[0]]
				outLen := len(value) - (n.end - n.start) + len(replacement)
				if outLen > xmlOutputLimit {
					return moerr.NewInvalidInput(proc.Ctx, "XML output limit exceeded")
				}
				if err = d.budget.spend(1+outLen/4096, outLen+len(replacement)); err != nil {
					return err
				}
				value = value[:n.start] + string(replacement) + value[n.end:]
			}
		} else {
			value, err = d.extract(program, ids)
			if err != nil {
				return err
			}
		}
		if err = d.budget.spend(1+len(value)/4096, len(value)); err != nil {
			return err
		}
		if err = rs.AppendBytes([]byte(value), false); err != nil {
			return err
		}
	}
	warnings.Flush(proc)
	return nil
}
