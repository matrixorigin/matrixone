// Copyright 2022 Matrix Origin
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

package plan

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (builder *QueryBuilder) buildParseJsonlData(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	return builder.buildParseJsonl("parse_jsonl_data", tbl, ctx, exprs, children)
}

func (builder *QueryBuilder) buildParseJsonlFile(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	return builder.buildParseJsonl("parse_jsonl_file", tbl, ctx, exprs, children)
}

// syntax:
// parse_jsonl_data(data, options)
// parse_jsonl_file(file, options)
// options is optional, when not present, return each line as a json value.
// options as a string, can be either a "short format", or a "long format".
// short format:
//     A string.  Each line of the jsonline data must be an array, type of the array elements is
//     specified using a single character:
//     		b: boolean
//     		i: int32
//     		I: int64
//     		f: float32
//     		F: float64
//     		s: string
//     		t: timestamp
//     		example: "bits", means the array has 4 elements, the result scheuma is
//     			col1: boolean, col2: int32, col3: timestamp;, col4: varchar
// long format: it is a json object.
//    {
//     	"format": "either array or object",
//     	"cols": [
//     		{
//     			"name": "col1",
//     			"type": "boolean"
//          },
//     		{
//     			"name": "col2",
//     			"type": "int32"
// 			},
//     		{
//     			"name": "col3",
//     			"type": "timestamp"
//          },
//     		{
//     			"name": "col4",
//     			"type": "varchar"
// 			}
//      ]
//   }

type ParseJsonlOptionsCol struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
type ParseJsonlOptions struct {
	Format string                 `json:"format"`
	Cols   []ParseJsonlOptionsCol `json:"cols"`
}

const (
	ParseJsonlFormatLine   = "line"   // treat each line as a single json value
	ParseJsonlFormatArray  = "array"  // each line is an array
	ParseJsonlFormatObject = "object" // each line is an object

	ParseJsonlTypeBool      = "bool"
	ParseJsonlTypeInt32     = "int32"
	ParseJsonlTypeInt64     = "int64"
	ParseJsonlTypeTimestamp = "timestamp"
	ParseJsonlTypeFloat32   = "float32"
	ParseJsonlTypeFloat64   = "float64"
	ParseJsonlTypeString    = "string"
)

func (builder *QueryBuilder) buildParseJsonl(tvfName string, tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) == 0 || len(exprs) > 2 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS != 1 or NARGS != 2).")
	}

	var opts ParseJsonlOptions
	if len(exprs) == 1 {
		opts.Format = ParseJsonlFormatLine
		opts.Cols = []ParseJsonlOptionsCol{
			{
				Name: "value",
				Type: "string",
			},
		}
	} else {
		param2, ok := tbl.Func.Exprs[1].(*tree.NumVal)
		if !ok {
			return 0, moerr.NewInvalidInputf(builder.GetContext(), "the 2nd param of %s must be a string", tvfName)
		}
		optstr := param2.String()

		if strings.Contains(optstr, "{") {
			// long format,
			err := json.Unmarshal([]byte(optstr), &opts)
			if err != nil {
				return 0, err
			}

			switch opts.Format {
			case ParseJsonlFormatArray, ParseJsonlFormatObject:
				// OK
			default:
				return 0, moerr.NewInvalidInputf(builder.GetContext(), "Invalid format %s in options, must be array or object", opts.Format)
			}
		} else {
			// short format,
			opts.Format = "array"
			for idx, c := range optstr {
				switch c {
				case 'b':
					opts.Cols = append(opts.Cols, ParseJsonlOptionsCol{Name: fmt.Sprintf("col%d", idx), Type: ParseJsonlTypeBool})
				case 'i':
					opts.Cols = append(opts.Cols, ParseJsonlOptionsCol{Name: fmt.Sprintf("col%d", idx), Type: ParseJsonlTypeInt32})
				case 'I':
					opts.Cols = append(opts.Cols, ParseJsonlOptionsCol{Name: fmt.Sprintf("col%d", idx), Type: ParseJsonlTypeInt64})
				case 'f':
					opts.Cols = append(opts.Cols, ParseJsonlOptionsCol{Name: fmt.Sprintf("col%d", idx), Type: ParseJsonlTypeFloat32})
				case 'F':
					opts.Cols = append(opts.Cols, ParseJsonlOptionsCol{Name: fmt.Sprintf("col%d", idx), Type: ParseJsonlTypeFloat64})
				case 's':
					opts.Cols = append(opts.Cols, ParseJsonlOptionsCol{Name: fmt.Sprintf("col%d", idx), Type: ParseJsonlTypeString})
				case 't':
					opts.Cols = append(opts.Cols, ParseJsonlOptionsCol{Name: fmt.Sprintf("col%d", idx), Type: ParseJsonlTypeTimestamp})
				default:
					return 0, moerr.NewInvalidInputf(builder.GetContext(), "Invalid character '%c' in options", c)
				}
			}
		}
	}

	// build the table def
	cols := make([]*plan.ColDef, 0, len(opts.Cols))
	for idx, col := range opts.Cols {
		cols = append(cols, &plan.ColDef{
			Name: col.Name,
		})
		switch col.Type {
		case ParseJsonlTypeBool:
			cols[idx].Typ = makeSimplePlan2Type(types.T_bool)
		case ParseJsonlTypeInt32:
			cols[idx].Typ = makeSimplePlan2Type(types.T_int32)
		case ParseJsonlTypeInt64:
			cols[idx].Typ = makeSimplePlan2Type(types.T_int64)
		case ParseJsonlTypeTimestamp:
			cols[idx].Typ = makeSimplePlan2Type(types.T_timestamp)
		case ParseJsonlTypeFloat32:
			cols[idx].Typ = makeSimplePlan2Type(types.T_float32)
		case ParseJsonlTypeFloat64:
			cols[idx].Typ = makeSimplePlan2Type(types.T_float64)
		case ParseJsonlTypeString:
			cols[idx].Typ = makeSimplePlan2Type(types.T_varchar)
		}
	}

	paramData, err := json.Marshal(opts)
	if err != nil {
		return 0, err
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name:     tvfName,
				Param:    paramData,
				IsSingle: true, // dont want to parse data twice.
			},
			Cols: cols,
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		TblFuncExprList: exprs,
		Children:        children,
	}

	return builder.appendNode(node, ctx), nil
}
