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

package plan

import (
	"encoding/json"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/jsonvalue"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (builder *QueryBuilder) buildJSONTable(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr) (int32, error) {
	if tbl.JSONTable == nil || len(exprs) != 1 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "JSON_TABLE requires a source, path and COLUMNS")
	}
	if exprs[0].GetP() != nil {
		target := types.T_varchar.ToType()
		var err error
		exprs[0], err = appendExplicitCastBeforeExpr(builder.GetContext(), exprs[0], makePlan2Type(&target))
		if err != nil {
			return 0, err
		}
	}
	switch types.T(exprs[0].Typ.Id) {
	case types.T_json, types.T_char, types.T_varchar, types.T_text, types.T_datalink, types.T_any:
	default:
		return 0, moerr.NewInvalidInput(builder.GetContext(), "JSON_TABLE source must be JSON, text or DATALINK")
	}
	if _, err := types.ParseStringToPath(tbl.JSONTable.Path); err != nil {
		return 0, err
	}
	colDefs := make([]*plan.ColDef, 0)
	names := make(map[string]struct{})
	options := jsonvalue.ConversionOptions{}
	if proc := builder.compCtx.GetProcess(); proc != nil && proc.GetSessionInfo() != nil {
		options.Location = proc.GetSessionInfo().TimeZone
	}
	var bindColumns func([]*tree.JSONTableColumn) ([]jsonvalue.TableColumn, error)
	bindColumns = func(columns []*tree.JSONTableColumn) ([]jsonvalue.TableColumn, error) {
		out := make([]jsonvalue.TableColumn, 0, len(columns))
		for _, column := range columns {
			if column.ReversePolicies {
				return nil, moerr.NewNotSupported(builder.GetContext(), "JSON_TABLE reversed policies require parse-time diagnostics")
			}
			bound := jsonvalue.TableColumn{Name: strings.ToLower(column.Name), OriginName: column.Name, Kind: column.Kind, Path: column.Path,
				OnEmpty: jsonvalue.TableResponse{Action: "null"}, OnError: jsonvalue.TableResponse{Action: "null"}}
			if column.Kind != "ordinality" {
				if _, err := types.ParseStringToPath(column.Path); err != nil {
					return nil, err
				}
			}
			if column.Kind == "nested" {
				var err error
				bound.Children, err = bindColumns(column.Children)
				if err != nil {
					return nil, err
				}
			} else {
				if _, exists := names[bound.Name]; exists {
					return nil, moerr.NewInvalidInputf(builder.GetContext(), "duplicate JSON_TABLE column '%s'", column.Name)
				}
				names[bound.Name] = struct{}{}
				typ := makeGeneratedPlan2Type(types.T_uint32, 0, 0, false)
				if column.Kind != "ordinality" {
					var err error
					typ, err = getTypeFromAst(builder.GetContext(), column.Type)
					if err != nil {
						return nil, err
					}
					typ.NotNullable = false
				}
				bound.Type = MakeTypeByPlan2Type(typ)
				switch bound.Type.Oid {
				case types.T_bool, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
					types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
					types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128, types.T_decimal256,
					types.T_char, types.T_varchar, types.T_text, types.T_binary, types.T_varbinary, types.T_blob,
					types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_year, types.T_json:
				default:
					return nil, moerr.NewNotSupportedf(builder.GetContext(), "JSON_TABLE target type %s", bound.Type.DescString())
				}
				for _, response := range []struct {
					ast *tree.JSONTableResponse
					dst *jsonvalue.TableResponse
				}{{column.OnEmpty, &bound.OnEmpty}, {column.OnError, &bound.OnError}} {
					if response.ast == nil {
						continue
					}
					response.dst.Action, response.dst.Default = response.ast.Action, response.ast.Default
					if response.ast.Action == "default" {
						value, err := types.ParseStringToByteJson(response.ast.Default)
						if err != nil {
							return nil, err
						}
						converted := jsonvalue.ConvertTableScalarWithContext(builder.GetContext(), value, bound.Type, options)
						if converted.Status == jsonvalue.StatusStatementError {
							return nil, converted.Err
						}
						switch converted.Status {
						case jsonvalue.StatusSuccess, jsonvalue.StatusJSONNull, jsonvalue.StatusTruncated:
							// Admission only: publish a truncation warning only if the
							// default is actually used during this execution attempt.
						default:
							return nil, moerr.NewInvalidInputf(builder.GetContext(), "invalid JSON_TABLE DEFAULT for column '%s'", column.Name)
						}
					}
				}
				colDefs = append(colDefs, &plan.ColDef{Name: bound.Name, OriginName: column.Name, Typ: typ})
			}
			out = append(out, bound)
		}
		return out, nil
	}
	columns, err := bindColumns(tbl.JSONTable.Columns)
	if err != nil {
		return 0, err
	}
	param, err := json.Marshal(jsonvalue.TableSpec{Version: 1, RootPath: tbl.JSONTable.Path, Columns: columns})
	if err != nil {
		return 0, err
	}
	return builder.appendNode(&plan.Node{NodeType: plan.Node_FUNCTION_SCAN, Stats: &plan.Stats{},
		TableDef:    &plan.TableDef{TableType: "func_table", Cols: colDefs, TblFunc: &plan.TableFunction{Name: "json_table", Param: param, IsSingle: true}},
		BindingTags: []int32{builder.genNewBindTag()}, TblFuncExprList: exprs}, ctx), nil
}
