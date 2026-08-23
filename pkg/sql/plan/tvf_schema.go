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
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// The column-schema spec shared by parse_jsonl_data / parse_jsonl_file and
// esql_tvf / sql_tvf. A spec string is either:
//   - short format: one character per column (b,i,I,f,F,s,t), or
//   - long format: a JSON object {"format": "...", "cols": [{"name","type"}...]}.
//
// esql_tvf / sql_tvf ignore the "format" field (they always consume CSV); it is
// meaningful only to the jsonl TVFs, which validate it in their own builder.

// parseTVFColumnSchema parses a column-schema spec string into ParseJsonlOptions.
// It does NOT validate the "format" field — callers that care (parse_jsonl)
// validate it themselves so this helper stays reusable by esql_tvf / sql_tvf.
func parseTVFColumnSchema(ctx context.Context, optstr string) (ParseJsonlOptions, error) {
	var opts ParseJsonlOptions
	if strings.Contains(optstr, "{") {
		// long format
		if err := json.Unmarshal([]byte(optstr), &opts); err != nil {
			return opts, err
		}
		return opts, nil
	}
	// short format: one type character per column
	opts.Format = ParseJsonlFormatArray
	for idx, c := range optstr {
		var typ string
		switch c {
		case 'b':
			typ = ParseJsonlTypeBool
		case 'i':
			typ = ParseJsonlTypeInt32
		case 'I':
			typ = ParseJsonlTypeInt64
		case 'f':
			typ = ParseJsonlTypeFloat32
		case 'F':
			typ = ParseJsonlTypeFloat64
		case 's':
			typ = ParseJsonlTypeString
		case 't':
			typ = ParseJsonlTypeTimestamp
		default:
			return opts, moerr.NewInvalidInputf(ctx, "Invalid character '%c' in options", c)
		}
		opts.Cols = append(opts.Cols, ParseJsonlOptionsCol{Name: fmt.Sprintf("col%d", idx), Type: typ})
	}
	return opts, nil
}

// buildTVFColDefs maps a parsed column schema to plan column definitions. It
// errors on an unrecognized type name so a malformed schema is rejected rather
// than silently producing an untyped column.
func buildTVFColDefs(ctx context.Context, opts ParseJsonlOptions) ([]*plan.ColDef, error) {
	cols := make([]*plan.ColDef, 0, len(opts.Cols))
	seen := make(map[string]bool, len(opts.Cols))
	for _, col := range opts.Cols {
		// A duplicate name would silently alias two output columns to one
		// source field position in the foreign TVF field mapping.
		if seen[col.Name] {
			return nil, moerr.NewInvalidInputf(ctx, "duplicate column name %q in options", col.Name)
		}
		seen[col.Name] = true
		var t types.T
		switch col.Type {
		case ParseJsonlTypeBool:
			t = types.T_bool
		case ParseJsonlTypeInt32:
			t = types.T_int32
		case ParseJsonlTypeInt64:
			t = types.T_int64
		case ParseJsonlTypeTimestamp:
			t = types.T_timestamp
		case ParseJsonlTypeFloat32:
			t = types.T_float32
		case ParseJsonlTypeFloat64:
			t = types.T_float64
		case ParseJsonlTypeString:
			t = types.T_varchar
		default:
			return nil, moerr.NewInvalidInputf(ctx, "Invalid column type %q in options", col.Type)
		}
		cols = append(cols, &plan.ColDef{
			Name: col.Name,
			Typ:  makeSimplePlan2Type(t),
		})
	}
	return cols, nil
}
