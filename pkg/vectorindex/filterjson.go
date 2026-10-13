// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vectorindex

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// filterJSONPred is one predicate of a CAGRA / IVF-PQ filter JSON array.
type filterJSONPred struct {
	Col  int    `json:"col"`
	Op   string `json:"op"`
	Val  any    `json:"val,omitempty"`
	Lo   any    `json:"lo,omitempty"`
	Hi   any    `json:"hi,omitempty"`
	Vals []any  `json:"vals,omitempty"`
}

// DescribeFilterJSON renders the predicate array of a CAGRA / IVF-PQ filter JSON as
// SQL, the predicates joined with AND. colName names a predicate column: -1 is the
// primary key, i the i-th INCLUDE column. An empty array renders as "".
func DescribeFilterJSON(filterJSON string, colName func(col int) string) (string, error) {
	if filterJSON == "" {
		return "", nil
	}
	var preds []filterJSONPred
	if err := json.Unmarshal([]byte(filterJSON), &preds); err != nil {
		return "", moerr.NewInternalErrorNoCtxf("invalid vector index filter JSON: %v", err)
	}
	parts := make([]string, 0, len(preds))
	for _, p := range preds {
		name := colName(p.Col)
		switch p.Op {
		case "=", "!=", "<", "<=", ">", ">=":
			parts = append(parts, fmt.Sprintf("%s %s %s", name, p.Op, describeFilterValue(p.Val)))
		case "between":
			parts = append(parts, fmt.Sprintf("%s BETWEEN %s AND %s", name, describeFilterValue(p.Lo), describeFilterValue(p.Hi)))
		case "in":
			vals := make([]string, len(p.Vals))
			for i, v := range p.Vals {
				vals[i] = describeFilterValue(v)
			}
			parts = append(parts, fmt.Sprintf("%s IN (%s)", name, strings.Join(vals, ", ")))
		case "is_null":
			parts = append(parts, name+" IS NULL")
		case "is_not_null":
			parts = append(parts, name+" IS NOT NULL")
		case "prefix":
			parts = append(parts, fmt.Sprintf("%s LIKE %s", name, describeFilterValue(fmt.Sprint(p.Val)+"%")))
		default:
			return "", moerr.NewInternalErrorNoCtxf("invalid vector index filter op %q", p.Op)
		}
	}
	return strings.Join(parts, " AND "), nil
}

// describeFilterValue renders a filter JSON value as a SQL literal.
func describeFilterValue(v any) string {
	if s, ok := v.(string); ok {
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
	return fmt.Sprint(v)
}

// DescribeIndexFilter renders the filter JSON of a CAGRA / IVF-PQ index search as SQL:
// column -1 is pkName, column i the i-th of include.
func DescribeIndexFilter(filterJSON string, include []string, pkName string) (string, error) {
	return DescribeFilterJSON(filterJSON, func(col int) string {
		if col == -1 {
			return pkName
		}
		if col >= 0 && col < len(include) {
			return include[col]
		}
		return fmt.Sprintf("#%d", col)
	})
}
