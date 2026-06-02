// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"context"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	defaultTTLEnable      = "on"
	defaultTTLJobInterval = "1h"
)

// validateTTLExpr validates the TTL expiry expression `col + INTERVAL n unit`.
// The left operand must reference a single existing column whose type is one of
// DATE / DATETIME / TIMESTAMP. cols maps lower-cased column names to their type id.
func validateTTLExpr(reqCtx context.Context, expr tree.Expr, cols map[string]int32) error {
	be, ok := expr.(*tree.BinaryExpr)
	if !ok || be.Op != tree.PLUS {
		return moerr.NewInvalidInput(reqCtx, "TTL expression must be of form 'column + INTERVAL n unit'")
	}
	col, ok := be.Left.(*tree.UnresolvedName)
	if !ok || col.NumParts != 1 || col.Star {
		return moerr.NewInvalidInput(reqCtx, "TTL expression must reference a single column")
	}
	typID, ok := cols[strings.ToLower(col.ColName())]
	if !ok {
		return moerr.NewInvalidInputf(reqCtx, "TTL column '%s' does not exist", col.ColName())
	}
	switch types.T(typID) {
	case types.T_date, types.T_datetime, types.T_timestamp:
	default:
		return moerr.NewInvalidInputf(reqCtx, "TTL column '%s' must be of type DATE, DATETIME or TIMESTAMP", col.ColName())
	}
	return nil
}

// normalizeTTLEnable validates and lower-cases the TTL_ENABLE value (on/off).
func normalizeTTLEnable(reqCtx context.Context, v string) (string, error) {
	s := strings.ToLower(strings.TrimSpace(v))
	if s != "on" && s != "off" {
		return "", moerr.NewInvalidInputf(reqCtx, "TTL_ENABLE must be 'on' or 'off', got '%s'", v)
	}
	return s, nil
}

// validateTTLJobInterval validates the TTL_JOB_INTERVAL value as a positive Go
// duration (e.g. 1h, 30m, 10s).
func validateTTLJobInterval(reqCtx context.Context, v string) (string, error) {
	s := strings.TrimSpace(v)
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return "", moerr.NewInvalidInputf(reqCtx, "TTL_JOB_INTERVAL must be a positive duration (e.g. '1h', '30m'), got '%s'", v)
	}
	return s, nil
}

// ttlExprString reflects the TTL expiry expression to a string suitable for
// persistence and for round-tripping through SHOW CREATE TABLE.
func ttlExprString(expr tree.Expr) string {
	return tree.StringWithOpts(expr, dialect.MYSQL, tree.WithQuoteIdentifier())
}
