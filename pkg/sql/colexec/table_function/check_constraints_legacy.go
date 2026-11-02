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

package table_function

import (
	"context"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// Keep metadata display and DDL namespace admission on one legacy SQL decoder.
func parseLegacyCheckConstraintRows(ctx context.Context, schema, table, createSQL string) ([]checkConstraintRow, error) {
	checks, err := plan2.RecoverLegacyCheckConstraints(ctx, createSQL)
	if err != nil {
		return nil, err
	}
	rows := make([]checkConstraintRow, 0, len(checks))
	for _, check := range checks {
		rows = append(rows, checkConstraintRow{
			schema: schema, table: table, name: check.Name, clause: check.Clause,
			constraintType: "CHECK", enforced: check.Enforced,
		})
	}
	return rows, nil
}
