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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

// RenameLegacyCheckConstraintNames updates only CHECK-name tokens in retained
// CREATE SQL. Do not synthesize executable Checks or reformat the entire AST:
// defaults, comments and other mode-sensitive payload must stay byte-for-byte.
func RenameLegacyCheckConstraintNames(ctx context.Context, sql, oldTable, newTable string) (string, error) {
	if oldTable == newTable {
		return sql, nil
	}
	rows, mode, err := recoverLegacyCheckConstraints(ctx, sql)
	if err != nil {
		return "", err
	}
	checks := make([]*pb.CheckDef, len(rows))
	for i, row := range rows {
		checks[i] = &pb.CheckDef{Name: row.Name}
	}
	renamed := RenameGeneratedCheckConstraintNames(checks, oldTable, newTable)
	names := make(map[string]string)
	for i, check := range renamed {
		if check.Name != checks[i].Name {
			names[strings.ToLower(checks[i].Name)] = check.Name
		}
	}
	if len(names) == 0 {
		return sql, nil
	}
	scanner := mysql.NewScannerWithSQLMode(dialect.MYSQL, sql, mysql.ParseSQLModeFlags(mode))
	defer mysql.PutScanner(scanner)
	var out strings.Builder
	written, previous := 0, 0
	start, end := 0, 0
	var pending string
	matched := make(map[string]bool)
	for {
		token, value := scanner.ScanWithComments()
		if token == 0 {
			break
		}
		if token == mysql.LEX_ERROR {
			return "", moerr.NewInvalidInput(ctx, "cannot locate legacy CHECK name tokens")
		}
		if token == mysql.COMMENT {
			continue
		}
		if token == mysql.CHECK && pending != "" {
			if start < written || end < start || end > len(sql) {
				return "", moerr.NewInvalidInput(ctx, "invalid legacy CHECK name token offsets")
			}
			out.WriteString(sql[written:start])
			out.WriteString(sqlquote.Ident(names[pending]))
			written = end
			matched[pending] = true
		}
		pending = ""
		if previous == mysql.CONSTRAINT {
			name := strings.ToLower(value)
			if _, ok := names[name]; ok {
				start, end = scanner.PrePos, scanner.Pos
				for start < end && strings.ContainsRune(" \t\r\n", rune(sql[start])) {
					start++
				}
				pending = name
			}
		}
		previous = token
	}
	if len(matched) != len(names) {
		return "", moerr.NewInvalidInput(ctx, "cannot locate all legacy CHECK names")
	}
	out.WriteString(sql[written:])
	return out.String(), nil
}
