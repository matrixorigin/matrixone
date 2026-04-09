// Copyright 2024 Matrix Origin
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

package dedup

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/testinfra/types"
)

// Deduplicator checks whether suggested cases overlap with existing test files.
type Deduplicator struct {
	repoRoot string
	// existing holds normalized SQL statement sets per category dir.
	// Key: directory path relative to repo root. Value: set of normalized SQL strings.
	existing map[string]map[string]bool
}

// New creates a Deduplicator that lazily loads existing cases.
func New(repoRoot string) *Deduplicator {
	return &Deduplicator{
		repoRoot: repoRoot,
		existing: make(map[string]map[string]bool),
	}
}

// Filter removes suggested cases whose SQL content significantly overlaps
// with existing test files in the target directory.
// Returns the filtered list and a list of skip reasons.
func (d *Deduplicator) Filter(cases []types.SuggestedCase) (kept []types.SuggestedCase, skipped []string) {
	for _, sc := range cases {
		dir := d.targetDir(sc)
		if dir == "" {
			kept = append(kept, sc)
			continue
		}

		existingSQL := d.loadDir(dir)
		newStatements := extractStatements(sc.Content)

		if d.isDuplicate(newStatements, existingSQL) {
			skipped = append(skipped, sc.Category+"/"+sc.Filename+": overlaps with existing case")
			continue
		}
		kept = append(kept, sc)
	}
	return
}

// targetDir returns the actual directory to check for duplicates.
func (d *Deduplicator) targetDir(sc types.SuggestedCase) string {
	switch sc.Type {
	case types.TestBVT, types.TestPITR, types.TestSnapshot:
		return filepath.Join("test", "distributed", "cases", sc.Category)
	default:
		// Nightly regression cases are in a separate repo; skip dedup for those.
		return ""
	}
}

// loadDir lazily scans all .sql/.test files in dir and extracts normalized SQL.
func (d *Deduplicator) loadDir(relDir string) map[string]bool {
	if stmts, ok := d.existing[relDir]; ok {
		return stmts
	}

	stmts := make(map[string]bool)
	absDir := filepath.Join(d.repoRoot, relDir)

	_ = filepath.WalkDir(absDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		name := entry.Name()
		if !(strings.HasSuffix(name, ".sql") || strings.HasSuffix(name, ".test")) {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		for _, stmt := range extractStatements(string(data)) {
			stmts[stmt] = true
		}
		return nil
	})

	d.existing[relDir] = stmts
	return stmts
}

// isDuplicate returns true if more than half of the new SQL statements
// already appear in the existing set.
func (d *Deduplicator) isDuplicate(newStatements []string, existing map[string]bool) bool {
	if len(newStatements) == 0 {
		return false
	}

	matches := 0
	for _, stmt := range newStatements {
		if existing[stmt] {
			matches++
		}
	}

	// >50% overlap means duplicate
	return matches*2 > len(newStatements)
}

// extractStatements parses SQL text into normalized statement strings.
// Strips comments, whitespace, and mo-tester tags to focus on actual SQL.
func extractStatements(content string) []string {
	var stmts []string
	var current strings.Builder

	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)

		// Skip empty lines, comments, and mo-tester tags
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		if strings.HasPrefix(trimmed, "#") {
			continue
		}

		current.WriteString(trimmed)
		current.WriteString(" ")

		// Statement ends with semicolon
		if strings.HasSuffix(trimmed, ";") {
			stmt := normalizeSQL(current.String())
			if stmt != "" && !isBoilerplate(stmt) {
				stmts = append(stmts, stmt)
			}
			current.Reset()
		}
	}

	// Handle last statement without trailing semicolon
	if current.Len() > 0 {
		stmt := normalizeSQL(current.String())
		if stmt != "" && !isBoilerplate(stmt) {
			stmts = append(stmts, stmt)
		}
	}

	return stmts
}

// normalizeSQL lowercases and collapses whitespace for comparison.
func normalizeSQL(sql string) string {
	sql = strings.ToLower(strings.TrimSpace(sql))
	// Collapse multiple spaces
	for strings.Contains(sql, "  ") {
		sql = strings.ReplaceAll(sql, "  ", " ")
	}
	return sql
}

// isBoilerplate returns true for common setup/teardown SQL that shouldn't
// contribute to duplicate detection.
func isBoilerplate(sql string) bool {
	prefixes := []string{
		"drop table ",
		"drop database ",
		"drop account ",
		"drop pitr ",
		"drop snapshot ",
		"use ",
		"set ",
		"begin;",
		"commit;",
		"rollback;",
	}
	for _, p := range prefixes {
		if strings.HasPrefix(sql, p) {
			return true
		}
	}
	return false
}
