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

package scanner

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// CaseInfo holds the test files for a single BVT category.
type CaseInfo struct {
	Category string
	Files    []string
}

// ScanBVTCases scans test/distributed/cases/ and returns case info per category.
func ScanBVTCases(repoRoot string) ([]CaseInfo, error) {
	casesDir := filepath.Join(repoRoot, "test", "distributed", "cases")
	entries, err := os.ReadDir(casesDir)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("read cases dir: %v", err)
	}

	result := make([]CaseInfo, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info := CaseInfo{Category: entry.Name()}
		catDir := filepath.Join(casesDir, entry.Name())
		_ = filepath.WalkDir(catDir, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil || d.IsDir() {
				return nil
			}
			if strings.HasSuffix(d.Name(), ".test") || strings.HasSuffix(d.Name(), ".sql") {
				info.Files = append(info.Files, d.Name())
			}
			return nil
		})
		sort.Strings(info.Files)
		result = append(result, info)
	}
	return result, nil
}

// FormatCaseSummary returns a compact text listing of BVT cases for the LLM prompt.
// Uses compact mode by default to save tokens.
func FormatCaseSummary(cases []CaseInfo) string {
	var b strings.Builder
	for _, c := range cases {
		b.WriteString(c.Category)
		b.WriteString("/")
		b.WriteString(strconv.Itoa(len(c.Files)))
		b.WriteString(" ")
	}
	return b.String()
}

// ReadSampleCases reads the first N lines from BVT .test/.sql files
// in categories relevant to the given changed paths.
// This gives the LLM concrete examples of existing case format.
func ReadSampleCases(repoRoot string, changedPaths []string, maxSamples int, maxLinesPerFile int) string {
	if maxSamples <= 0 {
		maxSamples = 3
	}
	if maxLinesPerFile <= 0 {
		maxLinesPerFile = 40
	}

	// Determine relevant categories from changed paths
	categories := inferCategories(changedPaths)
	if len(categories) == 0 {
		return ""
	}

	casesDir := filepath.Join(repoRoot, "test", "distributed", "cases")
	var b strings.Builder
	sampled := 0

	for _, cat := range categories {
		if sampled >= maxSamples {
			break
		}
		catDir := filepath.Join(casesDir, cat)
		entries, err := os.ReadDir(catDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if sampled >= maxSamples {
				break
			}
			name := e.Name()
			if e.IsDir() || !(strings.HasSuffix(name, ".test") || strings.HasSuffix(name, ".sql")) {
				continue
			}
			data, err := os.ReadFile(filepath.Join(catDir, name))
			if err != nil {
				continue
			}
			lines := strings.Split(string(data), "\n")
			if len(lines) > maxLinesPerFile {
				lines = lines[:maxLinesPerFile]
			}
			b.WriteString("### ")
			b.WriteString(cat)
			b.WriteString("/")
			b.WriteString(name)
			b.WriteString("\n```sql\n")
			b.WriteString(strings.Join(lines, "\n"))
			b.WriteString("\n```\n\n")
			sampled++
		}
	}
	return b.String()
}

// inferCategories maps changed file paths to likely BVT categories.
func inferCategories(paths []string) []string {
	seen := make(map[string]bool)
	mappings := map[string][]string{
		"pkg/sql/plan":       {"function", "optimizer", "subquery", "join", "window", "cte"},
		"pkg/sql/compile":    {"function", "expression", "dml"},
		"pkg/sql/parsers":    {"ddl", "dml", "expression"},
		"pkg/vm/engine/tae":  {"disttae", "dml"},
		"pkg/vm/engine/dist": {"disttae"},
		"pkg/txn":            {"pessimistic_transaction", "optimistic"},
		"pkg/lockservice":    {"pessimistic_transaction"},
		"pkg/frontend":       {"snapshot", "pitr", "tenant"},
		"pkg/cdc":            {"dml"},
		"pkg/fulltext":       {"fulltext"},
		"pkg/vectorindex":    {"vector"},
		"pkg/vectorize":      {"function", "expression"},
		"pkg/partition":      {"ddl"},
		"pkg/backup":         {"snapshot", "pitr"},
	}

	for _, p := range paths {
		for prefix, cats := range mappings {
			if strings.HasPrefix(p, prefix) {
				for _, c := range cats {
					seen[c] = true
				}
			}
		}
	}

	result := make([]string, 0, len(seen))
	for c := range seen {
		result = append(result, c)
	}
	sort.Strings(result)
	return result
}
