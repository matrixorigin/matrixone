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

package mysql

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var yaccProductionStartRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*:`)

var legacyKeywordPolicyExceptions = map[string]string{
	// These words predate the incremental keyword policy. Do not add new entries
	// without first deciding whether the keyword should be non-reserved instead.
	"_binary":      "legacy parser behavior before incremental keyword policy",
	"apply":        "legacy parser behavior before incremental keyword policy",
	"cdc":          "legacy parser behavior before incremental keyword policy",
	"centroidx":    "legacy parser behavior before incremental keyword policy",
	"config":       "legacy parser behavior before incremental keyword policy",
	"current_role": "legacy parser behavior before incremental keyword policy",
	"datalink":     "legacy parser behavior before incremental keyword policy",
	"dedup":        "legacy parser behavior before incremental keyword policy",
	"end":          "legacy parser behavior before incremental keyword policy",
	"escape":       "legacy parser behavior before incremental keyword policy",
	"ilike":        "legacy parser behavior before incremental keyword policy",
	"intersect":    "legacy parser behavior before incremental keyword policy",
	"minus":        "legacy parser behavior before incremental keyword policy",
	"phyplan":      "legacy parser behavior before incremental keyword policy",
	"quick":        "legacy parser behavior before incremental keyword policy",
	"reindex":      "legacy parser behavior before incremental keyword policy",
	"retention":    "legacy parser behavior before incremental keyword policy",
	"runs":         "legacy parser behavior before incremental keyword policy",
	"schedule":     "legacy parser behavior before incremental keyword policy",
	"strict":       "legacy parser behavior before incremental keyword policy",
	"temporary":    "legacy parser behavior before incremental keyword policy",
	"timeout":      "legacy parser behavior before incremental keyword policy",
	"timezone":     "legacy parser behavior before incremental keyword policy",
	"until":        "legacy parser behavior before incremental keyword policy",
	"upgrade":      "legacy parser behavior before incremental keyword policy",
}

func TestNewKeywordsHaveExplicitIdentifierPolicy(t *testing.T) {
	identTokens := mysqlIdentifierTokens(t)
	mysqlReserved := mysqlKeywordReservedStatus(t)
	tokenNames := mysqlTokenNames(t)

	remainingLegacy := make(map[string]struct{}, len(legacyKeywordPolicyExceptions))
	for word, reason := range legacyKeywordPolicyExceptions {
		if reason == "" {
			t.Fatalf("legacy keyword policy exception %q must include a reason", word)
		}
		remainingLegacy[word] = struct{}{}
	}

	violations := make([]string, 0, len(keywords))
	for word, token := range keywords {
		tokenName, ok := tokenNames[token]
		if !ok {
			t.Fatalf("keyword %q uses token %d, but token name was not found in mysql_sql.go", word, token)
		}
		if tokenName == "ID" || identTokens[tokenName] {
			continue
		}
		if reserved, found := mysqlReserved[strings.ToUpper(word)]; found && reserved {
			continue
		}
		if _, ok := legacyKeywordPolicyExceptions[word]; ok {
			delete(remainingLegacy, word)
			continue
		}
		violations = append(violations, fmt.Sprintf("%s -> %s", word, tokenName))
	}

	if len(violations) > 0 {
		sort.Strings(violations)
		t.Fatalf("new keywords must either be accepted by ident through non_reserved_keyword/not_keyword, be MySQL reserved, or have an explicit legacy exception:\n%s", strings.Join(violations, "\n"))
	}

	if len(remainingLegacy) > 0 {
		stale := make([]string, 0, len(remainingLegacy))
		for word := range remainingLegacy {
			stale = append(stale, word)
		}
		sort.Strings(stale)
		t.Fatalf("legacy keyword policy exceptions are no longer needed and should be removed:\n%s", strings.Join(stale, "\n"))
	}
}

func mysqlTokenNames(t *testing.T) map[int]string {
	t.Helper()

	data, err := os.ReadFile("mysql_sql.go")
	if err != nil {
		t.Fatal(err)
	}

	tokenNames := make(map[int]string)
	re := regexp.MustCompile(`const\s+([A-Z][A-Z0-9_]*)\s+=\s+([0-9]+)`)
	for _, match := range re.FindAllStringSubmatch(string(data), -1) {
		value, err := strconv.Atoi(match[2])
		if err != nil {
			t.Fatal(err)
		}
		tokenNames[value] = match[1]
	}
	return tokenNames
}

func mysqlIdentifierTokens(t *testing.T) map[string]bool {
	t.Helper()

	data, err := os.ReadFile("mysql_sql.y")
	if err != nil {
		t.Fatal(err)
	}

	tokens := map[string]bool{
		"ID":       true,
		"QUOTE_ID": true,
	}
	for _, production := range []string{"non_reserved_keyword", "not_keyword"} {
		for token := range collectYaccProductionTokens(t, string(data), production) {
			tokens[token] = true
		}
	}
	return tokens
}

func mysqlKeywordReservedStatus(t *testing.T) map[string]bool {
	t.Helper()

	dataPath := filepath.Join("..", "..", "..", "..", "util", "sysview", "data.go")
	data, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}

	status := make(map[string]bool)
	re := regexp.MustCompile(`\('([^']+)'\s*,\s*([01])\)`)
	for _, match := range re.FindAllStringSubmatch(string(data), -1) {
		status[match[1]] = match[2] == "1"
	}
	return status
}

func collectYaccProductionTokens(t *testing.T, grammar, production string) map[string]bool {
	t.Helper()

	var body strings.Builder
	inProduction := false
	for _, line := range strings.Split(grammar, "\n") {
		line = strings.TrimRight(line, "\r")
		trimmed := strings.TrimSpace(stripLineComment(line))
		if !inProduction {
			if strings.HasPrefix(trimmed, production+":") {
				inProduction = true
				body.WriteString(strings.TrimPrefix(trimmed, production+":"))
				body.WriteByte('\n')
			}
			continue
		}
		if trimmed == "%%" || isYaccProductionStart(trimmed) {
			break
		}
		body.WriteString(trimmed)
		body.WriteByte('\n')
	}
	if !inProduction {
		t.Fatalf("production %q not found in mysql_sql.y", production)
	}

	tokens := make(map[string]bool)
	tokenRE := regexp.MustCompile(`\b[A-Z][A-Z0-9_]*\b`)
	for _, token := range tokenRE.FindAllString(body.String(), -1) {
		tokens[token] = true
	}
	return tokens
}

func stripLineComment(line string) string {
	if idx := strings.Index(line, "//"); idx >= 0 {
		return line[:idx]
	}
	return line
}

func isYaccProductionStart(line string) bool {
	if strings.HasPrefix(line, "|") || strings.HasPrefix(line, "{") || strings.HasPrefix(line, "}") || line == "" {
		return false
	}
	return yaccProductionStartRE.MatchString(line)
}
