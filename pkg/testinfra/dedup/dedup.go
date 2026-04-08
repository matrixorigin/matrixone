// Copyright 2024 Matrix Origin
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

// Package dedup provides SQL test case deduplication based on SQL statement
// fingerprinting. It normalizes SQL statements to produce a canonical
// fingerprint that can be compared for equality, enabling detection of
// duplicate or near-duplicate test cases.
package dedup

import (
	"crypto/sha256"
	"fmt"
	"regexp"
	"strings"
)

// Fingerprint represents the normalized hash of a SQL statement.
type Fingerprint string

// SQLFingerprinter normalizes SQL statements to detect duplicates.
type SQLFingerprinter struct{}

// NewSQLFingerprinter creates a new fingerprinter.
func NewSQLFingerprinter() *SQLFingerprinter {
	return &SQLFingerprinter{}
}

// Fingerprint normalizes a SQL statement and returns its fingerprint.
func (f *SQLFingerprinter) Fingerprint(sql string) Fingerprint {
	normalized := NormalizeSQL(sql)
	hash := sha256.Sum256([]byte(normalized))
	return Fingerprint(fmt.Sprintf("%x", hash[:8]))
}

// NormalizeSQL produces a canonical form of a SQL statement by:
//   - converting to lowercase
//   - replacing literal numbers with ?
//   - replacing quoted strings with ?
//   - collapsing whitespace
//   - removing trailing semicolons
//   - removing comments
func NormalizeSQL(sql string) string {
	s := sql

	// Remove single-line comments
	s = removeSingleLineComments(s)

	// Remove multi-line comments
	s = removeMultiLineComments(s)

	// Lowercase
	s = strings.ToLower(s)

	// Replace quoted strings (single and double quotes)
	s = replaceQuotedStrings(s)

	// Replace numbers
	s = replaceNumbers(s)

	// Collapse whitespace
	s = collapseWhitespace(s)

	// Trim
	s = strings.TrimSpace(s)

	// Remove trailing semicolons
	s = strings.TrimRight(s, ";")
	s = strings.TrimSpace(s)

	return s
}

var singleLineCommentRe = regexp.MustCompile(`--[^\n]*`)

func removeSingleLineComments(s string) string {
	return singleLineCommentRe.ReplaceAllString(s, "")
}

var multiLineCommentRe = regexp.MustCompile(`/\*.*?\*/`)

func removeMultiLineComments(s string) string {
	return multiLineCommentRe.ReplaceAllString(s, "")
}

// replaceQuotedStrings replaces 'string' and "string" with ?
func replaceQuotedStrings(s string) string {
	var result strings.Builder
	i := 0
	for i < len(s) {
		if s[i] == '\'' || s[i] == '"' {
			quote := s[i]
			i++
			for i < len(s) && s[i] != quote {
				if s[i] == '\\' {
					i++ // skip escaped char
				}
				i++
			}
			if i < len(s) {
				i++ // skip closing quote
			}
			result.WriteByte('?')
		} else {
			result.WriteByte(s[i])
			i++
		}
	}
	return result.String()
}

// numberRe matches standalone numbers (integers and decimals).
var numberRe = regexp.MustCompile(`\b\d+(\.\d+)?\b`)

func replaceNumbers(s string) string {
	return numberRe.ReplaceAllString(s, "?")
}

var whitespaceRe = regexp.MustCompile(`\s+`)

func collapseWhitespace(s string) string {
	return whitespaceRe.ReplaceAllString(s, " ")
}

// DedupResult describes the result of deduplication.
type DedupResult struct {
	// Unique are SQL statements that have no duplicates.
	Unique []string
	// Duplicates maps a fingerprint to the group of duplicate SQLs.
	Duplicates map[Fingerprint][]string
}

// Dedup takes a list of SQL statements and identifies duplicates.
func (f *SQLFingerprinter) Dedup(sqls []string) *DedupResult {
	groups := make(map[Fingerprint][]string)
	for _, sql := range sqls {
		fp := f.Fingerprint(sql)
		groups[fp] = append(groups[fp], sql)
	}

	result := &DedupResult{
		Duplicates: make(map[Fingerprint][]string),
	}
	for fp, group := range groups {
		if len(group) == 1 {
			result.Unique = append(result.Unique, group[0])
		} else {
			result.Duplicates[fp] = group
		}
	}
	return result
}

// ExtractSQLStatements parses a mo-tester .test file content and extracts
// the SQL statements from it, ignoring comments and tag lines.
func ExtractSQLStatements(content string) []string {
	var statements []string
	var current strings.Builder

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Skip empty lines
		if trimmed == "" {
			continue
		}

		// Skip mo-tester tag lines (-- @bvt, -- @skip, etc.)
		if strings.HasPrefix(trimmed, "-- @") {
			continue
		}

		// Skip pure comment lines
		if strings.HasPrefix(trimmed, "--") {
			continue
		}

		current.WriteString(trimmed)
		current.WriteString(" ")

		// Statement ends with semicolon
		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
		}
	}

	// Handle statement without trailing semicolon
	if current.Len() > 0 {
		stmt := strings.TrimSpace(current.String())
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}
