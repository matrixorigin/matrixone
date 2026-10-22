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

package frontend

import (
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

const outfileAuditTargetLimit = 4096

var outfileCredentialField = regexp.MustCompile(`(?i)(?:^|[/,:&?;])(?:password|passwd|secret|token|credential|access[_-]?key|private[_-]?key)=`)
var outfileURI = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*://`)

type safeOutfileTarget struct {
	target    string
	truncated bool
	redacted  bool
	invalid   bool
}

func topLevelOutfile(stmt tree.Statement) (*tree.ExportParam, bool) {
	selectStmt, ok := stmt.(*tree.Select)
	if !ok || selectStmt.IsPerform {
		return nil, false
	}
	ep := tree.SelectIntoExport(selectStmt)
	return ep, ep != nil && ep.Outfile
}

func makeOutfileReceipt(stmt tree.Statement) (*motrace.StatementOutfileReceipt, bool) {
	ep, ok := topLevelOutfile(stmt)
	if !ok {
		return nil, false
	}
	target := sanitizeOutfileTarget(ep.FilePath)
	return &motrace.StatementOutfileReceipt{
		Version:         1,
		Target:          target.target,
		Format:          outfileFormat(ep),
		Complete:        false,
		TargetTruncated: target.truncated,
		TargetRedacted:  target.redacted,
		TargetInvalid:   target.invalid,
	}, true
}

func outfileFormat(ep *tree.ExportParam) string {
	if ep == nil {
		return "unknown"
	}
	format := strings.ToLower(ep.ExportFormat)
	if format == "" {
		format = inferFormatFromSuffix(ep.FilePath)
		if format == "" {
			format = "csv"
		}
	}
	switch format {
	case "csv", "jsonline", "parquet":
		return format
	default:
		return "unknown"
	}
}

func sanitizeOutfileTarget(raw string) safeOutfileTarget {
	result := safeOutfileTarget{}
	if !utf8.ValidString(raw) || raw == "" {
		result.invalid = true
		return result
	}
	safe := raw

	if outfileURI.MatchString(raw) {
		parseable := escapeOutfileFormatSpecifiers(raw)
		parsed, err := url.Parse(parseable)
		if err != nil || parsed.Scheme == "" {
			result.invalid = true
			return result
		}
		if parsed.User != nil {
			parsed.User = nil
			result.redacted = true
		}
		if parsed.RawQuery != "" || parsed.ForceQuery {
			parsed.RawQuery = ""
			parsed.ForceQuery = false
			result.redacted = true
		}
		if parsed.Fragment != "" || parsed.RawFragment != "" {
			parsed.Fragment = ""
			parsed.RawFragment = ""
			result.redacted = true
		}
		safe = parsed.String()
		safe = strings.ReplaceAll(safe, "%25", "%")
	} else if parsed, err := fileservice.ParseETLService(raw); err == nil && parsed.Service != "" && len(parsed.ServiceArguments) != 0 {
		// Service arguments may contain cloud credentials. Keep only the service
		// identity and file expression; StageFilePath is never consulted here.
		safe = parsed.Service + ":" + parsed.File
		result.redacted = true
	}

	if query := strings.IndexAny(safe, "?#"); query >= 0 {
		safe = safe[:query]
		result.redacted = true
	}
	if outfileCredentialField.MatchString(safe) || strings.Contains(safe, "@") && strings.Contains(safe, ":") {
		// Unknown credential-bearing path shapes are not partially retained.
		safe = "redacted"
		result.redacted = true
	}

	pathForValidation := escapeOutfileFormatSpecifiers(safe)
	if _, err := fileservice.ParsePath(pathForValidation); err != nil {
		result.invalid = true
		safe = ""
	}
	if !utf8.ValidString(safe) {
		result.invalid = true
		safe = ""
	}
	if len(safe) > outfileAuditTargetLimit {
		cut := outfileAuditTargetLimit
		for cut > 0 && !utf8.ValidString(safe[:cut]) {
			cut--
		}
		safe = safe[:cut]
		result.truncated = true
	}
	result.target = strings.Clone(safe)
	return result
}

// escapeOutfileFormatSpecifiers protects printf-style filename templates and
// valid percent escapes from url.Parse with one percent-escape pass.
// Malformed percent escapes are left untouched so url.Parse keeps rejecting
// them rather than accepting them as literal percent bytes.
func escapeOutfileFormatSpecifiers(raw string) string {
	for i := 0; i < len(raw); {
		if raw[i] != '%' {
			i++
			continue
		}
		end := i + 1
		for end < len(raw) && raw[end] >= '0' && raw[end] <= '9' {
			end++
		}
		if end < len(raw) && raw[end] == 'd' {
			// Preserve printf templates, including %d and %05d, for the
			// existing stage export resolver.
			i = end + 1
			continue
		}
		if i+2 < len(raw) && isHexByte(raw[i+1]) && isHexByte(raw[i+2]) {
			// Existing percent-encoded URI bytes remain valid and round-trip.
			i += 3
			continue
		}
		return raw
	}
	return strings.ReplaceAll(raw, "%", "%25")
}

func isHexByte(b byte) bool {
	return b >= '0' && b <= '9' || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F'
}

func redactOutfileStatementText(stmt tree.Statement, text string) string {
	ep, ok := topLevelOutfile(stmt)
	if !ok {
		return text
	}
	safe := sanitizeOutfileTarget(ep.FilePath)
	if !safe.redacted && !safe.truncated && !safe.invalid {
		return text
	}
	rendered := tree.String(stmt, dialect.MYSQL)
	if ep.FilePath != "" && strings.Contains(rendered, ep.FilePath) {
		return strings.ReplaceAll(rendered, ep.FilePath, safe.target)
	}
	if ep.FilePath != "" && strings.Contains(text, ep.FilePath) {
		return strings.ReplaceAll(text, ep.FilePath, safe.target)
	}
	return "SELECT ... INTO OUTFILE " + safe.target
}
