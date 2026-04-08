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

package planner

import (
	"bufio"
	"path"
	"regexp"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/testinfra/types"
)

// ParseUnifiedDiff parses a unified diff (as produced by `git diff`) and
// returns a DiffSummary describing the changed files.
//
// It extracts:
// - file paths from "diff --git a/... b/..." lines
// - change kind (added, deleted, modified, renamed)
// - Go package paths (derived from directory)
// - changed function names (best-effort, from @@ hunk headers)
// - total added / deleted line counts
func ParseUnifiedDiff(diffText string) *types.DiffSummary {
	summary := &types.DiffSummary{}

	scanner := bufio.NewScanner(strings.NewReader(diffText))
	var currentFile *types.FileChange

	for scanner.Scan() {
		line := scanner.Text()

		// --- detect new file in diff ---
		if strings.HasPrefix(line, "diff --git ") {
			if currentFile != nil {
				summary.Files = append(summary.Files, *currentFile)
			}
			currentFile = parseDiffHeader(line)
			continue
		}

		if currentFile == nil {
			continue
		}

		// --- detect change kind ---
		if strings.HasPrefix(line, "new file mode") {
			currentFile.ChangeKind = "added"
			continue
		}
		if strings.HasPrefix(line, "deleted file mode") {
			currentFile.ChangeKind = "deleted"
			continue
		}
		if strings.HasPrefix(line, "rename from ") || strings.HasPrefix(line, "rename to ") {
			currentFile.ChangeKind = "renamed"
			continue
		}

		// --- extract function names from hunk headers ---
		if strings.HasPrefix(line, "@@") {
			if fn := extractFuncFromHunk(line); fn != "" {
				currentFile.Functions = appendIfNew(currentFile.Functions, fn)
			}
			continue
		}

		// --- count added/deleted lines ---
		if strings.HasPrefix(line, "+") && !strings.HasPrefix(line, "+++") {
			summary.TotalAdded++
		}
		if strings.HasPrefix(line, "-") && !strings.HasPrefix(line, "---") {
			summary.TotalDeleted++
		}
	}

	if currentFile != nil {
		summary.Files = append(summary.Files, *currentFile)
	}

	return summary
}

// parseDiffHeader parses a "diff --git a/path b/path" line.
func parseDiffHeader(line string) *types.FileChange {
	// "diff --git a/pkg/sql/plan/build.go b/pkg/sql/plan/build.go"
	parts := strings.SplitN(line, " b/", 2)
	if len(parts) != 2 {
		return &types.FileChange{Path: line, ChangeKind: "modified"}
	}
	filePath := parts[1]
	fc := &types.FileChange{
		Path:       filePath,
		ChangeKind: "modified",
	}

	// derive Go package from directory
	dir := path.Dir(filePath)
	if isGoPackage(filePath) && dir != "." {
		fc.Package = dir
	}

	return fc
}

// hunkFuncRe matches the function name in a Go diff hunk header like:
// @@ -10,5 +10,6 @@ func (p *Planner) Build(...
var hunkFuncRe = regexp.MustCompile(`@@[^@]+@@\s+(?:func\s+(?:\([^)]+\)\s+)?(\w+))`)

// extractFuncFromHunk tries to extract a Go function name from a hunk header.
func extractFuncFromHunk(line string) string {
	matches := hunkFuncRe.FindStringSubmatch(line)
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}

// isGoPackage returns true if the file path looks like a Go source file.
func isGoPackage(filePath string) bool {
	return strings.HasSuffix(filePath, ".go")
}

func appendIfNew(slice []string, s string) []string {
	for _, v := range slice {
		if v == s {
			return slice
		}
	}
	return append(slice, s)
}
