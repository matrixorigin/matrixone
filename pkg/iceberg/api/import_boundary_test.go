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

package api

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type importBoundaryPolicy struct {
	name               string
	importPrefixes     []string
	allowedDirectories []string
	allowedFiles       []string
}

var importBoundaryPolicies = []importBoundaryPolicy{
	{
		name:           "Iceberg",
		importPrefixes: []string{"github.com/apache/iceberg-go"},
		allowedDirectories: []string{
			"pkg/iceberg/adapter/iceberggo",
		},
	},
	{
		name: "Arrow",
		importPrefixes: []string{
			"github.com/apache/arrow-go",
			"github.com/apache/arrow/go",
		},
		allowedDirectories: []string{
			"pkg/iceberg/adapter/iceberggo",
			"pkg/container/arrowbridge",
			"pkg/sql/colexec/external/arrowio",
			"pkg/udf/python",
		},
		allowedFiles: []string{
			"pkg/sql/colexec/external/reader_arrow.go",
			"pkg/sql/compile/compile.go",
		},
	},
}

func TestIcebergAndArrowImportBoundaries(t *testing.T) {
	root := findRepoRoot(t)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".proto-vendor", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if parseErr != nil {
			return parseErr
		}
		relativePath, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		slashPath := filepath.ToSlash(relativePath)
		for _, imp := range file.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			for _, policy := range importBoundaryPolicies {
				if importMatchesPrefix(importPath, policy.importPrefixes) &&
					!policy.allows(slashPath) {
					t.Fatalf("forbidden %s import outside an approved owner: %s imports %s",
						policy.name, slashPath, importPath)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan imports: %v", err)
	}
}

func (p importBoundaryPolicy) allows(relativePath string) bool {
	for _, directory := range p.allowedDirectories {
		if relativePath == directory || strings.HasPrefix(relativePath, directory+"/") {
			return true
		}
	}
	for _, file := range p.allowedFiles {
		if relativePath == file {
			return true
		}
	}
	return false
}

func importMatchesPrefix(importPath string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if importPath == prefix || strings.HasPrefix(importPath, prefix+"/") {
			return true
		}
	}
	return false
}

func TestImportBoundaryPolicies(t *testing.T) {
	tests := []struct {
		name         string
		policy       importBoundaryPolicy
		relativePath string
		importPath   string
		allowed      bool
	}{
		{
			name:         "UDF owns Arrow but not Iceberg",
			policy:       importBoundaryPolicies[1],
			relativePath: "pkg/udf/python/gateway.go",
			importPath:   "github.com/apache/arrow-go/v18/arrow/flight/gen/flight",
			allowed:      true,
		},
		{
			name:         "UDF cannot own Iceberg",
			policy:       importBoundaryPolicies[0],
			relativePath: "pkg/udf/python/gateway.go",
			importPath:   "github.com/apache/iceberg-go/io",
			allowed:      false,
		},
		{
			name:         "Iceberg adapter owns Iceberg",
			policy:       importBoundaryPolicies[0],
			relativePath: "pkg/iceberg/adapter/iceberggo/io.go",
			importPath:   "github.com/apache/iceberg-go/io",
			allowed:      true,
		},
		{
			name:         "sibling package is not an Arrow owner",
			policy:       importBoundaryPolicies[1],
			relativePath: "pkg/udf/pythonish/gateway.go",
			importPath:   "github.com/apache/arrow-go/v18/arrow",
			allowed:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := importMatchesPrefix(tt.importPath, tt.policy.importPrefixes)
			if matches && tt.policy.allows(tt.relativePath) != tt.allowed {
				t.Fatalf("policy allowed=%v for %s importing %s, want %v",
					tt.policy.allows(tt.relativePath), tt.relativePath, tt.importPath, tt.allowed)
			}
		})
	}
}

func TestIcebergPackagesUseMOErrorAndCauseAwareTimeouts(t *testing.T) {
	root := findRepoRoot(t)
	bareFmtErrorf := "fmt." + "Errorf("
	bareErrorsNew := "errors." + "New("
	plainTimeout := "context." + "WithTimeout("
	for _, rel := range []string{"pkg/iceberg", "pkg/sql/iceberg"} {
		dir := filepath.Join(root, rel)
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			text := string(data)
			if strings.Contains(text, bareFmtErrorf) || strings.Contains(text, bareErrorsNew) {
				t.Fatalf("use moerr instead of bare error constructors in %s", path)
			}
			if strings.Contains(text, plainTimeout) {
				t.Fatalf("use context.WithTimeoutCause in %s", path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", rel, err)
		}
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get wd: %v", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("go.mod not found from %s", dir)
		}
		dir = parent
	}
}
