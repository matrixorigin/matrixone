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

func TestIcebergAdapterImportBoundary(t *testing.T) {
	root := findRepoRoot(t)
	forbidden := []string{
		"github.com/apache/iceberg-go",
		"github.com/apache/arrow-go",
		"github.com/apache/arrow/go",
	}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if parseErr != nil {
			return parseErr
		}
		slashPath := filepath.ToSlash(path)
		adapterAllowed := strings.Contains(slashPath, "/pkg/iceberg/adapter/iceberggo/")
		for _, imp := range file.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			for _, prefix := range forbidden {
				if strings.HasPrefix(importPath, prefix) && !adapterAllowed {
					t.Fatalf("forbidden Iceberg/Arrow import outside adapter: %s imports %s", slashPath, importPath)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan imports: %v", err)
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
