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

package fileservice

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This is a bounded owner inventory, not a proof about arbitrary future code.
// The real factory separately rejects missing context and checks its own target.
func TestSQLFileAcquisitionOwners(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	allowed := map[string]map[string]bool{
		"pkg/frontend/export.go":              {"openNewFile": true, "writeParquetToFile": true},
		"pkg/frontend/table_dump.go":          {"openTableDumpFS": true},
		"pkg/frontend/data_branch_output.go":  {"prepareFSForDiffAsFile": true, "removeFileIgnoreError": true, "validateOutputDirPath": true},
		"pkg/sql/plan/utils.go":               {"GetForETLWithType": true},
		"pkg/sql/plan/function/func_unary.go": {"readFromParsedPath": true},
	}
	calls := 0
	for _, dir := range []string{"pkg/frontend", "pkg/sql"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if !bytes.Contains(data, []byte("GetForETL")) && !bytes.Contains(data, []byte("NewS3FS")) && !bytes.Contains(data, []byte("NewLocalETLFS")) {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, data, 0)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			inspect := func(node ast.Node, owner string) {
				ast.Inspect(node, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					pkg, ok := sel.X.(*ast.Ident)
					if !ok || pkg.Name != "fileservice" {
						return true
					}
					switch sel.Sel.Name {
					case "GetForETL", "GetForETLWithParsedPath":
						calls++
						require.True(t, allowed[rel][owner], "unmapped acquisition owner: %s:%s", rel, owner)
						if len(call.Args) > 0 {
							if ctxCall, ok := call.Args[0].(*ast.CallExpr); ok {
								if ctxSel, ok := ctxCall.Fun.(*ast.SelectorExpr); ok {
									if ctxPkg, ok := ctxSel.X.(*ast.Ident); ok && ctxPkg.Name == "context" {
										require.NotContains(t, []string{"TODO", "Background"}, ctxSel.Sel.Name, "SQL acquisition discarded statement context: %s:%s", rel, owner)
									}
								}
							}
						}
					case "NewS3FS", "NewLocalETLFS":
						// This existing owner opens only local dump storage after
						// openTableDumpFS has admitted a service-less target.
						if rel != "pkg/frontend/table_dump.go" || owner != "openLocalTableDump" || sel.Sel.Name != "NewLocalETLFS" {
							t.Errorf("unmapped native constructor used by SQL owner %s:%s", rel, owner)
						}
					}
					return true
				})
			}
			for _, decl := range file.Decls {
				switch decl := decl.(type) {
				case *ast.FuncDecl:
					inspect(decl, decl.Name.Name)
				case *ast.GenDecl:
					for _, spec := range decl.Specs {
						if spec, ok := spec.(*ast.ValueSpec); ok && len(spec.Names) > 0 {
							inspect(spec, spec.Names[0].Name)
						}
					}
				}
			}
			return nil
		})
		require.NoError(t, err)
	}
	require.Positive(t, calls, "owner inventory selected no acquisition calls")
}
