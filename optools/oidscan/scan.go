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

package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
)

// Switch is one switch statement that dispatches on a type id: at least one of
// its case expressions names a T_* constant.
type Switch struct {
	File string // slash-separated, relative to the scan root
	Line int
	Func string // enclosing function or method declaration

	// Oids is the number of distinct T_* constants its cases name.
	Oids       int
	HasDefault bool

	// Loops are the for/range statements around the switch inside the same
	// function body, innermost first, as one-line source headers. Loops
	// outside a function literal that holds the switch are not included.
	Loops []string

	// Invariant reports that the switch is inside a loop and its tag (for a
	// tagless switch, its case expressions) uses no variable that the
	// innermost loop declares or assigns, so the switch can move out of the
	// loop. It is a syntactic check: a call in the tag is assumed to return
	// the same result on every iteration.
	Invariant bool
}

// Scan parses every non-test .go file under root/pkg (skipping testdata and
// vendor directories) and returns its type-id switches, ordered by file and
// line.
func Scan(root string) ([]Switch, error) {
	var out []Switch
	fset := token.NewFileSet()
	base := filepath.Join(root, "pkg")
	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == "testdata" || n == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out = append(out, scanFile(fset, filepath.ToSlash(rel), f)...)
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		return out[i].Line < out[j].Line
	})
	return out, err
}

func scanFile(fset *token.FileSet, rel string, f *ast.File) []Switch {
	var out []Switch
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		var stack []ast.Node
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			stack = append(stack, n)
			sw, ok := n.(*ast.SwitchStmt)
			if !ok {
				return true
			}
			oids, hasDefault := caseOids(sw)
			if oids == 0 {
				return true
			}
			s := Switch{
				File:       rel,
				Line:       fset.Position(sw.Pos()).Line,
				Func:       fd.Name.Name,
				Oids:       oids,
				HasDefault: hasDefault,
			}
			var innermost ast.Node
		outer:
			for i := len(stack) - 2; i >= 0; i-- {
				switch l := stack[i].(type) {
				case *ast.FuncLit:
					break outer
				case *ast.ForStmt, *ast.RangeStmt:
					if innermost == nil {
						innermost = l
					}
					s.Loops = append(s.Loops, loopHeader(fset, l))
				}
			}
			if innermost != nil {
				s.Invariant = !usesAny(switchSubject(sw), loopAssigned(innermost, sw))
			}
			out = append(out, s)
			return true
		})
	}
	return out
}

// caseOids counts the distinct T_* names in sw's case expressions.
func caseOids(sw *ast.SwitchStmt) (n int, hasDefault bool) {
	seen := map[string]bool{}
	for _, st := range sw.Body.List {
		cc := st.(*ast.CaseClause)
		if cc.List == nil {
			hasDefault = true
		}
		for _, e := range cc.List {
			ast.Inspect(e, func(m ast.Node) bool {
				var name string
				switch x := m.(type) {
				case *ast.SelectorExpr:
					name = x.Sel.Name
				case *ast.Ident:
					name = x.Name
				}
				if strings.HasPrefix(name, "T_") {
					seen[name] = true
				}
				return true
			})
		}
	}
	return len(seen), hasDefault
}

// switchSubject is what decides the branch: the tag, plus the init statement,
// or the case expressions of a tagless switch.
func switchSubject(sw *ast.SwitchStmt) []ast.Node {
	var nodes []ast.Node
	if sw.Init != nil {
		nodes = append(nodes, sw.Init)
	}
	if sw.Tag != nil {
		return append(nodes, sw.Tag)
	}
	for _, st := range sw.Body.List {
		for _, e := range st.(*ast.CaseClause).List {
			nodes = append(nodes, e)
		}
	}
	return nodes
}

// loopAssigned returns the names a loop declares or assigns: its range
// variables, init and post statements, and every assignment, declaration and
// ++/-- in its body. The init statement of sw, which runs on every evaluation
// of sw, is skipped; other assignments inside sw are included, which can only
// make the result more conservative.
func loopAssigned(loop ast.Node, sw *ast.SwitchStmt) map[string]bool {
	names := map[string]bool{}
	addLHS := func(e ast.Expr) {
		// x, x.f, x[i] and *x all change what x reaches
		for {
			switch v := e.(type) {
			case *ast.Ident:
				names[v.Name] = true
				return
			case *ast.SelectorExpr:
				e = v.X
			case *ast.IndexExpr:
				e = v.X
			case *ast.StarExpr:
				e = v.X
			case *ast.ParenExpr:
				e = v.X
			default:
				return
			}
		}
	}
	if r, ok := loop.(*ast.RangeStmt); ok {
		if r.Key != nil {
			addLHS(r.Key)
		}
		if r.Value != nil {
			addLHS(r.Value)
		}
	}
	ast.Inspect(loop, func(n ast.Node) bool {
		if n != nil && n == sw.Init {
			return false
		}
		switch x := n.(type) {
		case *ast.AssignStmt:
			for _, l := range x.Lhs {
				addLHS(l)
			}
		case *ast.IncDecStmt:
			addLHS(x.X)
		case *ast.ValueSpec:
			for _, id := range x.Names {
				names[id.Name] = true
			}
		case *ast.RangeStmt:
			if x.Key != nil {
				addLHS(x.Key)
			}
			if x.Value != nil {
				addLHS(x.Value)
			}
		}
		return true
	})
	return names
}

func usesAny(nodes []ast.Node, names map[string]bool) bool {
	found := false
	for _, n := range nodes {
		ast.Inspect(n, func(m ast.Node) bool {
			if id, ok := m.(*ast.Ident); ok && names[id.Name] {
				found = true
			}
			return !found
		})
		if found {
			return true
		}
	}
	return false
}

func loopHeader(fset *token.FileSet, n ast.Node) string {
	switch l := n.(type) {
	case *ast.RangeStmt:
		return "range " + oneLine(fset, l.X)
	case *ast.ForStmt:
		if l.Cond == nil {
			return "for"
		}
		return "for " + oneLine(fset, l.Cond)
	}
	return ""
}

func oneLine(fset *token.FileSet, n ast.Node) string {
	var b bytes.Buffer
	_ = printer.Fprint(&b, fset, n)
	s := strings.Join(strings.Fields(b.String()), " ")
	if len(s) > 80 {
		s = s[:77] + "..."
	}
	return s
}
