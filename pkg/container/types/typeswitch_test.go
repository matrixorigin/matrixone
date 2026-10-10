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

package types

import (
	"bufio"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Type-switch families. A switch over T that names any member of a family must name every
// member, or every member of one of its subfamilies and no other family member, or carry a
// comment, on the switch line or in the comment block right above it:
//
//	// typeswitch:partial <reason>
//
// Members are discovered from the T constants in types.go, so a type added to a family or
// subfamily is checked everywhere a member is switched on.
type typeSwitchSubfamily struct {
	name   string
	member func(name string) bool
}

var typeSwitchFamilies = []struct {
	name        string
	member      func(name string) bool
	subfamilies []typeSwitchSubfamily
}{
	{"float", func(n string) bool {
		return strings.HasPrefix(n, "T_float") || strings.HasPrefix(n, "T_bf")
	}, []typeSwitchSubfamily{
		{"low-precision", func(n string) bool { return n != "T_float32" && n != "T_float64" }},
	}},
	{"vector", func(n string) bool { return strings.HasPrefix(n, "T_array_") }, []typeSwitchSubfamily{
		{"block-scaled", func(n string) bool { return n == "T_array_float8" || n == "T_array_float4" }},
	}},
}

const typeSwitchTag = "typeswitch:partial"

var updateTypeSwitchBaseline = flag.Bool("update-typeswitch", false,
	"rewrite testdata/typeswitch_baseline.txt with the current untagged partial switches")

// TestTypeSwitchFamilies fails on a switch in pkg/ (tests excluded) that names a member of
// a family but neither the whole family nor exactly a whole subfamily, has no
// typeswitch:partial tag, and is not in the baseline of
// switches that predate the check. The baseline is keyed by file, function and family, with
// a count; it may shrink, never grow. Regenerate it with
//
//	go test ./pkg/container/types -run TestTypeSwitchFamilies -args -update-typeswitch
func TestTypeSwitchFamilies(t *testing.T) {
	families := typeSwitchFamilyMembers(t)
	found, untaggedBare := scanTypeSwitches(t, filepath.Join("..", "..", ".."), families)
	require.Empty(t, untaggedBare, "a typeswitch:partial tag needs a reason; see pkg/container/types/testdata/README.md")

	baselinePath := filepath.Join("testdata", "typeswitch_baseline.txt")
	if *updateTypeSwitchBaseline {
		writeTypeSwitchBaseline(t, baselinePath, found)
		return
	}
	baseline := readTypeSwitchBaseline(t, baselinePath)
	var problems []string
	for key, sites := range found {
		if len(sites) > baseline[key] {
			problems = append(problems, sites...)
		}
	}
	sort.Strings(problems)
	require.Empty(t, problems,
		"switches that name part of a type family: add the members, or tag the switch "+
			"with `// typeswitch:partial <reason>` when the omission is intended; do not add them to "+
			"the baseline. See pkg/container/types/testdata/README.md")
}

// typeSwitchFamilyMembers returns, per family, its members from the T constants of
// types.go.
func typeSwitchFamilyMembers(t *testing.T) map[string][]string {
	f, err := parser.ParseFile(token.NewFileSet(), "types.go", nil, 0)
	require.NoError(t, err)
	var consts []string
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, sp := range gd.Specs {
			vs := sp.(*ast.ValueSpec)
			if id, ok := vs.Type.(*ast.Ident); !ok || id.Name != "T" {
				continue
			}
			for _, n := range vs.Names {
				consts = append(consts, n.Name)
			}
		}
	}
	out := make(map[string][]string, len(typeSwitchFamilies))
	for _, fam := range typeSwitchFamilies {
		for _, c := range consts {
			if fam.member(c) {
				out[fam.name] = append(out[fam.name], c)
			}
		}
		require.Greater(t, len(out[fam.name]), 1, fam.name)
		for _, sub := range fam.subfamilies {
			n := 0
			for _, c := range out[fam.name] {
				if sub.member(c) {
					n++
				}
			}
			require.Greater(t, n, 1, sub.name)
		}
	}
	return out
}

// scanTypeSwitches returns the untagged partial switches under root/pkg, grouped by
// "file\tfunc\tfamily", each site as "file:line func: family switch misses ...", and the
// sites whose tag has no reason.
func scanTypeSwitches(t *testing.T, root string, families map[string][]string) (map[string][]string, []string) {
	found := map[string][]string{}
	var bare []string
	fset := token.NewFileSet()
	err := filepath.Walk(filepath.Join(root, "pkg"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		comments := map[int]string{}
		for _, cg := range f.Comments {
			for _, c := range cg.List {
				comments[fset.Position(c.Pos()).Line] = c.Text
			}
		}
		// the tag on the switch line or in the comment block right above it
		tagOf := func(line int) (string, bool) {
			for l := line; ; l-- {
				text, ok := comments[l]
				if !ok {
					if l == line {
						continue
					}
					return "", false
				}
				if i := strings.Index(text, typeSwitchTag); i >= 0 {
					return strings.TrimSpace(text[i+len(typeSwitchTag):]), true
				}
			}
		}
		var fn string
		ast.Inspect(f, func(n ast.Node) bool {
			if d, ok := n.(*ast.FuncDecl); ok {
				fn = d.Name.Name
			}
			sw, ok := n.(*ast.SwitchStmt)
			if !ok {
				return true
			}
			names := map[string]bool{}
			for _, st := range sw.Body.List {
				for _, e := range st.(*ast.CaseClause).List {
					ast.Inspect(e, func(m ast.Node) bool {
						switch x := m.(type) {
						case *ast.SelectorExpr:
							names[x.Sel.Name] = true
						case *ast.Ident:
							names[x.Name] = true
						}
						return true
					})
				}
			}
			line := fset.Position(sw.Pos()).Line
			for _, fam := range typeSwitchFamilies {
				var named, missing []string
				for _, m := range families[fam.name] {
					if names[m] {
						named = append(named, m)
					} else {
						missing = append(missing, m)
					}
				}
				if len(named) == 0 || len(missing) == 0 || namesWholeSubfamily(fam.subfamilies, families[fam.name], names) {
					continue
				}
				if reason, ok := tagOf(line); ok {
					if reason == "" {
						bare = append(bare, rel+":"+strconv.Itoa(line))
					}
					continue
				}
				key := rel + "\t" + fn + "\t" + fam.name
				found[key] = append(found[key], fmt.Sprintf("%s:%d %s: %s switch misses %s",
					rel, line, fn, fam.name, strings.Join(missing, ", ")))
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)
	return found, bare
}

// namesWholeSubfamily reports whether the switch names every member of one subfamily and no
// other member of the family.
func namesWholeSubfamily(subs []typeSwitchSubfamily, members []string, names map[string]bool) bool {
	for _, sub := range subs {
		whole := true
		for _, m := range members {
			if sub.member(m) != names[m] {
				whole = false
				break
			}
		}
		if whole {
			return true
		}
	}
	return false
}

func readTypeSwitchBaseline(t *testing.T, path string) map[string]int {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	out := map[string]int{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		require.Len(t, parts, 4, line)
		n, err := strconv.Atoi(parts[3])
		require.NoError(t, err, line)
		out[strings.Join(parts[:3], "\t")] = n
	}
	require.NoError(t, sc.Err())
	return out
}

func writeTypeSwitchBaseline(t *testing.T, path string, found map[string][]string) {
	keys := make([]string, 0, len(found))
	for k := range found {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("# Untagged partial type switches that predate TestTypeSwitchFamilies:\n")
	b.WriteString("# file\tfunction\tfamily\tcount. Fix or tag an entry, then regenerate; never add one.\n")
	for _, k := range keys {
		fmt.Fprintf(&b, "%s\t%d\n", k, len(found[k]))
	}
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
}
