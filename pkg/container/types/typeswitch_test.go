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
	// The families below are the traits table's: a type is a member when its
	// row has the family's flag (traits_table.go).
	{"decimal", oidHas(T.IsDecimal), nil},
	{"temporal", oidHas(T.IsDateRelate), nil},
}

// oidByName maps the name of each T constant with an OidString to its value.
var oidByName = func() map[string]T {
	m := map[string]T{}
	for _, oid := range AllOids() {
		if name := oid.OidString(); name != "unknown_type" {
			m[name] = oid
		}
	}
	return m
}()

// oidHas makes a family predicate over constant names from a predicate over T.
func oidHas(pred func(T) bool) func(string) bool {
	return func(n string) bool {
		oid, ok := oidByName[n]
		return ok && pred(oid)
	}
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
	problems := baselineProblems(found, readTypeSwitchBaseline(t, baselinePath))
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
		// check reports the construct at pos when it names part of a family; a chain or a
		// list must name at least minNamed members to count
		check := func(pos token.Pos, kind string, names map[string]bool, minNamed int) {
			line := fset.Position(pos).Line
			for _, fam := range typeSwitchFamilies {
				var named, missing []string
				for _, m := range families[fam.name] {
					if names[m] {
						named = append(named, m)
					} else {
						missing = append(missing, m)
					}
				}
				if len(named) < minNamed || len(missing) == 0 || namesWholeSubfamily(fam.subfamilies, families[fam.name], names) {
					continue
				}
				if reason, ok := tagOf(line); ok {
					if reason == "" {
						bare = append(bare, rel+":"+strconv.Itoa(line))
					}
					continue
				}
				family := fam.name
				if kind != "switch" {
					family += " " + kind
				}
				key := rel + "\t" + fn + "\t" + family
				found[key] = append(found[key], fmt.Sprintf("%s:%d %s: %s %s misses %s",
					rel, line, fn, fam.name, kind, strings.Join(missing, ", ")))
			}
		}
		inChain := map[ast.Node]bool{}
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.FuncDecl:
				fn = x.Name.Name
			case *ast.SwitchStmt:
				names := map[string]bool{}
				for _, st := range x.Body.List {
					for _, e := range st.(*ast.CaseClause).List {
						collectTypeNames(e, names)
					}
				}
				check(x.Pos(), "switch", names, 1)
			case *ast.BinaryExpr:
				if (x.Op == token.LOR || x.Op == token.LAND) && !inChain[x] {
					if names, ok := typeComparisonChain(x, families, inChain); ok {
						check(x.Pos(), "chain", names, 2)
					}
				}
			case *ast.CompositeLit:
				if isTypeListLiteral(x.Type) {
					names := map[string]bool{}
					for _, e := range x.Elts {
						if kv, ok := e.(*ast.KeyValueExpr); ok {
							e = kv.Key
						}
						collectTypeNames(e, names)
					}
					check(x.Pos(), "list", names, 2)
				}
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)
	return found, bare
}

func collectTypeNames(e ast.Node, names map[string]bool) {
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

// typeComparisonChain returns the type constants a || chain of == comparisons (or a && chain
// of != comparisons) names; IsFloat, IsLowPrecisionFloat, IsArray and IsArrayRelate calls in
// the chain name their members. ok is false when no leaf is such a comparison. The nested
// binary expressions of the chain are marked in seen.
func typeComparisonChain(e *ast.BinaryExpr, families map[string][]string, seen map[ast.Node]bool) (map[string]bool, bool) {
	cmp := token.EQL
	if e.Op == token.LAND {
		cmp = token.NEQ
	}
	names := map[string]bool{}
	found := false
	var walk func(n ast.Expr)
	walk = func(n ast.Expr) {
		for {
			p, ok := n.(*ast.ParenExpr)
			if !ok {
				break
			}
			n = p.X
		}
		switch x := n.(type) {
		case *ast.BinaryExpr:
			if x.Op == e.Op {
				seen[x] = true
				walk(x.X)
				walk(x.Y)
				return
			}
			if x.Op == cmp {
				before := len(names)
				collectTypeNames(x, names)
				found = found || len(names) > before
			}
		case *ast.CallExpr:
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok {
				return
			}
			switch sel.Sel.Name {
			case "IsFloat":
				for _, m := range families["float"] {
					names[m] = true
				}
			case "IsLowPrecisionFloat":
				for _, m := range families["float"] {
					if m != "T_float32" && m != "T_float64" {
						names[m] = true
					}
				}
			case "IsArray", "IsArrayRelate":
				for _, m := range families["vector"] {
					names[m] = true
				}
			case "IsDecimal":
				for _, m := range families["decimal"] {
					names[m] = true
				}
			case "IsDateRelate":
				for _, m := range families["temporal"] {
					names[m] = true
				}
			}
		}
	}
	walk(e)
	return names, found
}

// isTypeListLiteral reports whether t is []T, [N]T or map[T]V, with T this package's type or
// types.T.
func isTypeListLiteral(t ast.Expr) bool {
	isT := func(e ast.Expr) bool {
		switch x := e.(type) {
		case *ast.Ident:
			return x.Name == "T"
		case *ast.SelectorExpr:
			id, ok := x.X.(*ast.Ident)
			return ok && id.Name == "types" && x.Sel.Name == "T"
		}
		return false
	}
	switch x := t.(type) {
	case *ast.ArrayType:
		return isT(x.Elt)
	case *ast.MapType:
		return isT(x.Key)
	}
	return false
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

// baselineProblems returns the sites found that the baseline does not
// allow: more untagged partial switches under a key than it lists, or a
// listed switch that misses a member its entry does not list (so a type
// added to a family is flagged even in a switch that predates the check).
func baselineProblems(found map[string][]string, baseline map[string]baselineEntry) []string {
	var problems []string
	for key, sites := range found {
		entry := baseline[key]
		if len(sites) > entry.count {
			problems = append(problems, sites...)
			continue
		}
		for _, site := range sites {
			for _, m := range siteMisses(site) {
				if !entry.misses[m] {
					problems = append(problems, site+" (newly missing "+m+")")
				}
			}
		}
	}
	sort.Strings(problems)
	return problems
}

// TestTypeSwitchBaselineProblems checks the baseline rules on made-up sites.
func TestTypeSwitchBaselineProblems(t *testing.T) {
	key := "pkg/x/x.go\tf\tdecimal"
	site := func(misses string) string { return "pkg/x/x.go:1 f: decimal switch misses " + misses }
	baseline := map[string]baselineEntry{key: {count: 1, misses: map[string]bool{"T_decimal256": true}}}

	require.Empty(t, baselineProblems(map[string][]string{key: {site("T_decimal256")}}, baseline),
		"a listed switch missing what it lists is allowed")
	require.Empty(t, baselineProblems(map[string][]string{}, baseline), "a fixed switch is allowed")
	require.Equal(t, []string{site("T_decimal256, T_decimal512") + " (newly missing T_decimal512)"},
		baselineProblems(map[string][]string{key: {site("T_decimal256, T_decimal512")}}, baseline),
		"a type added to the family is flagged in a listed switch")
	require.Len(t, baselineProblems(map[string][]string{key: {site("T_decimal256"), site("T_decimal256")}}, baseline), 2,
		"a second partial switch under the key is flagged")
	require.Len(t, baselineProblems(map[string][]string{"pkg/y/y.go\tg\ttemporal": {site("T_year")}}, baseline), 1,
		"an unlisted key is flagged")
	require.Equal(t, []string{"T_a", "T_b"}, siteMisses("p.go:3 f: temporal chain misses T_a, T_b"))
}

// baselineEntry is one line of typeswitch_baseline.txt: how many untagged
// partial switches a file, function and family may hold, and the members they
// may miss.
type baselineEntry struct {
	count  int
	misses map[string]bool
}

// siteMisses returns the members a site reported by scanTypeSwitches misses.
func siteMisses(site string) []string {
	i := strings.LastIndex(site, " misses ")
	if i < 0 {
		return nil
	}
	return strings.Split(site[i+len(" misses "):], ", ")
}

func readTypeSwitchBaseline(t *testing.T, path string) map[string]baselineEntry {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	out := map[string]baselineEntry{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		require.Len(t, parts, 5, line)
		n, err := strconv.Atoi(parts[3])
		require.NoError(t, err, line)
		misses := map[string]bool{}
		for _, m := range strings.Split(parts[4], ",") {
			misses[m] = true
		}
		out[strings.Join(parts[:3], "\t")] = baselineEntry{count: n, misses: misses}
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
	b.WriteString("# file\tfunction\tfamily\tcount\tmembers they miss. Fix or tag an entry, then regenerate; never add one.\n")
	for _, k := range keys {
		set := map[string]bool{}
		for _, site := range found[k] {
			for _, m := range siteMisses(site) {
				set[m] = true
			}
		}
		misses := make([]string, 0, len(set))
		for m := range set {
			misses = append(misses, m)
		}
		sort.Strings(misses)
		fmt.Fprintf(&b, "%s\t%d\t%s\n", k, len(found[k]), strings.Join(misses, ","))
	}
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o644))
}
