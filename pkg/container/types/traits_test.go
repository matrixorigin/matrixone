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
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// declaredOids parses types.go and returns every constant declared with type
// T, by name.
func declaredOids(t *testing.T) map[string]T {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "types.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err)
	out := map[string]T{}
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, spec := range gd.Specs {
			vs := spec.(*ast.ValueSpec)
			if id, ok := vs.Type.(*ast.Ident); !ok || id.Name != "T" {
				continue
			}
			require.Len(t, vs.Values, len(vs.Names))
			for i, name := range vs.Names {
				lit, ok := vs.Values[i].(*ast.BasicLit)
				require.True(t, ok, "%s: T constants must be integer literals", name.Name)
				v, err := strconv.ParseUint(lit.Value, 0, 8)
				require.NoError(t, err, name.Name)
				out[name.Name] = T(v)
			}
		}
	}
	require.NotEmpty(t, out)
	return out
}

// TestTraitsCoverEveryOid fails when a T_ constant has no row in
// traits_table.go, or a row has no T_ constant.
func TestTraitsCoverEveryOid(t *testing.T) {
	declared := declaredOids(t)
	want := make([]T, 0, len(declared))
	for name, oid := range declared {
		require.True(t, traits[oid].defined, "%s (%d) has no row in traits_table.go", name, oid)
		want = append(want, oid)
	}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	require.Equal(t, want, AllOids(), "AllOids must be exactly the T_ constants")

	// AllOids returns a copy
	oids := AllOids()
	oids[0] = 255
	require.NotEqual(t, T(255), AllOids()[0])
}

// TestTraitsFlagFamilies ties the table's family flags to the name-based
// families TestTypeSwitchFamilies checks switches against, so a type added to
// a family (say, a new T_float* constant) must also get the family's flags.
func TestTraitsFlagFamilies(t *testing.T) {
	declared := declaredOids(t)
	inFamily := map[string]map[string]bool{}
	for fam, members := range typeSwitchFamilyMembers(t) {
		inFamily[fam] = map[string]bool{}
		for _, m := range members {
			inFamily[fam][m] = true
		}
	}
	for name, oid := range declared {
		float, vec := inFamily["float"][name], inFamily["vector"][name]
		lowPrecision := float && name != "T_float32" && name != "T_float64"
		blockScaled := name == "T_array_float8" || name == "T_array_float4"
		require.Equal(t, float, oid.IsFloat(), "%s: IsFloat", name)
		require.Equal(t, lowPrecision, oid.IsLowPrecisionFloat(), "%s: IsLowPrecisionFloat", name)
		require.Equal(t, vec, oid.IsArray(), "%s: IsArray", name)
		require.Equal(t, vec && blockScaled, oid.IsBlockScaledArray(), "%s: IsBlockScaledArray", name)
		require.Equal(t, vec && !blockScaled, oid.IsArrayRelate(), "%s: IsArrayRelate", name)
	}
}

func TestBuildTraitsRejectsDuplicateRows(t *testing.T) {
	require.Panics(t, func() {
		buildTraits([]traitRow{{T_int8, typeTraits{}}, {T_int8, typeTraits{}}})
	})
}

// outcome runs f and reports its result, or the panic value it raised.
func outcome[R any](f func() R) string {
	var s string
	func() {
		defer func() {
			if r := recover(); r != nil {
				s = fmt.Sprintf("panic(%T: %v)", r, r)
			}
		}()
		s = fmt.Sprintf("%#v", f())
	}()
	return s
}

// TestTraitsMatchLegacy checks, for every possible T value (defined or not),
// that each method now read from the table returns what its switch
// implementation returned, including panicking with the same value.
func TestTraitsMatchLegacy(t *testing.T) {
	for i := 0; i < 256; i++ {
		oid := T(i)
		cases := []struct {
			name      string
			got, want string
		}{
			{"String", outcome(oid.String), outcome(func() string { return legacyString(oid) })},
			{"OidString", outcome(oid.OidString), outcome(func() string { return legacyOidString(oid) })},
			{"TypeLen", outcome(oid.TypeLen), outcome(func() int { return legacyTypeLen(oid) })},
			{"FixedLength", outcome(oid.FixedLength), outcome(func() int { return legacyFixedLength(oid) })},
			{"IsFixedLen", outcome(oid.IsFixedLen), outcome(func() bool { return legacyIsFixedLen(oid) })},
			{"IsOrdered", outcome(oid.IsOrdered), outcome(func() bool { return legacyIsOrdered(oid) })},
			{"IsUnsignedInt", outcome(oid.IsUnsignedInt), outcome(func() bool { return legacyIsUnsignedInt(oid) })},
			{"IsSignedInt", outcome(oid.IsSignedInt), outcome(func() bool { return legacyIsSignedInt(oid) })},
			{"IsInteger", outcome(oid.IsInteger), outcome(func() bool { return legacyIsInteger(oid) })},
			{"IsFloat", outcome(oid.IsFloat), outcome(func() bool { return legacyIsFloat(oid) })},
			{"IsLowPrecisionFloat", outcome(oid.IsLowPrecisionFloat), outcome(func() bool { return legacyIsLowPrecisionFloat(oid) })},
			{"IsMySQLString", outcome(oid.IsMySQLString), outcome(func() bool { return legacyIsMySQLString(oid) })},
			{"IsDateRelate", outcome(oid.IsDateRelate), outcome(func() bool { return legacyIsDateRelate(oid) })},
			{"IsArrayRelate", outcome(oid.IsArrayRelate), outcome(func() bool { return legacyIsArrayRelate(oid) })},
			{"IsBlockScaledArray", outcome(oid.IsBlockScaledArray), outcome(func() bool { return legacyIsBlockScaledArray(oid) })},
			{"IsArray", outcome(oid.IsArray), outcome(func() bool { return legacyIsArray(oid) })},
			{"IsDecimal", outcome(oid.IsDecimal), outcome(func() bool { return legacyIsDecimal(oid) })},
			{"Type.IsInt", outcome(Type{Oid: oid}.IsInt), outcome(func() bool { return legacyTypeIsInt(Type{Oid: oid}) })},
			{"Type.IsUInt", outcome(Type{Oid: oid}.IsUInt), outcome(func() bool { return legacyTypeIsUInt(Type{Oid: oid}) })},
			{"Type.IsFloat", outcome(Type{Oid: oid}.IsFloat), outcome(func() bool { return legacyTypeIsFloat(Type{Oid: oid}) })},
			{"Type.IsDecimal", outcome(Type{Oid: oid}.IsDecimal), outcome(func() bool { return legacyTypeIsDecimal(Type{Oid: oid}) })},
			{"TryToType", outcome(func() string { typ, ok := oid.TryToType(); return fmt.Sprintf("%#v %v", typ, ok) }),
				outcome(func() string { typ, ok := legacyTryToType(oid); return fmt.Sprintf("%#v %v", typ, ok) })},
		}
		for _, c := range cases {
			require.Equal(t, c.want, c.got, "T(%d).%s", i, c.name)
		}
	}
}

var benchOids = func() []T {
	oids := AllOids()
	// include a value with no row, as callers can pass one
	return append(oids, 5)
}()

// sizedOids are the type ids FixedLength does not panic on.
var sizedOids = func() (out []T) {
	for _, oid := range AllOids() {
		if traits[oid].fixedLen.ok {
			out = append(out, oid)
		}
	}
	return
}()

var benchSink int

// BenchmarkTraitsPredicates compares the table with the switch
// implementations on the hot predicates.
func BenchmarkTraitsPredicates(b *testing.B) {
	b.Run("FixedLength/table", func(b *testing.B) {
		n := 0
		for i := 0; i < b.N; i++ {
			for _, oid := range sizedOids {
				n += oid.FixedLength()
			}
		}
		benchSink = n
	})
	b.Run("FixedLength/switch", func(b *testing.B) {
		n := 0
		for i := 0; i < b.N; i++ {
			for _, oid := range sizedOids {
				n += legacyFixedLength(oid)
			}
		}
		benchSink = n
	})
	b.Run("IsFloat/table", func(b *testing.B) {
		n := 0
		for i := 0; i < b.N; i++ {
			for _, oid := range benchOids {
				if oid.IsFloat() {
					n++
				}
			}
		}
		benchSink = n
	})
	b.Run("IsFloat/switch", func(b *testing.B) {
		n := 0
		for i := 0; i < b.N; i++ {
			for _, oid := range benchOids {
				if legacyIsFloat(oid) {
					n++
				}
			}
		}
		benchSink = n
	})
	b.Run("IsDateRelate/table", func(b *testing.B) {
		n := 0
		for i := 0; i < b.N; i++ {
			for _, oid := range benchOids {
				if oid.IsDateRelate() {
					n++
				}
			}
		}
		benchSink = n
	})
	b.Run("IsDateRelate/switch", func(b *testing.B) {
		n := 0
		for i := 0; i < b.N; i++ {
			for _, oid := range benchOids {
				if legacyIsDateRelate(oid) {
					n++
				}
			}
		}
		benchSink = n
	})
}
