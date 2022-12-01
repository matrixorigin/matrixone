// Copyright 2021 Matrix Origin
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

package like

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

const (
	DEFAULT_ESCAPE_CHAR = '\\'
)

// <source column> like 'rule'
// XXX: rs here is the selection list.
func BtSliceAndConst(xs []string, expr []byte, rs []bool) ([]bool, error) {
	return BtSliceNullAndConst(xs, expr, nil, rs)
}

func isNotNull(n *nulls.Nulls, i uint64) bool {
	if n == nil {
		return true
	}
	return !n.Contains(i)
}

func removeEscapeChar(src []byte, escapeChar byte) []byte {
	var target []byte
	max := len(src)
	for i := 0; i < max; i++ {
		if src[i] == escapeChar && i+1 < max {
			i = i + 1
		}
		target = append(target, src[i])
	}
	return target
}

func BtSliceNullAndConst(xs []string, expr []byte, ns *nulls.Nulls, rs []bool) ([]bool, error) {
	// Opt Rule #1: if expr is empty string, only empty string like empty string.
	n := uint32(len(expr))
	if n == 0 {
		for i, s := range xs {
			rs[i] = isNotNull(ns, uint64(i)) && len(s) == 0
		}
		return rs, nil
	}

	// Opt Rule #2: anything matches %
	if n == 1 && expr[0] == '%' {
		for i := range xs {
			rs[i] = isNotNull(ns, uint64(i))
		}
		return rs, nil
	}

	// Opt Rule #3: single char matches _.
	// XXX in UTF8 world, should we do single RUNE matches _?
	if n == 1 && expr[0] == '_' {
		for i, s := range xs {
			rs[i] = isNotNull(ns, uint64(i)) && len(s) == 1
		}
		return rs, nil
	}

	// Opt Rule #3.1: single char, no wild card, so it is a simple compare eq.
	if n == 1 && expr[0] != '_' && expr[0] != '%' {
		for i, s := range xs {
			rs[i] = isNotNull(ns, uint64(i)) && len(s) == 1 && s[0] == expr[0]
		}
		return rs, nil
	}

	// Opt Rule #4.  [_%]somethingInBetween[_%]
	if n > 1 && !bytes.ContainsAny(expr[1:len(expr)-1], "_%") {
		c0 := expr[0]   // first character
		c1 := expr[n-1] // last character
		if n > 2 && expr[n-2] == DEFAULT_ESCAPE_CHAR {
			c1 = DEFAULT_ESCAPE_CHAR
		}
		switch {
		case !(c0 == '%' || c0 == '_') && !(c1 == '%' || c1 == '_'):
			// Rule 4.1: no wild card, so it is a simple compare eq.
			for i, s := range xs {
				rs[i] = isNotNull(ns, uint64(i)) && uint32(len(s)) == n && bytes.Equal(expr, []byte(s))
			}
			return rs, nil
		case c0 == '_' && !(c1 == '%' || c1 == '_'):
			// Rule 4.2: _foobarzoo,
			for i, s := range xs {
				rs[i] = isNotNull(ns, uint64(i)) && uint32(len(s)) == n && bytes.Equal(expr[1:], []byte(s)[1:])
			}
			return rs, nil
		case c0 == '%' && !(c1 == '%' || c1 == '_'):
			// Rule 4.3, %foobarzoo, it turns into a suffix match.
			suffix := removeEscapeChar(expr[1:], DEFAULT_ESCAPE_CHAR)
			for i, s := range xs {
				rs[i] = isNotNull(ns, uint64(i)) && bytes.HasSuffix([]byte(s), suffix)
			}
			return rs, nil
		case c1 == '_' && !(c0 == '%' || c0 == '_'):
			// Rule 4.4, foobarzoo_, it turns into eq ingoring last char.
			prefix := removeEscapeChar(expr[:n-1], DEFAULT_ESCAPE_CHAR)
			for i, s := range xs {
				rs[i] = isNotNull(ns, uint64(i)) && uint32(len(s)) == n && bytes.Equal(prefix, []byte(s)[:n-1])
			}
			return rs, nil
		case c1 == '%' && !(c0 == '%' || c0 == '_'):
			// Rule 4.5 foobarzoo%, prefix match
			prefix := removeEscapeChar(expr[:n-1], DEFAULT_ESCAPE_CHAR)
			for i, s := range xs {
				rs[i] = isNotNull(ns, uint64(i)) && bytes.HasPrefix([]byte(s), prefix)
			}
			return rs, nil
		case c0 == '%' && c1 == '%':
			// Rule 4.6 %foobarzoo%, now it is contains
			substr := removeEscapeChar(expr[1:n-1], DEFAULT_ESCAPE_CHAR)
			for i, s := range xs {
				rs[i] = isNotNull(ns, uint64(i)) && bytes.Contains([]byte(s), substr)
			}
			return rs, nil
		case c0 == '%' && c1 == '_':
			// Rule 4.7 %foobarzoo_,
			suffix := removeEscapeChar(expr[1:n-1], DEFAULT_ESCAPE_CHAR)
			for i, s := range xs {
				bs := []byte(s)
				rs[i] = isNotNull(ns, uint64(i)) && len(s) > 0 && bytes.HasSuffix(bs[:len(bs)-1], suffix)
			}
			return rs, nil
		case c0 == '_' && c1 == '%':
			// Rule 4.8 _foobarzoo%
			prefix := removeEscapeChar(expr[1:n-1], DEFAULT_ESCAPE_CHAR)
			for i, s := range xs {
				rs[i] = isNotNull(ns, uint64(i)) && len(s) > 0 && bytes.HasPrefix([]byte(s)[1:], prefix)
			}
			return rs, nil
		}
	}

	// Done opt rules, fall back to regexp
	reg, err := regexp.Compile(convert(expr))
	if err != nil {
		return nil, err
	}
	for i, s := range xs {
		rs[i] = isNotNull(ns, uint64(i)) && reg.Match([]byte(s))
	}
	return rs, nil
}

// 'source' like 'rule'
func BtConstAndConst(s string, expr []byte) (bool, error) {
	ss := []string{s}
	rs := []bool{false}
	rs, err := BtSliceAndConst(ss, expr, rs)
	if err != nil {
		return false, err
	}
	return rs[0], nil
}

// <source column> like <rule column>
func BtSliceAndSlice(xs []string, exprs [][]byte, rs []bool) ([]bool, error) {
	if len(xs) != len(exprs) {
		return nil, moerr.NewInternalErrorNoCtx("unexpected error when LIKE operator")
	}

	for i := range xs {
		isLike, err := BtConstAndConst(xs[i], exprs[i])
		if err != nil {
			return nil, err
		}
		rs[i] = isLike
	}
	return rs, nil
}

// 'source' like <rule column>
func BtConstAndSliceNull(p string, exprs [][]byte, ns *nulls.Nulls, rs []bool) ([]bool, error) {
	for i, ex := range exprs {
		rs[i] = false
		if isNotNull(ns, uint64(i)) {
			k, err := BtConstAndConst(p, ex)
			if err != nil {
				return nil, err
			}
			rs[i] = k
		}
	}
	return rs, nil
}

// <source column may contains null> like
func BtSliceNullAndSliceNull(xs []string, exprs [][]byte, ns *nulls.Nulls, rs []bool) ([]bool, error) {
	for i := range xs {
		rs[i] = false
		if isNotNull(ns, uint64(i)) {
			k, err := BtConstAndConst(xs[i], exprs[i])
			if err != nil {
				return nil, err
			}
			rs[i] = k
		}
	}
	return rs, nil
}

func convert(expr []byte) string {
	return fmt.Sprintf("^(?s:%s)$", replace(*(*string)(unsafe.Pointer(&expr))))
}

func replace(s string) string {
	var oldCharactor rune

	r := make([]byte, len(s)+strings.Count(s, `%`))
	w := 0
	start := 0
	for len(s) > start {
		character, wid := utf8.DecodeRuneInString(s[start:])
		if oldCharactor == '\\' {
			w += copy(r[w:], s[start:start+wid])
			start += wid
			oldCharactor = 0
			continue
		}
		switch character {
		case '_':
			w += copy(r[w:], []byte{'.'})
		case '%':
			w += copy(r[w:], []byte{'.', '*'})
		case '\\':
		default:
			w += copy(r[w:], s[start:start+wid])
		}
		start += wid
		oldCharactor = character
	}
	return string(r[:w])
}
