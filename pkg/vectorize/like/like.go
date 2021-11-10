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
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"regexp"
	"strings"
	"unicode/utf8"
	"unsafe"
)

var (
	SliceLikePure  func(*types.Bytes, []byte, []int64) ([]int64, error)
	SliceLikeSlice func(*types.Bytes, *types.Bytes, []int64) ([]int64, error)
	PureLikeSlice  func([]byte, *types.Bytes, []int64) ([]int64, error)
	PureLikePure   func([]byte, []byte, []int64) ([]int64, error)
)

var _ = SliceLikePure
var _ = SliceLikeSlice
var _ = PureLikeSlice
var _ = PureLikePure

func init() {
	SliceLikePure = sliceLikePure
	SliceLikeSlice = sliceLikeSlice
	PureLikeSlice = pureLikeSlice
	PureLikePure = pureLikePure
}

func sliceLikePure(s *types.Bytes, expr []byte, rs []int64) ([]int64, error) {
	n := uint32(len(expr))
	if n == 0 {
		count := 0
		for i, m := range s.Lengths {
			if m == 0 {
				rs[count] = int64(i)
				count++
			}
		}
		return rs[:count], nil
	}
	if n == 1 && expr[0] == '%' {
		count := 0
		for i := range s.Lengths {
			rs[count] = int64(i)
			count++
		}
		return rs[:count], nil
	}
	if n == 1 && expr[0] == '_' {
		count := 0
		for i, m := range s.Lengths {
			if m == 1 {
				rs[count] = int64(i)
				count++
			}
		}
		return rs[:count], nil
	}
	if n > 1 && !bytes.ContainsAny(expr[1:len(expr)-1], "_%") {
		c0 := expr[0]   // first character
		c1 := expr[n-1] // last character
		switch {
		case !(c0 == '%' || c0 == '_') && !(c1 == '%' || c1 == '_'):
			count := 0
			for i, o := range s.Offsets {
				if s.Lengths[i] == n && bytes.Compare(expr, s.Data[o:o+s.Lengths[i]]) == 0 {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c0 == '_' && !(c1 == '%' || c1 == '_'):
			suffix := expr[1:]
			count := 0
			for i, o := range s.Offsets {
				if s.Lengths[i] == n && bytes.Compare(suffix, s.Data[o+1:o+s.Lengths[i]]) == 0 {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c0 == '%' && !(c1 == '%' || c1 == '_'):
			suffix := expr[1:]
			count := 0
			for i, o := range s.Offsets {
				if bytes.HasSuffix(s.Data[o:o+s.Lengths[i]], suffix) {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c1 == '_' && !(c0 == '%' || c0 == '_'):
			prefix := expr[:n-1]
			count := 0
			for i, o := range s.Offsets {
				if s.Lengths[i] == n && bytes.Compare(prefix, s.Data[o:o+s.Lengths[i]-1]) == 0 {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c1 == '%' && !(c0 == '%' || c0 == '_'):
			prefix := expr[:n-1]
			count := 0
			for i, o := range s.Offsets {
				if s.Lengths[i] >= n && bytes.Compare(s.Data[o:o+uint32(n-1)], prefix) == 0 {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c0 == '%' && c1 == '%':
			substr := expr[1 : n-1]
			count := 0
			for i, o := range s.Offsets {
				if bytes.Contains(s.Data[o:o+s.Lengths[i]], substr) {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c0 == '%' && c1 == '_':
			suffix := expr[1 : n-1]
			count := 0
			for i, o := range s.Offsets {
				if s.Lengths[i] > 0 && bytes.HasSuffix(s.Data[o:o+s.Lengths[i]-1], suffix) {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c0 == '_' && c1 == '%':
			prefix := expr[1 : n-1]
			count := 0
			for i, o := range s.Offsets {
				if s.Lengths[i] > 0 && bytes.HasPrefix(s.Data[o+1:o+s.Lengths[i]], prefix) {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		}
	}
	reg, err := regexp.Compile(convert(expr))
	if err != nil {
		return nil, err
	}
	count := 0
	for i, o := range s.Offsets {
		if reg.Match(s.Data[o : o+s.Lengths[i]]) {
			rs[count] = int64(i)
			count++
		}
	}
	return rs[:count], nil
}

func sliceLikeSlice(s *types.Bytes, exprs *types.Bytes, rs []int64) ([]int64, error) {
	count := 0
	n := len(s.Data)
	if n != len(exprs.Data) {
		return nil, errors.New("unexpected error when LIKE operator")
	}
	for i, o1 := range s.Offsets {
		o2 := exprs.Offsets[i]
		k, err := pureLikePure(s.Data[o1 : o1 + s.Lengths[i]], exprs.Data[o2 : o2 + exprs.Lengths[i]], make([]int64, 1))
		if err != nil {
			return nil, err
		}
		if k != nil {
			rs[count] = i
			count++
		}
	}
	return rs[:count], nil
}

func pureLikeSlice(p []byte, exprs *types.Bytes, rs []int64) ([]int64, error) {
	count := 0
	for i, o := range exprs.Offsets {
		k, err := pureLikePure(p, exprs.Data[o : o + exprs.Lengths[i]], make([]int64, 1))
		if err != nil {
			return nil, err
		}
		if k != nil {
			rs[count] = i
			count++
		}
	}
	return rs[:count], nil
}

func pureLikePure(p []byte, expr []byte, rs []int64) ([]int64, error) {
	n := len(expr)
	if n == 0 {
		count := 0
		if len(p) == 0 {
			rs[0] = int64(0)
			count++
		}
		return rs[:count], nil
	}
	if n == 1 && expr[0] == '%' {
		rs[0] = int64(0)
		return rs[:1], nil
	}
	if n == 1 && expr[0] == '_' {
		count := 0
		if len(p) == 1 {
			rs[0] = int64(0)
			count++
		}
		return rs[:1], nil
	}
	if n > 1 && !bytes.ContainsAny(expr[1:n-1], "_%") {
		c0 := expr[0]   // first character
		c1 := expr[n-1] // last character
		switch {
		case !(c0 == '%' || c0 == '_') && !(c1 == '%' || c1 == '_'):
			count := 0
			if len(p) == n && bytes.Compare(expr, p) == 0 {
				rs[count] = int64(0)
				count++
			}
			return rs[:count], nil
		case c0 == '_' && !(c1 == '%' || c1 == '_'):
			suffix := expr[1:]
			count := 0
			if len(p) == n && bytes.Compare(suffix, p[1:]) == 0 {
				rs[count] = int64(0)
				count++
			}
			return rs[:count], nil
		case c0 == '%' && !(c1 == '%' || c1 == '_'):
			suffix := expr[1:]
			count := 0
			if bytes.HasSuffix(p, suffix) {
				rs[count] = int64(0)
				count++
			}
			return rs[:count], nil
		case c1 == '_' && !(c0 == '%' || c0 == '_'):
			prefix := expr[:n-1]
			count := 0
			if len(p) == n && bytes.Compare(prefix, p[:n-1]) == 0 {
				rs[count] = int64(0)
				count++
			}
			return rs[:count], nil
		case c1 == '%' && !(c0 == '%' || c0 == '_'):
			prefix := expr[:n-1]
			count := 0
			if len(p) >= n && bytes.Compare(p[:n-1], prefix) == 0 {
				rs[count] = int64(0)
				count++
			}
			return rs[:count], nil
		case c0 == '%' && c1 == '%':
			substr := expr[1 : n-1]
			count := 0
			if bytes.Contains(p, substr) {
				rs[count] = int64(0)
				count++
			}
			return rs[:count], nil
		case c0 == '%' && c1 == '_':
			suffix := expr[1 : n-1]
			count := 0
			if len(p) > 0 && bytes.HasSuffix(p[:len(p)-1], suffix) {
				rs[count] = int64(0)
				count++
			}
			return rs[:count], nil
		case c0 == '_' && c1 == '%':
			prefix := expr[1 : n-1]
			count := 0
			if len(p) > 0 && bytes.HasPrefix(p[1:], prefix) {
				rs[count] = int64(0)
				count++
			}
			return rs[:count], nil
		}
	}
	reg, err := regexp.Compile(convert(expr))
	if err != nil {
		return nil, err
	}
	count := 0
	if reg.Match(p) {
		rs[count] = int64(0)
		count++
	}
	return rs[:count], nil
}

func convert(expr []byte) string {
	return fmt.Sprintf("^(?s:%s)$", replace(*(*string)(unsafe.Pointer(&expr))))
}

func replace(s string) string {
	var oc rune

	r := make([]byte, len(s)+strings.Count(s, `%`))
	w := 0
	start := 0
	for len(s) > start {
		c, wid := utf8.DecodeRuneInString(s[start:])
		if oc == '\\' {
			w += copy(r[w:], s[start:start+wid])
			start += wid
			oc = 0
			continue
		}
		switch c {
		case '_':
			w += copy(r[w:], []byte{'*'})
		case '%':
			w += copy(r[w:], []byte{'.', '*'})
		case '\\':
		default:
			w += copy(r[w:], s[start:start+wid])
		}
		start += wid
		oc = c
	}
	return string(r)
}

