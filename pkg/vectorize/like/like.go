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
	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"regexp"
	"strings"
	"unicode/utf8"
	"unsafe"
)

var (
	SliceLikePure          func(*types.Bytes, []byte, []int64) ([]int64, error)
	SliceLikeSlice         func(*types.Bytes, *types.Bytes, []int64) ([]int64, error)
	PureLikeSlice          func([]byte, *types.Bytes, []int64) ([]int64, error)
	PureLikePure           func([]byte, []byte, []int64) ([]int64, error)
	SliceNullLikePure      func(*types.Bytes, []byte, *roaring.Bitmap, []int64) ([]int64, error)
	SliceNullLikeSliceNull func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64) ([]int64, error)
	PureLikeSliceNull      func([]byte, *types.Bytes, *roaring.Bitmap, []int64) ([]int64, error)
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
	SliceNullLikePure = sliceNullLikePure
	SliceNullLikeSliceNull = sliceNullLikeSliceNull
	PureLikeSliceNull = pureLikeSliceNull
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
			for i := range s.Offsets {
				if s.Lengths[i] == n && bytes.Compare(expr, s.Get(int64(i))) == 0 {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c0 == '_' && !(c1 == '%' || c1 == '_'):
			suffix := expr[1:]
			count := 0
			for i := range s.Offsets {
				if s.Lengths[i] == n && bytes.Compare(suffix, s.Get(int64(i))) == 0 {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c0 == '%' && !(c1 == '%' || c1 == '_'):
			suffix := expr[1:]
			count := 0
			for i := range s.Offsets {
				if bytes.HasSuffix(s.Get(int64(i)), suffix) {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c1 == '_' && !(c0 == '%' || c0 == '_'):
			prefix := expr[:n-1]
			count := 0
			for i := range s.Offsets {
				if s.Lengths[i] == n && bytes.Compare(prefix, s.Get(int64(i))) == 0 {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c1 == '%' && !(c0 == '%' || c0 == '_'):
			prefix := expr[:n-1]
			count := 0
			for i := range s.Offsets {
				if s.Lengths[i] >= n-1 && bytes.HasPrefix(s.Get(int64(i)), prefix) {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c0 == '%' && c1 == '%':
			substr := expr[1 : n-1]
			count := 0
			for i := range s.Offsets {
				if bytes.Contains(s.Get(int64(i)), substr) {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c0 == '%' && c1 == '_':
			suffix := expr[1 : n-1]
			count := 0
			for i := range s.Offsets {
				if s.Lengths[i] > 0 && bytes.HasSuffix(s.Get(int64(i)), suffix) {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c0 == '_' && c1 == '%':
			prefix := expr[1 : n-1]
			count := 0
			for i := range s.Offsets {
				if s.Lengths[i] > 0 && bytes.HasPrefix(s.Get(int64(i)), prefix) {
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
	for i := range s.Offsets {
		if reg.Match(s.Get(int64(i))) {
			rs[count] = int64(i)
			count++
		}
	}
	return rs[:count], nil
}

func sliceLikeSlice(s *types.Bytes, exprs *types.Bytes, rs []int64) ([]int64, error) {
	count := 0
	tempSlice := make([]int64, 1)
	n := len(s.Lengths)
	if n != len(exprs.Lengths) {
		return nil, errors.New("unexpected error when LIKE operator")
	}
	for i := range s.Offsets {
		k, err := pureLikePure(s.Get(int64(i)), exprs.Get(int64(i)), tempSlice)
		if err != nil {
			return nil, err
		}
		if k != nil {
			rs[count] = int64(i)
			count++
		}
	}
	return rs[:count], nil
}

func pureLikeSlice(p []byte, exprs *types.Bytes, rs []int64) ([]int64, error) {
	count := 0
	tempSlice := make([]int64, 1)
	for i := range exprs.Offsets {
		k, err := pureLikePure(p, exprs.Get(int64(i)), tempSlice)
		if err != nil {
			return nil, err
		}
		if k != nil {
			rs[count] = int64(i)
			count++
		}
	}
	return rs[:count], nil
}

func pureLikePure(p []byte, expr []byte, rs []int64) ([]int64, error) {
	n := len(expr)
	if n == 0 {
		if len(p) == 0 {
			rs[0] = int64(0)
			return rs[:1], nil
		}
		return nil, nil
	}
	if n == 1 && expr[0] == '%' {
		rs[0] = int64(0)
		return rs[:1], nil
	}
	if n == 1 && expr[0] == '_' {
		if len(p) == 1 {
			rs[0] = int64(0)
			return rs[:1], nil
		}
		return nil, nil
	}
	if n > 1 && !bytes.ContainsAny(expr[1:n-1], "_%") {
		c0 := expr[0]   // first character
		c1 := expr[n-1] // last character
		switch {
		case !(c0 == '%' || c0 == '_') && !(c1 == '%' || c1 == '_'):
			if len(p) == n && bytes.Compare(expr, p) == 0 {
				rs[0] = int64(0)
				return rs[:1], nil
			}
			return nil, nil
		case c0 == '_' && !(c1 == '%' || c1 == '_'):
			suffix := expr[1:]
			if len(p) == n && bytes.Compare(suffix, p[1:]) == 0 {
				rs[0] = int64(0)
				return rs[:1], nil
			}
			return nil, nil
		case c0 == '%' && !(c1 == '%' || c1 == '_'):
			suffix := expr[1:]
			if bytes.HasSuffix(p, suffix) {
				rs[0] = int64(0)
				return rs[:1], nil
			}
			return nil, nil
		case c1 == '_' && !(c0 == '%' || c0 == '_'):
			prefix := expr[:n-1]
			if len(p) == n && bytes.Compare(prefix, p[:n-1]) == 0 {
				rs[0] = int64(0)
				return rs[:1], nil
			}
			return nil, nil
		case c1 == '%' && !(c0 == '%' || c0 == '_'):
			prefix := expr[:n-1]
			if len(p) >= n-1 && bytes.HasPrefix(p[:n-1], prefix) {
				rs[0] = int64(0)
				return rs[:1], nil
			}
			return nil, nil
		case c0 == '%' && c1 == '%':
			substr := expr[1 : n-1]
			if bytes.Contains(p, substr) {
				rs[0] = int64(0)
				return rs[:1], nil
			}
			return nil, nil
		case c0 == '%' && c1 == '_':
			suffix := expr[1 : n-1]
			if len(p) > 0 && bytes.HasSuffix(p[:len(p)-1], suffix) {
				rs[0] = int64(0)
				return rs[:1], nil
			}
			return nil, nil
		case c0 == '_' && c1 == '%':
			prefix := expr[1 : n-1]
			if len(p) > 0 && bytes.HasPrefix(p[1:], prefix) {
				rs[0] = int64(0)
				return rs[:1], nil
			}
			return nil, nil
		}
	}
	reg, err := regexp.Compile(convert(expr))
	if err != nil {
		return nil, err
	}
	if reg.Match(p) {
		rs[0] = int64(0)
		return rs[:1], nil
	}
	return nil, nil
}

func sliceNullLikePure(s *types.Bytes, expr []byte, nulls *roaring.Bitmap, rs []int64) ([]int64, error) {
	var cFlag int8 // case flag for like
	var reg *regexp.Regexp
	var err error
	n := len(expr) // expr length
	count := 0
	nullsIter := nulls.Iterator()
	nextNull := 0

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	switch {
	case n == 0:
		cFlag = 1
	case n == 1 && expr[0] == '%':
		cFlag = 2
	case n == 1 && expr[0] == '_':
		cFlag = 3
	case n > 1 && !bytes.ContainsAny(expr[1:len(expr)-1], "_%"):
		cFlag = 4
	default:
		cFlag = 5
		reg, err = regexp.Compile(convert(expr))
		if err != nil {
			return nil, err
		}
	}

	for i, j := 0, len(s.Offsets); i < j; i++ {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else {
			p := s.Get(int64(i))
			switch cFlag {
			case 1:
				if len(p) == 0 {
					rs[count] = int64(i)
					count++
				}
			case 2:
				rs[count] = int64(i)
				count++
			case 3:
				if len(p) == 1 {
					rs[count] = int64(i)
					count++
				}
			case 4:
				c0, c1 := expr[0], expr[n-1] // first and last
				switch {
				case !(c0 == '%' || c0 == '_') && !(c1 == '%' || c1 == '_'):
					if len(p) == n && bytes.Compare(expr, p) == 0 {
						rs[count] = int64(i)
						count++
					}
				case c0 == '_' && !(c1 == '%' || c1 == '_'):
					suffix := expr[1:]
					if len(p) == n && bytes.Compare(suffix, p[1:]) == 0 {
						rs[count] = int64(i)
						count++
					}
				case c0 == '%' && !(c1 == '%' || c1 == '_'):
					suffix := expr[1:]
					if bytes.HasSuffix(p, suffix) {
						rs[count] = int64(i)
						count++
					}
				case c1 == '_' && !(c0 == '%' || c0 == '_'):
					prefix := expr[:n-1]
					if len(p) == n && bytes.Compare(prefix, p[:n-1]) == 0 {
						rs[count] = int64(i)
						count++
					}
				case c1 == '%' && !(c0 == '%' || c0 == '_'):
					prefix := expr[:n-1]
					if len(p) >= n-1 && bytes.HasPrefix(p[:n-1], prefix) {
						rs[count] = int64(i)
						count++
					}
				case c0 == '%' && c1 == '%':
					substr := expr[1 : n-1]
					if bytes.Contains(p, substr) {
						rs[count] = int64(i)
						count++
					}
				case c0 == '%' && c1 == '_':
					suffix := expr[1 : n-1]
					if len(p) > 0 && bytes.HasSuffix(p[:len(p)-1], suffix) {
						rs[count] = int64(i)
						count++
					}
				case c0 == '_' && c1 == '%':
					prefix := expr[1 : n-1]
					if len(p) > 0 && bytes.HasPrefix(p[1:], prefix) {
						rs[count] = int64(i)
						count++
					}
				default:
					if reg.Match(p) {
						rs[count] = int64(i)
						count++
					}
				}
			case 5:
				if reg.Match(p) {
					rs[count] = int64(i)
					count++
				}
			default:
				return nil, errors.New("unexpected match rule of LIKE operator")
			}
		}
	}
	return rs[:count], nil
}

func sliceNullLikeSliceNull(s *types.Bytes, exprs *types.Bytes, nulls *roaring.Bitmap, rs []int64) ([]int64, error) {
	count := 0
	nullsIter := nulls.Iterator()
	nextNull := 0
	tempSlice := make([]int64, 1)

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, n := 0, len(s.Offsets); i < n; i++ {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else {
			k, err := pureLikePure(s.Get(int64(i)), exprs.Get(int64(i)), tempSlice)
			if err != nil {
				return nil, err
			}
			if k != nil {
				rs[count] = int64(i)
				count++
			}
		}
	}
	return rs[:count], nil
}

func pureLikeSliceNull(p []byte, exprs *types.Bytes, nulls *roaring.Bitmap, rs []int64) ([]int64, error) {
	count := 0
	nullsIter := nulls.Iterator()
	nextNull := 0
	tempSlice := make([]int64, 1)

	if nullsIter.HasNext() {
		nextNull = int(nullsIter.Next())
	} else {
		nextNull = -1
	}

	for i, n := 0, len(exprs.Offsets); i < n; i++ {
		if i == nextNull {
			if nullsIter.HasNext() {
				nextNull = int(nullsIter.Next())
			} else {
				nextNull = -1
			}
		} else {
			k, err := pureLikePure(p, exprs.Get(int64(i)), tempSlice)
			if err != nil {
				return nil, err
			}
			if k != nil {
				rs[count] = int64(i)
				count++
			}
		}
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
			w += copy(r[w:], []byte{'.'})
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
