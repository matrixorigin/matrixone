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
	"regexp"
	"strings"
	"unicode/utf8"
	"unsafe"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	// BtSliceAndConst is a like function between a slice and a const.
	BtSliceAndConst func(*types.Bytes, []byte, []int64) ([]int64, error)
	// BtSliceAndSlice is a like function between two slices.
	BtSliceAndSlice func(*types.Bytes, *types.Bytes, []int64) ([]int64, error)
	// BtConstAndSlice is a like function between a const and a slice
	BtConstAndSlice func([]byte, *types.Bytes, []int64) ([]int64, error)
	// BtConstAndConst is a like function between two const values.
	BtConstAndConst func([]byte, []byte, []int64) ([]int64, error)
	// BtSliceNullAndConst is a like function between a slice (has null value) and a const value.
	BtSliceNullAndConst func(*types.Bytes, []byte, *roaring.Bitmap, []int64) ([]int64, error)
	// BtSliceNullAndSliceNull is a like function between two slices which have null value.
	BtSliceNullAndSliceNull func(*types.Bytes, *types.Bytes, *roaring.Bitmap, []int64) ([]int64, error)
	// BtConstAndSliceNull is a like function between a const value and a slice (has null value).
	BtConstAndSliceNull func([]byte, *types.Bytes, *roaring.Bitmap, []int64) ([]int64, error)
)

var _ = BtSliceAndConst
var _ = BtSliceAndSlice
var _ = BtConstAndSlice
var _ = BtConstAndConst
var _ = BtSliceNullAndConst
var _ = BtSliceNullAndSliceNull
var _ = BtConstAndSliceNull

func init() {
	BtSliceAndConst = sliceLikeScalar
	BtSliceAndSlice = sliceLikeSlice
	BtConstAndSlice = scalarLikeSlice
	BtConstAndConst = scalarLikeScalar
	BtSliceNullAndConst = sliceContainsNullLikeScalar
	BtSliceNullAndSliceNull = likeBetweenSlicesContainNull
	BtConstAndSliceNull = scalarLikeSliceContainsNull
}

// <source column> like 'rule'
func sliceLikeScalar(s *types.Bytes, expr []byte, rs []int64) ([]int64, error) {
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
				if s.Lengths[i] == n && bytes.Equal(expr, s.Get(int64(i))) {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c0 == '_' && !(c1 == '%' || c1 == '_'):
			suffix := expr[1:]
			count := 0
			for i := range s.Offsets {
				temp := s.Get(int64(i))
				if len(temp) > 0 {
					temp = temp[1:]
				}
				if s.Lengths[i] == n && bytes.Equal(suffix, temp) {
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
				if s.Lengths[i] == n && bytes.Equal(prefix, s.Get(int64(i))) {
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
				temp := s.Get(int64(i))
				if len(temp) > 0 {
					temp = temp[:len(temp)-1]
				}
				if s.Lengths[i] > 0 && bytes.HasSuffix(temp, suffix) {
					rs[count] = int64(i)
					count++
				}
			}
			return rs[:count], nil
		case c0 == '_' && c1 == '%':
			prefix := expr[1 : n-1]
			count := 0
			for i := range s.Offsets {
				temp := s.Get(int64(i))
				if len(temp) > 0 {
					temp = temp[1:]
				}
				if s.Lengths[i] > 0 && bytes.HasPrefix(temp, prefix) {
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

// <source column> like <rule column>
func sliceLikeSlice(s *types.Bytes, exprs *types.Bytes, rs []int64) ([]int64, error) {
	count := 0
	tempSlice := make([]int64, 1)
	n := len(s.Lengths)
	if n != len(exprs.Lengths) {
		return nil, errors.New("unexpected error when LIKE operator")
	}
	for i := range s.Offsets {
		k, err := scalarLikeScalar(s.Get(int64(i)), exprs.Get(int64(i)), tempSlice)
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

// 'source' like <rule column>
func scalarLikeSlice(p []byte, exprs *types.Bytes, rs []int64) ([]int64, error) {
	count := 0
	tempSlice := make([]int64, 1)
	for i := range exprs.Offsets {
		k, err := scalarLikeScalar(p, exprs.Get(int64(i)), tempSlice)
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

// 'source' like 'rule'
func scalarLikeScalar(p []byte, expr []byte, rs []int64) ([]int64, error) {
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
			if len(p) == n && bytes.Equal(expr, p) {
				rs[0] = int64(0)
				return rs[:1], nil
			}
			return nil, nil
		case c0 == '_' && !(c1 == '%' || c1 == '_'):
			suffix := expr[1:]
			if len(p) == n && bytes.Equal(suffix, p[1:]) {
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
			if len(p) == n && bytes.Equal(prefix, p[:n-1]) {
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

// 'source' like <rule column may contains null>
func sliceContainsNullLikeScalar(s *types.Bytes, expr []byte, nulls *roaring.Bitmap, rs []int64) ([]int64, error) {
	var cFlag int8 // case flag for like

	reg, err := regexp.Compile(convert(expr))
	if err != nil {
		return nil, err
	}

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
					if len(p) == n && bytes.Equal(expr, p) {
						rs[count] = int64(i)
						count++
					}
				case c0 == '_' && !(c1 == '%' || c1 == '_'):
					suffix := expr[1:]
					if len(p) == n && bytes.Equal(suffix, p[1:]) {
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
					if len(p) == n && bytes.Equal(prefix, p[:n-1]) {
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
					temp := s.Get(int64(i))
					if len(temp) > 0 {
						temp = temp[1:]
					}
					if s.Lengths[i] > 0 && bytes.HasPrefix(temp, prefix) {
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

// <source column may contains null> like
func likeBetweenSlicesContainNull(s *types.Bytes, exprs *types.Bytes, nulls *roaring.Bitmap, rs []int64) ([]int64, error) {
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
			k, err := scalarLikeScalar(s.Get(int64(i)), exprs.Get(int64(i)), tempSlice)
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

// 'source' like <rule column may contains null>
func scalarLikeSliceContainsNull(p []byte, exprs *types.Bytes, nulls *roaring.Bitmap, rs []int64) ([]int64, error) {
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
			k, err := scalarLikeScalar(p, exprs.Get(int64(i)), tempSlice)
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
