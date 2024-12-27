// Copyright 2022 Matrix Origin
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
	"cmp"
	"iter"
	"slices"
)

func ptrTo[T any](v T) *T {
	return &v
}

func ceilingDiv[T int | int64](n, by T) T {
	res := n / by
	if n%by == 0 {
		return res
	}
	return res + 1
}

func zeroToNil[T comparable](v T) *T {
	var zero T
	if v == zero {
		return nil
	}
	return &v
}

func SortedList(seq iter.Seq2[*DirEntry, error]) (ret []DirEntry, err error) {
	for entry, err := range seq {
		if err != nil {
			return nil, err
		}
		ret = append(ret, *entry)
	}
	slices.SortFunc(ret, func(a, b DirEntry) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return
}

func firstNonZero[T comparable](args ...T) T {
	var zero T
	for _, arg := range args {
		if arg != zero {
			return arg
		}
	}
	return zero
}
