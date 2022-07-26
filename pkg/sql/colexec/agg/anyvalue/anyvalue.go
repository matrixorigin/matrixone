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

package anyvalue

func NewAnyvalue[T any]() *Anyvalue[T] {
	return &Anyvalue[T]{}
}

func (a *Anyvalue[T]) Grows(size int) {
	if len(a.NotSet) == 0 {
		a.NotSet = make([]bool, 0)
	}

	for i := 0; i < size; i++ {
		a.NotSet = append(a.NotSet, false)
	}
}

func (a *Anyvalue[T]) Eval(vs []T) []T {
	return vs
}

func (a *Anyvalue[T]) Fill(i int64, value T, ov T, z int64, isEmpty bool, isNull bool) (T, bool) {
	if !isNull && !a.NotSet[i] {
		a.NotSet[i] = true
		return value, false
	}
	return ov, isEmpty
}

func (a *Anyvalue[T]) Merge(xIndex int64, yIndex int64, x T, y T, xEmpty bool, yEmpty bool, yAnyValue any) (T, bool) {
	if !yEmpty {
		ya := yAnyValue.(*Anyvalue[T])
		if ya.NotSet[yIndex] && !a.NotSet[xIndex] {
			a.NotSet[xIndex] = true
			return y, false
		}
	}
	return x, xEmpty
}
