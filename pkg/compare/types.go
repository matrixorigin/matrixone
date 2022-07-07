// Copyright 2021 - 2022 Matrix Origin
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

package compare

import (
	"github.com/matrixorigin/matrixone/pkg/container/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type Compare interface {
	Vector() vector.AnyVector
	Set(int, vector.AnyVector)
	Copy(int, int, int64, int64)
	Compare(int, int, int64, int64) int
}

type compare[T types.All] struct {
	xs  [][]T
	cmp func(T, T) int
	ns  []*bitmap.Bitmap
	vs  []vector.AnyVector
	cpy func([]T, []T, int64, int64)
}
