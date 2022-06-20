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

package vector

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

type ref interface {
	constraints.Integer | constraints.Float | bool |
		types.Date | types.Datetime | types.Timestamp | types.Decimal64 | types.Decimal128
}

func MustTCols[T ref](v *Vector) []T {
	if t, ok := v.Col.([]T); ok {
		return t
	}
	if v.Typ.Oid == types.T_any {
		return nil
	}
	panic("unexpected parameter types were received")
}

func MustBytesCols(v *Vector) *types.Bytes {
	if t, ok := v.Col.(*types.Bytes); ok {
		return t
	}
	if v.Typ.Oid == types.T_any {
		return nil
	}
	panic("unexpected parameter types were received")
}
