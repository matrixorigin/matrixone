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

package max

import "github.com/matrixorigin/matrixone/pkg/container/types"

type int8Max struct {
	v   int8
	cnt int64
	typ types.Type
}

type int16Max struct {
	v   int16
	cnt int64
	typ types.Type
}

type int32Max struct {
	v   int32
	cnt int64
	typ types.Type
}

type int64Max struct {
	v   int64
	cnt int64
	typ types.Type
}

type uint8Max struct {
	v   uint8
	cnt int64
	typ types.Type
}

type uint16Max struct {
	cnt int64
	v   uint16
	typ types.Type
}

type uint32Max struct {
	cnt int64
	v   uint32
	typ types.Type
}

type uint64Max struct {
	cnt int64
	v   uint64
	typ types.Type
}

type float32Max struct {
	cnt int64
	v   float32
	typ types.Type
}

type float64Max struct {
	cnt int64
	v   float64
	typ types.Type
}

type strMax struct {
	cnt int64
	v   []byte
	typ types.Type
}
