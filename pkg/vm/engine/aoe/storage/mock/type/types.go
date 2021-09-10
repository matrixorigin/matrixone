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

package mocktype

import ()

type ColType uint16

const (
	INVALID ColType = iota
	BOOLEAN
	SMALLINT
	INTEGER
	BIGINT

	STRING
)

func (t ColType) Size() uint64 {
	switch t {
	case BOOLEAN:
		return uint64(1)
	case SMALLINT:
		return uint64(2)
	case INTEGER:
		return uint64(4)
	case BIGINT:
		return uint64(8)
	case STRING:
		return uint64(1)
	}
	panic("Unsupported")
}

func (t ColType) IsStr() bool {
	switch t {
	case STRING:
		return true
	}
	return false
}
