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

package tree

import (
	"strings"
)

type CStrParts [4]*CStr
type CStr struct {
	o string
	c string
	// quote bool
}

func NewCStr(str string, lower int64) *CStr {
	cs := &CStr{o: str}
	if lower == 0 {
		cs.c = cs.o
		return cs
	}
	cs.c = strings.ToLower(cs.o)
	return cs
}

func (cs *CStr) SetConfig(lower int64) {
	if lower == 0 {
		cs.c = cs.o
		return
	}
	cs.c = strings.ToLower(cs.o)
}

func (cs *CStr) ToLower() string {
	return strings.ToLower(cs.o)
}

func (cs *CStr) Origin() string {
	return cs.o
}

func (cs *CStr) Compare() string {
	return cs.c
}

func (cs *CStr) Empty() bool {
	return len(cs.o) == 0
}

func NewCStrUseOrigin(str string, useOrigin int64) *CStr {
	cs := &CStr{o: str}
	if useOrigin == 1 {
		cs.c = cs.o
		return cs
	}
	cs.c = strings.ToLower(cs.o)
	return cs
}
