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

package mergeorder

import (
	"fmt"
	"matrixone/pkg/container/batch"
)

const (
	Build = iota
	Eval
	End
)

// Direction for ordering results.
type Direction int8

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

type Container struct {
	state int
	ds    []bool   // ds[i] == true: the attrs[i] are in descending order
	attrs []string // sorted list of attributes
	bat   *batch.Batch
}

type Field struct {
	Attr string
	Type Direction
}

type Argument struct {
	Flg bool // is local merge
	Fs  []Field
	Ctr Container
}

var directionName = [...]string{
	DefaultDirection: "",
	Ascending:        "ASC",
	Descending:       "DESC",
}

func (n Field) String() string {
	s := n.Attr
	if n.Type != DefaultDirection {
		s += " " + n.Type.String()
	}
	return s
}

func (i Direction) String() string {
	if i < 0 || i > Direction(len(directionName)-1) {
		return fmt.Sprintf("Direction(%d)", i)
	}
	return directionName[i]
}
