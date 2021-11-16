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

package top

import (
	"fmt"
	"matrixone/pkg/compare"
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
	n     int // number of attributes involved in sorting
	sels  []int64
	attrs []string
	cmps  []compare.Compare
}

type Field struct {
	Attr string
	Type Direction
}

type Argument struct {
	Limit int64
	Fs    []Field
	Ctr   Container
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

func (ctr *Container) compare(i, j int64) int {
	for k := 0; k < ctr.n; k++ {
		if r := ctr.cmps[k].Compare(0, 0, i, j); r != 0 {
			return r
		}
	}
	return 0
}

// maximum heap
func (ctr *Container) Len() int {
	return len(ctr.sels)
}

func (ctr *Container) Less(i, j int) bool {
	return ctr.compare(ctr.sels[i], ctr.sels[j]) > 0
}

func (ctr *Container) Swap(i, j int) {
	ctr.sels[i], ctr.sels[j] = ctr.sels[j], ctr.sels[i]
}

func (ctr *Container) Push(x interface{}) {
	ctr.sels = append(ctr.sels, x.(int64))
}

func (ctr *Container) Pop() interface{} {
	n := len(ctr.sels) - 1
	x := ctr.sels[n]
	ctr.sels = ctr.sels[:n]
	return x
}
