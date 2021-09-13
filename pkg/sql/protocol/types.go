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

package protocol

import "matrixone/pkg/vm"

const ( // extend type
	Attr = iota
	Unary
	Multi
	Paren
	Value
	Binary
)

const ( // table define type
	Index = iota
	Attribute
)

const ( // partition type
	List = iota
	Range
	ListWithSub
	RangeWithSub
)

type Field struct {
	Type int8
	Attr string
}

type TopArgument struct {
	Flg   bool
	Limit int64
	Fs    []Field
}

type Extend struct {
	Op    int
	Name  string
	Alias string
}

type GroupArgument struct {
	Gs    []string
	Es    []Extend
	Refer map[string]uint64
}

type JoinArgument struct {
	R      string
	S      string
	Rattrs []string
	Sattrs []string
}

type ProjectionArgument struct {
	Attrs []string
	Refer map[string]uint64
}

type Segment struct {
	IsRemote bool
	Version  uint64
	Id       string
	GroupId  string
	TabletId string
}

type Source struct {
	ID    string
	DB    string
	Segs  []Segment
	Refer map[string]uint64
}

type Scope struct {
	Magic int
	Data  Source
	Ss    []Scope
	Ins   vm.Instructions
}
