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

package relation

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
)

type Segment struct {
	IsRemote bool
	Version  uint64
	Id       string
	GroupId  string
	TabletId string
	Node     metadata.Node
}

type Unit struct {
	Segs []*Segment
	N    metadata.Node
}

type Relation struct {
	S     bool // is single
	ID    string
	DB    string
	Rid   string // real id
	Us    []*Unit
	Cols  []string
	R     engine.Relation
	Attrs map[string]types.Type
}
