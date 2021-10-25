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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/metadata"
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
	S     bool // 是否带dbname前缀
	ID    string
	DB    string
	Rid   string // renamed id
	Us    []*Unit
	Rs    []string // result columns
	Cols  []string
	R     engine.Relation
	Attrs map[string]types.Type
}
