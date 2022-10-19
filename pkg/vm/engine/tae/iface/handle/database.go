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

package handle

import "io"

type Database interface {
	io.Closer

	GetID() uint64
	GetName() string
	CreateRelation(def any) (Relation, error)
	CreateRelationWithID(def any, id uint64) (Relation, error)
	DropRelationByName(name string) (Relation, error)
	TruncateByName(name string) (Relation, error)
	TruncateWithID(name string, newTableId uint64) (Relation, error)

	UnsafeGetRelation(id uint64) (Relation, error)
	GetRelationByName(name string) (Relation, error)
	RelationCnt() int64
	Relations() []Relation

	MakeRelationIt() RelationIt
	String() string
	GetMeta() any
}
