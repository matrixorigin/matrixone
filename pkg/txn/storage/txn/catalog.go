// Copyright 2022 Matrix Origin
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

package txnstorage

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	txnengine "github.com/matrixorigin/matrixone/pkg/vm/engine/txn"
)

type DatabaseAttrs struct {
	ID   string
	Name string
}

func (d DatabaseAttrs) PrimaryKey() Text {
	return Text(d.ID)
}

type RelationAttrs struct {
	ID               string
	DatabaseID       string
	Name             string
	Type             txnengine.RelationType
	Comments         string
	Properties       map[string]string
	PrimaryColumnIDs []string
}

func (r RelationAttrs) PrimaryKey() Text {
	return Text(r.ID)
}

type AttributeAttrs struct {
	ID         string
	RelationID string
	engine.Attribute
}

func (a AttributeAttrs) PrimaryKey() Text {
	return Text(a.ID)
}

type IndexAttrs struct {
	ID         string
	RelationID string
	engine.IndexTableDef
}

func (i IndexAttrs) PrimaryKey() Text {
	return Text(i.ID)
}
