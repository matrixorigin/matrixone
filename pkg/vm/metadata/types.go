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

package metadata

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type Nodes []Node

type Node struct {
	Id   string `json:"id"`
	Addr string `json:"address"`
}

type Attribute struct {
	// Alg compression algorithm
	Alg int
	// Name name of attribute
	Name string
	// Default contains default expression definition of attribute
	Default DefaultExpr
	// type of attribute
	Type types.Type

	PrimaryKey bool
}

type DefaultExpr struct {
	Exist  bool
	Value  interface{}
	IsNull bool
}

// MakeDefaultExpr returns a new DefaultExpr
func MakeDefaultExpr(exist bool, value interface{}, isNull bool) DefaultExpr {
	return DefaultExpr{
		Exist:  exist,
		Value:  value,
		IsNull: isNull,
	}
}

// EmptyDefaultExpr means there is no definition for default expr
var EmptyDefaultExpr = DefaultExpr{Exist: false}

func (attr Attribute) HasDefaultExpr() bool {
	return attr.Default.Exist
}

func (attr Attribute) GetDefaultExpr() (interface{}, bool) {
	return attr.Default.Value, attr.Default.IsNull
}
