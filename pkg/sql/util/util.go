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

package util

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strings"
)

func SplitTableAndColumn(name string) (string, string) {
	var schema string

	{ // determine if it is a function call
		i, j := strings.Index(name, "("), strings.Index(name, ")")
		if i >= 0 && j >= 0 && j > i {
			return "", name
		}
	}
	xs := strings.Split(name, ".")
	for i := 0; i < len(xs)-1; i++ {
		if i > 0 {
			schema += "."
		}
		schema += xs[i]
	}
	return schema, xs[len(xs)-1]
}

func FindPrimaryKey(def *plan.TableDef) bool {
	for _, def := range def.Defs {
		switch def.Def.(type) {
		case *plan.TableDef_DefType_Pk:
			return true
		}
	}
	return false
}

// TableIsClusterTable check the table type is cluster table
func TableIsClusterTable(tableType string) bool {
	return tableType == catalog.SystemClusterRel
}

var (
	clusterTableAttributeName = "account_id"
	clusterTableAttributeType = &tree.T{InternalType: tree.InternalType{
		Family:   tree.IntFamily,
		Width:    32,
		Oid:      uint32(defines.MYSQL_TYPE_LONG),
		Unsigned: true,
	}}
)

func GetClusterTableAttributeName() string {
	return clusterTableAttributeName
}

func GetClusterTableAttributeType() *tree.T {
	return clusterTableAttributeType
}

func IsClusterTableAttribute(name string) bool {
	return name == clusterTableAttributeName
}
