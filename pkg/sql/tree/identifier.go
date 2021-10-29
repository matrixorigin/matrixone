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

import "fmt"

// IdentifierName is referenced in the expression
type IdentifierName interface {
	Expr
}

//sql indentifier
type Identifier string

//
type UnrestrictedIdentifier string

//the list of identifiers.
type IdentifierList []Identifier

type ColumnItem struct {
	IdentifierName

	//the name of the column
	ColumnName Identifier
}

//the unresolved qualified name like column name.
type UnresolvedName struct {
	exprImpl
	//the number of name parts specified, including the star. Always 1 or greater.
	NumParts int

	//the name ends with a star. then the first element is empty in the Parts
	Star bool

	// Parts are the name components (at most 4: column, table, db/schema, catalog.), in reverse order.
	Parts NameParts
}

//the path in an UnresolvedName.
type NameParts = [4]string

func NewUnresolvedName(parts ...string) (*UnresolvedName, error) {
	l := len(parts)
	if l < 1 || l > 4 {
		panic(fmt.Errorf("the count of name parts among [1,4]"))
		return nil, nil
	}
	beg := 0
	for i := 0; i < l; i++ {
		if len(parts[i]) == 0 {
			beg++
		} else {
			break
		}
	}
	//if beg >= l{
	//	return nil,fmt.Errorf("name parts are all empty string")
	//}
	u := &UnresolvedName{
		NumParts: l - beg,
		Star:     false,
	}
	for i := beg; i < len(parts); i++ {
		u.Parts[i-beg] = parts[l-1-(i-beg)]
	}
	return u, nil
}

func NewUnresolvedNameWithStar(parts ...string) (*UnresolvedName, error) {
	l := len(parts)
	if l < 1 || l > 3 {
		panic(fmt.Errorf("the count of name parts among [1,3]"))
		return nil, nil
	}
	beg := 0
	for i := 0; i < l; i++ {
		if len(parts[i]) == 0 {
			beg++
		} else {
			break
		}
	}
	//if beg >= l{
	//	return nil,fmt.Errorf("name parts are all empty string")
	//}
	u := &UnresolvedName{
		NumParts: 1 + l - beg,
		Star:     true,
	}
	u.Parts[0] = ""
	for i := beg; i < len(parts); i++ {
		u.Parts[i+1-beg] = parts[l-1-(i-beg)]
	}
	return u, nil
}

//variable in the scalar expression
type VarName interface {
	Expr
}

var _ VarName = &UnresolvedName{}
var _ VarName = UnqualifiedStar{}

//'*' in the scalar expression
type UnqualifiedStar struct {
	VarName
}

var starName VarName = UnqualifiedStar{}

func StarExpr() VarName {
	return starName
}
