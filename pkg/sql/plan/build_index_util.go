// Copyright 2023 Matrix Origin
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

package plan

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strings"
)

// checkConstraintNames Check whether the name of the constraint(index,unqiue etc) is legal, and handle constraints without a name
func checkConstraintNames(uniqueConstraints []*tree.UniqueIndex, indexConstraints []*tree.Index, ctx context.Context) error {
	constrNames := map[string]bool{}
	// Check not empty constraint name whether is duplicated.
	for _, constr := range indexConstraints {
		err := checkDuplicateConstraint(constrNames, constr.Name, false, ctx)
		if err != nil {
			return err
		}
	}
	for _, constr := range uniqueConstraints {
		err := checkDuplicateConstraint(constrNames, constr.Name, false, ctx)
		if err != nil {
			return err
		}
	}
	// set empty constraint names(index and unique index)
	for _, constr := range indexConstraints {
		setEmptyIndexName(constrNames, constr)
	}
	for _, constr := range uniqueConstraints {
		setEmptyUniqueIndexName(constrNames, constr)
	}
	return nil
}

// checkDuplicateConstraint Check whether the constraint name is duplicate
func checkDuplicateConstraint(namesMap map[string]bool, name string, foreign bool, ctx context.Context) error {
	if name == "" {
		return nil
	}
	nameLower := strings.ToLower(name)
	if namesMap[nameLower] {
		if foreign {
			return moerr.NewInvalidInput(ctx, "Duplicate foreign key constraint name '%s'", name)
		}
		return moerr.NewDuplicateKey(ctx, name)
	}
	namesMap[nameLower] = true
	return nil
}

// setEmptyUniqueIndexName Set name for unqiue index constraint with an empty name
func setEmptyUniqueIndexName(namesMap map[string]bool, indexConstr *tree.UniqueIndex) {
	if indexConstr.Name == "" && len(indexConstr.KeyParts) > 0 {
		colName := indexConstr.KeyParts[0].ColName.Parts[0]
		constrName := colName
		i := 2
		if strings.EqualFold(constrName, "PRIMARY") {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[constrName] {
			// loop forever until we find constrName that haven't been used.
			constrName = fmt.Sprintf("%s_%d", colName, i)
			i++
		}
		indexConstr.Name = constrName
		namesMap[constrName] = true
	}
}

// setEmptyIndexName Set name for index constraint with an empty name
func setEmptyIndexName(namesMap map[string]bool, indexConstr *tree.Index) {
	if indexConstr.Name == "" && len(indexConstr.KeyParts) > 0 {
		var colName string
		if colName == "" {
			colName = indexConstr.KeyParts[0].ColName.Parts[0]
		}
		constrName := colName
		i := 2
		if strings.EqualFold(constrName, "PRIMARY") {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[constrName] {
			//  loop forever until we find constrName that haven't been used.
			constrName = fmt.Sprintf("%s_%d", colName, i)
			i++
		}
		indexConstr.Name = constrName
		namesMap[constrName] = true
	}
}

// TODO
// Currently, using expression as index keyparts are not supported in matrixone
func checkIndexKeypartSupportability(context context.Context, keyParts []*tree.KeyPart) error {
	for _, key := range keyParts {
		if key.Expr != nil {
			return moerr.NewInternalError(context, "unsupported index which using expression as keypart")
		}
		if key.Direction != tree.DefaultDirection {
			return moerr.NewInternalError(context, "the index keypart does not support sorting")
		}
	}
	return nil
}
