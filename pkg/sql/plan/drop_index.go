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

package plan

import "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"

func (b *build) BuildDropIndex(stmt *tree.DropIndex, plan *DropIndex) error {
	_, _, r, err := b.tableName(&stmt.TableName)
	if err != nil {
		return err
	}
	plan.Relation = r
	plan.Id = string(stmt.Name)
	plan.IfExistFlag = stmt.IfExists
	return nil
}