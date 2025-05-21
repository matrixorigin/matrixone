// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// return true or false for filter. arg = [search_string, mode, key1, key2, ..., KeyN]
func fullTextMatch(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return moerr.NewNotSupported(proc.Ctx, "MATCH() AGAINST() function cannot be replaced by FULLTEXT INDEX and full table scan with fulltext search is not supported yet.")
}

// return score.  arg = [search_string, mode, key1, key2, ..., KeyN]
func fullTextMatchScore(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return moerr.NewNotSupported(proc.Ctx, "MATCH() AGAINST() function cannot be replaced by FULLTEXT INDEX and full table scan with fulltext search is not supported yet.")
}
