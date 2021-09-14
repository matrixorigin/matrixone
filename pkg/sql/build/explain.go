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

package build

import (
	"fmt"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/explain"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
)

func (b *build) buildExplain(stmt tree.Explain) (op.OP, error) {
	switch n := stmt.(type) {
	case *tree.ExplainStmt:
		o, err := b.BuildStatement(n.Statement)
		if err != nil {
			return nil, err
		}
		return explain.New(o), nil
	case *tree.ExplainAnalyze:
		o, err := b.BuildStatement(n.Statement)
		if err != nil {
			return nil, err
		}
		return explain.New(o), nil
	case *tree.ExplainFor:
		o, err := b.BuildStatement(n.Statement)
		if err != nil {
			return nil, err
		}
		return explain.New(o), nil
	}
	return nil, sqlerror.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport explain statement: '%v'", stmt))
}
