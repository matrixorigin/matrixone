// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package group_concat

import (
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// for example:
// group_concat(distinct a,b order by a seporator '|')
// dist: true
// groupExpr: a,b
// orderByExpr: a
// separator: "|"
type Argument struct {
	Dist        bool
	GroupExpr   []*plan.Expr // group Expressions
	OrderByExpr []*plan.Expr // orderby Expressions, for now we don't care about it
	Separator   string
	// because we store multiAgg and UnaryAgg separately.
	// we use this to record the order in sql.
	// like 'select group_concat(a),avg(a) from t;'
	// this orderId will be 0.
	// but for 'select avg(a), group_concat(a) from t;'
	// this orderId will be 1.
	OrderId int32
}
