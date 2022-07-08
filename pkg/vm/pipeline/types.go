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

package pipeline

import (
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Pipeline contains the information associated with a pipeline in a query execution plan.
// A query execution plan may contains one or more pipelines.
// As an example:
//
//  CREATE TABLE order
//  (
//        order_id    INT,
//        uid          INT,
//        item_id      INT,
//        year         INT,
//        nation       VARCHAR(100)
//  );
//
//  CREATE TABLE customer
//  (
//        uid          INT,
//        nation       VARCHAR(100),
//        city         VARCHAR(100)
//  );
//
//  CREATE TABLE supplier
//  (
//        item_id      INT,
//        nation       VARCHAR(100),
//        city         VARCHAR(100)
//  );
//
// 	SELECT c.city, s.city, sum(o.revenue) AS revenue
//  FROM customer c, order o, supplier s
//  WHERE o.uid = c.uid
//  AND o.item_id = s.item_id
//  AND c.nation = 'CHINA'
//  AND s.nation = 'CHINA'
//  AND o.year >= 1992 and o.year <= 1997
//  GROUP BY c.city, s.city, o.year
//  ORDER BY o.year asc, revenue desc;
//
//  AST PLAN:
//     order
//       |
//     group
//       |
//     filter
//       |
//     join
//     /  \
//    s   join
//        /  \
//       l   c
//
// In this example, a possible pipeline is as follows:
//
// pipeline:
// o ⨝ c ⨝ s
//  -> σ(c.nation = 'CHINA' ∧  o.year >= 1992 ∧  o.year <= 1997 ∧  s.nation = 'CHINA')
//  -> γ([c.city, s.city, o.year, sum(o.revenue) as revenue], c.city, s.city, o.year)
//  -> τ(o.year asc, revenue desc)
//  -> π(c.city, s.city, revenue)
type Pipeline struct {
	// attrs, column list.
	attrs []string
	// orders to be executed
	instructions vm.Instructions
	reg          *process.WaitRegister
}
