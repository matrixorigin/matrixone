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

//Delete statement
type Delete struct {
	statementImpl
	Table     TableExpr
	Where     *Where
	OrderBy   OrderBy
	Limit     *Limit
}

func NewDelete(t TableExpr,w *Where,o OrderBy,l *Limit)*Delete{
	return &Delete{
		Table:         t,
		Where:         w,
		OrderBy:       o,
		Limit:         l,
	}
}