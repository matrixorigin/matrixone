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

func newScopeSet() *ScopeSet {
	return &ScopeSet{}
}

func (qry *Query) IsEmpty() bool {
	return len(qry.Stack) == 0
}

func (qry *Query) Clear() {
	qry.Stack = make([]*ScopeSet, 0, 4)
}

func (qry *Query) Top() *ScopeSet {
	return qry.Stack[len(qry.Stack)-1]
}

func (qry *Query) Push(ss *ScopeSet) {
	qry.Stack = append(qry.Stack, ss)
}

func (qry *Query) Pop() *ScopeSet {
	n := len(qry.Stack) - 1
	ss := qry.Stack[n]
	qry.Stack = qry.Stack[:n]
	return ss
}
