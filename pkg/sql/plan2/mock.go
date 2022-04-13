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

package plan2

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type MockCompilerContext struct {
	objects map[string]*ObjectRef
}

func (m *MockCompilerContext) Resolve(name string) *ObjectRef {
	return m.objects[name]
}

func (m *MockCompilerContext) Cost(obj *ObjectRef, e *Expr) Cost {
	var c Cost
	div := 1.0
	if e != nil {
		div = 10.0
	}

	c.Card = 1000000 / div
	c.Rowsize = 100
	c.Ndv = 900000 / div
	c.Start = 0
	c.Total = 1000
	return c
}

type MockOptimizer struct {
	ctxt MockCompilerContext
}

func (moc *MockOptimizer) Optimize(stmt tree.Statement) *Query {
	return nil
}

func (moc *MockOptimizer) CurrentContext() CompilerContext {
	return &moc.ctxt
}
