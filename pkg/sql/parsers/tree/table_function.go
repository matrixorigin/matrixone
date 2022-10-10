// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

type TableFunction struct {
	statementImpl
	Func *FuncExpr
}

func (t *TableFunction) Format(ctx *FmtCtx) {
	t.Func.Format(ctx)
}

func (t TableFunction) Id() string {
	_, _, name := t.Func.Func.FunctionReference.(*UnresolvedName).GetNames()
	return name
}
