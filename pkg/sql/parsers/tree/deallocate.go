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

package tree

type Deallocate struct {
	Statement
	IsDrop bool
	Name   Identifier
}

func (node *Deallocate) Format(ctx *FmtCtx) {
	if node.IsDrop {
		ctx.WriteString("drop")
	} else {
		ctx.WriteString("deallocate")
	}
	ctx.WriteString(" prepare ")
	node.Name.Format(ctx)
}

func (node *Deallocate) GetStatementType() string { return "Deallocate" }
func (node *Deallocate) GetQueryType() string     { return QueryTypeDCL }

func NewDeallocate(name Identifier, isDrop bool) *Deallocate {
	return &Deallocate{
		IsDrop: isDrop,
		Name:   name,
	}
}
