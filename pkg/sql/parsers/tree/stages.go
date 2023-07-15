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

type CreateStage struct {
	statementImpl
	IfNotExists bool
	Name        Identifier
	URL         string
	CRENTIALS   string
}

func (node *CreateStage) Format(ctx *FmtCtx) {
	ctx.WriteString("create stage ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	node.Name.Format(ctx)

	ctx.WriteString("URL=")
	ctx.WriteString(node.URL)
	
	if node.CRENTIALS !=


}

func (node *CreateStage) GetStatementType() string { return "Create Stage" }
func (node *CreateStage) GetQueryType() string     { return QueryTypeOth }
