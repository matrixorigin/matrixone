// Copyright 2024 Matrix Origin
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

import "strconv"

type UpgradeStatement struct {
	statementImpl
	Target *Target
	Retry  int64
}

// input:  "upgrade account all with retry 10",
type Target struct {
	NodeFormatter
	AccountName  string
	IsALLAccount bool // Add attribute to indicate whether it is an ALL account
}

func (node *Target) Format(ctx *FmtCtx) {
	if node.IsALLAccount {
		ctx.WriteString("all")
	} else {
		ctx.WriteString(node.AccountName)
	}
}

func (node *UpgradeStatement) Format(ctx *FmtCtx) {
	ctx.WriteString("upgrade account ")
	node.Target.Format(ctx)
	if node.Retry > 0 {
		ctx.WriteString(" with retry " + strconv.FormatInt(node.Retry, 10))
	}
}

func (node *UpgradeStatement) GetStatementType() string { return "UpgradeStatement" }

func (node *UpgradeStatement) GetQueryType() string { return QueryTypeOth }

func (node UpgradeStatement) TypeName() string { return "tree.UpgradeStatement" }
