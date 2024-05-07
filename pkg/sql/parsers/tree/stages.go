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

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {

	reuse.CreatePool[CreateStage](
		func() *CreateStage { return &CreateStage{} },
		func(c *CreateStage) { c.reset() },
		reuse.DefaultOptions[CreateStage](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DropStage](
		func() *DropStage { return &DropStage{} },
		func(d *DropStage) { d.reset() },
		reuse.DefaultOptions[DropStage](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AlterStage](
		func() *AlterStage { return &AlterStage{} },
		func(a *AlterStage) { a.reset() },
		reuse.DefaultOptions[AlterStage](), //.
	) //WithEnableChecker()

}

type CreateStage struct {
	statementImpl
	IfNotExists bool
	Name        Identifier
	Url         string
	Credentials StageCredentials
	Status      StageStatus
	Comment     StageComment
}

func NewCreateStage(ifNotExists bool, name Identifier, url string, credentials StageCredentials, status StageStatus, comment StageComment) *CreateStage {
	createStage := reuse.Alloc[CreateStage](nil)
	createStage.IfNotExists = ifNotExists
	createStage.Name = name
	createStage.Url = url
	createStage.Credentials = credentials
	createStage.Status = status
	createStage.Comment = comment
	return createStage
}

func (node *CreateStage) Format(ctx *FmtCtx) {
	ctx.WriteString("create stage ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	node.Name.Format(ctx)

	ctx.WriteString(" url=")
	ctx.WriteString(fmt.Sprintf("'%s'", node.Url))

	node.Credentials.Format(ctx)
	node.Status.Format(ctx)
	node.Comment.Format(ctx)
}

func (node *CreateStage) Free() {
	reuse.Free[CreateStage](node, nil)
}

func (node *CreateStage) reset() {
	*node = CreateStage{}
}

func (node CreateStage) TypeName() string { return "tree.CreateStage" }

func (node *CreateStage) GetStatementType() string { return "Create Stage" }
func (node *CreateStage) GetQueryType() string     { return QueryTypeOth }

type DropStage struct {
	statementImpl
	IfNotExists bool
	Name        Identifier
}

func (node *DropStage) Free() {
	reuse.Free[DropStage](node, nil)
}

func (node *DropStage) Format(ctx *FmtCtx) {
	ctx.WriteString("drop stage ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	node.Name.Format(ctx)
}

func (node *DropStage) reset() {
	*node = DropStage{}
}

func (node *DropStage) GetStatementType() string { return "Drop Stage" }
func (node *DropStage) GetQueryType() string     { return QueryTypeOth }

func (node DropStage) TypeName() string { return "tree.DropStage" }

func NewDropStage(ifNotExists bool, name Identifier) *DropStage {
	dropStage := reuse.Alloc[DropStage](nil)
	dropStage.IfNotExists = ifNotExists
	dropStage.Name = name
	return dropStage
}

type AlterStage struct {
	statementImpl
	IfNotExists       bool
	Name              Identifier
	UrlOption         StageUrl
	CredentialsOption StageCredentials
	StatusOption      StageStatus
	Comment           StageComment
}

func (node *AlterStage) Free() {
	reuse.Free[AlterStage](node, nil)
}

func (node *AlterStage) Format(ctx *FmtCtx) {
	ctx.WriteString("alter stage ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	node.Name.Format(ctx)
	ctx.WriteString(" set ")
	node.UrlOption.Format(ctx)
	node.CredentialsOption.Format(ctx)
	node.StatusOption.Format(ctx)
	node.Comment.Format(ctx)
}

func (node *AlterStage) reset() {
	*node = AlterStage{}
}

func (node *AlterStage) GetStatementType() string { return "Alter Stage" }
func (node *AlterStage) GetQueryType() string     { return QueryTypeOth }

func (node AlterStage) TypeName() string { return "tree.AlterStage" }

func NewAlterStage(ifNotExists bool, name Identifier, urlOption StageUrl, credentialsOption StageCredentials, statusOption StageStatus, comment StageComment) *AlterStage {
	alterStage := reuse.Alloc[AlterStage](nil)
	alterStage.IfNotExists = ifNotExists
	alterStage.Name = name
	alterStage.UrlOption = urlOption
	alterStage.CredentialsOption = credentialsOption
	alterStage.StatusOption = statusOption
	alterStage.Comment = comment
	return alterStage
}

type StageStatusOption int

const (
	StageStatusEnabled StageStatusOption = iota
	StageStatusDisabled
)

func (sso StageStatusOption) String() string {
	switch sso {
	case StageStatusEnabled:
		return "enabled"
	case StageStatusDisabled:
		return "disabled"
	default:
		return "disabled"
	}
}

type StageStatus struct {
	Exist  bool
	Option StageStatusOption
}

func (node *StageStatus) Format(ctx *FmtCtx) {
	if node.Exist {
		switch node.Option {
		case StageStatusEnabled:
			ctx.WriteString(" enabled")
		case StageStatusDisabled:
			ctx.WriteString(" disabled")
		}
	}
}

type StageComment struct {
	Exist   bool
	Comment string
}

func (node *StageComment) Format(ctx *FmtCtx) {
	if node.Exist {
		ctx.WriteString(" comment ")
		ctx.WriteString(fmt.Sprintf("'%s'", node.Comment))
	}
}

type StageCredentials struct {
	Exist       bool
	Credentials []string
}

func (node *StageCredentials) Format(ctx *FmtCtx) {
	if node.Exist {

		ctx.WriteString(" crentiasl=")
		ctx.WriteString("{")
		for i := 0; i < len(node.Credentials)-1; i += 2 {
			ctx.WriteString(fmt.Sprintf("'%s'", node.Credentials[i]))
			ctx.WriteString("=")
			ctx.WriteString(fmt.Sprintf("'%s'", node.Credentials[i+1]))
			if i != len(node.Credentials)-2 {
				ctx.WriteString(",")
			}
		}
		ctx.WriteString("}")
	}
}

type StageUrl struct {
	Exist bool
	Url   string
}

func (node *StageUrl) Format(ctx *FmtCtx) {
	if node.Exist {
		ctx.WriteString(" url=")
		ctx.WriteString(fmt.Sprintf("'%s'", node.Url))
	}
}

type ShowStages struct {
	showImpl
	Like *ComparisonExpr
}

func (node *ShowStages) Format(ctx *FmtCtx) {
	ctx.WriteString("show stages ")
	if node.Like != nil {
		node.Like.Format(ctx)
	}
}
func (node *ShowStages) GetStatementType() string { return "Show Stages" }
func (node *ShowStages) GetQueryType() string     { return QueryTypeOth }
