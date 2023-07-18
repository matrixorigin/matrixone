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

import "fmt"

type CreateStage struct {
	statementImpl
	IfNotExists bool
	Name        Identifier
	Url         string
	Credentials StageCredentials
	Status      StageStatus
	Comment     StageComment
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

func (node *CreateStage) GetStatementType() string { return "Create Stage" }
func (node *CreateStage) GetQueryType() string     { return QueryTypeOth }

type DropStage struct {
	statementImpl
	IfNotExists bool
	Name        Identifier
}

func (node *DropStage) Format(ctx *FmtCtx) {
	ctx.WriteString("drop stage ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	node.Name.Format(ctx)
}

func (node *DropStage) GetStatementType() string { return "Drop Stage" }
func (node *DropStage) GetQueryType() string     { return QueryTypeOth }

type AlterStage struct {
	statementImpl
	IfNotExists       bool
	Name              Identifier
	UrlOption         StageUrl
	CredentialsOption StageCredentials
	StatusOption      StageStatus
	Comment           StageComment
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

func (node *AlterStage) GetStatementType() string { return "Alter Stage" }
func (node *AlterStage) GetQueryType() string     { return QueryTypeOth }

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
