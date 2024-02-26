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

import "github.com/matrixorigin/matrixone/pkg/common/reuse"

func init() {
	reuse.CreatePool[DropSnapShot](
		func() *DropSnapShot { return &DropSnapShot{} },
		func(d *DropSnapShot) { d.reset() },
		reuse.DefaultOptions[DropSnapShot]().
			WithEnableChecker())
}

type SnapshotLevel int

const (
	SNAPSHOTLEVELCLUSTER SnapshotLevel = iota
	SNAPSHOTLEVELACCOUNT
	SNAPSHOTLEVELDATABASE
	SNAPSHOTLEVELTABLE
)

type SnapshotLevelType struct {
	Level SnapshotLevel
}

func (node *SnapshotLevelType) Format(ctx *FmtCtx) {
	switch node.Level {
	case SNAPSHOTLEVELCLUSTER:
		ctx.WriteString("cluster")
	case SNAPSHOTLEVELACCOUNT:
		ctx.WriteString("account")
	case SNAPSHOTLEVELDATABASE:
		ctx.WriteString("database")
	case SNAPSHOTLEVELTABLE:
		ctx.WriteString("table")
	}
}

type AccountOption struct {
	HasActOpt   bool
	AccountName Identifier
}

type ObejectInfo struct {
	sLevel  SnapshotLevelType // snapshot level
	objName Identifier        // object name
}

func (node *ObejectInfo) Format(ctx *FmtCtx) {
	node.sLevel.Format(ctx)
	ctx.WriteString(" ")
	node.objName.Format(ctx)
}

type CreateSnapShot struct {
	statementImpl

	IfNotExists bool
	sName       Identifier // snapshot name
	obejectInfo ObejectInfo
	accountOpt  AccountOption // account option
}

func (node *CreateSnapShot) Format(ctx *FmtCtx) {
	ctx.WriteString("create snapshot ")

	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}

	node.sName.Format(ctx)
	ctx.WriteString(" on ")
	node.obejectInfo.Format(ctx)
	if node.accountOpt.HasActOpt {
		ctx.WriteString(" for ")
		node.accountOpt.AccountName.Format(ctx)
	}
}

func (node *CreateSnapShot) GetStatementType() string { return "Create Snapshot" }

func (node *CreateSnapShot) GetQueryType() string { return QueryTypeOth }

type DropSnapShot struct {
	statementImpl

	IfExists   bool
	sName      Identifier    // snapshot name
	accountOpt AccountOption // account option
}

func (node *DropSnapShot) Free() { reuse.Free[DropSnapShot](node, nil) }

func (node *DropSnapShot) reset() { *node = DropSnapShot{} }

func (node DropSnapShot) TypeName() string { return "tree.DropSnapShot" }

func NewDropSnapShot(ifExists bool, sName Identifier, accountOpt AccountOption) *DropSnapShot {
	drop := reuse.Alloc[DropSnapShot](nil)
	drop.IfExists = ifExists
	drop.sName = sName
	drop.accountOpt = accountOpt
	return drop
}

func (node *DropSnapShot) Format(ctx *FmtCtx) {
	ctx.WriteString("drop snapshot ")

	if node.IfExists {
		ctx.WriteString("if exists ")
	}

	node.sName.Format(ctx)
	if node.accountOpt.HasActOpt {
		ctx.WriteString(" for ")
		node.accountOpt.AccountName.Format(ctx)
	}
}

func (node *DropSnapShot) GetStatementType() string { return "Drop Snapshot" }

func (node *DropSnapShot) GetQueryType() string { return QueryTypeOth }

type ShowSnapShot struct {
	statementImpl
	Where *Where
}

func (node *ShowSnapShot) Format(ctx *FmtCtx) {
	ctx.WriteString("show snapshot")
	if node.Where != nil {
		ctx.WriteString(" ")
		node.Where.Format(ctx)
	}
}

func (node *ShowSnapShot) GetStatementType() string { return "Show Snapshot" }

func (node *ShowSnapShot) GetQueryType() string { return QueryTypeDQL }
