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
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[DropSnapShot](
		func() *DropSnapShot { return &DropSnapShot{} },
		func(d *DropSnapShot) { d.reset() },
		reuse.DefaultOptions[DropSnapShot](), //.
	) //WithEnableChecker()
}

type SnapshotLevel int

const (
	SNAPSHOTLEVELCLUSTER SnapshotLevel = iota
	SNAPSHOTLEVELACCOUNT
	SNAPSHOTLEVELDATABASE
	SNAPSHOTLEVELTABLE
)

func (s SnapshotLevel) String() string {
	switch s {
	case SNAPSHOTLEVELCLUSTER:
		return "cluster"
	case SNAPSHOTLEVELACCOUNT:
		return "account"
	case SNAPSHOTLEVELDATABASE:
		return "database"
	case SNAPSHOTLEVELTABLE:
		return "table"
	}
	return "unknown"
}

type SnapshotLevelType struct {
	Level SnapshotLevel
}

func (node *SnapshotLevelType) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Level.String())
}

type ObjectInfo struct {
	SLevel  SnapshotLevelType // snapshot level
	ObjName Identifier        // object name
}

func (node *ObjectInfo) Format(ctx *FmtCtx) {
	node.SLevel.Format(ctx)
	ctx.WriteString(" ")
	node.ObjName.Format(ctx)
}

type CreateSnapShot struct {
	statementImpl

	IfNotExists bool
	Name        Identifier // snapshot name
	Object      ObjectInfo
}

func (node *CreateSnapShot) Format(ctx *FmtCtx) {
	ctx.WriteString("create snapshot ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}
	node.Name.Format(ctx)
	ctx.WriteString(" for ")
	node.Object.Format(ctx)
}

func (node *CreateSnapShot) GetStatementType() string { return "Create Snapshot" }

func (node *CreateSnapShot) GetQueryType() string { return QueryTypeOth }

type DropSnapShot struct {
	statementImpl

	IfExists bool
	Name     Identifier // snapshot name
}

func (node *DropSnapShot) Free() { reuse.Free[DropSnapShot](node, nil) }

func (node *DropSnapShot) reset() { *node = DropSnapShot{} }

func (node DropSnapShot) TypeName() string { return "tree.DropSnapShot" }

func NewDropSnapShot(ifExists bool, Name Identifier) *DropSnapShot {
	drop := reuse.Alloc[DropSnapShot](nil)
	drop.IfExists = ifExists
	drop.Name = Name
	return drop
}

func (node *DropSnapShot) Format(ctx *FmtCtx) {
	ctx.WriteString("drop snapshot ")

	if node.IfExists {
		ctx.WriteString("if exists ")
	}

	node.Name.Format(ctx)
}

func (node *DropSnapShot) GetStatementType() string { return "Drop Snapshot" }

func (node *DropSnapShot) GetQueryType() string { return QueryTypeOth }

type ShowSnapShots struct {
	statementImpl
	Where *Where
}

func (node *ShowSnapShots) Format(ctx *FmtCtx) {
	ctx.WriteString("show snapshots")
	if node.Where != nil {
		ctx.WriteString(" ")
		node.Where.Format(ctx)
	}
}

func (node *ShowSnapShots) GetStatementType() string { return "Show Snapshot" }

func (node *ShowSnapShots) GetQueryType() string { return QueryTypeDQL }

type RestoreLevel int

const (
	RESTORELEVELCLUSTER RestoreLevel = iota
	RESTORELEVELACCOUNT
	RESTORELEVELDATABASE
	RESTORELEVELTABLE
)

func (s RestoreLevel) String() string {
	switch s {
	case RESTORELEVELCLUSTER:
		return "cluster"
	case RESTORELEVELACCOUNT:
		return "account"
	case RESTORELEVELDATABASE:
		return "database"
	case RESTORELEVELTABLE:
		return "table"
	}
	return "unknown"
}

type RestoreSnapShot struct {
	statementImpl

	Level         RestoreLevel
	AccountName   Identifier // account name
	DatabaseName  Identifier // database name
	TableName     Identifier // table name
	SnapShotName  Identifier // snapshot name
	ToAccountName Identifier // to account name
}

func (node *RestoreSnapShot) Format(ctx *FmtCtx) {
	ctx.WriteString("restore ")
	switch node.Level {
	case RESTORELEVELCLUSTER:
		ctx.WriteString("cluster")
	case RESTORELEVELACCOUNT:
		ctx.WriteString("account ")
		node.AccountName.Format(ctx)
	case RESTORELEVELDATABASE:
		ctx.WriteString("account ")
		node.AccountName.Format(ctx)
		ctx.WriteString(" database ")
		node.DatabaseName.Format(ctx)
	case RESTORELEVELTABLE:
		ctx.WriteString("account ")
		node.AccountName.Format(ctx)
		ctx.WriteString(" database ")
		node.DatabaseName.Format(ctx)
		ctx.WriteString(" table ")
		node.TableName.Format(ctx)
	}

	ctx.WriteString(" from snapshot ")
	node.SnapShotName.Format(ctx)

	if len(node.ToAccountName) > 0 {
		ctx.WriteString(" to account ")
		node.ToAccountName.Format(ctx)
	}
}

func (node *RestoreSnapShot) GetStatementType() string { return "Restore Snapshot" }

func (node *RestoreSnapShot) GetQueryType() string { return QueryTypeOth }
