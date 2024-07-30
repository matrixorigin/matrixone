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
	reuse.CreatePool[DropPitr](
		func() *DropPitr { return &DropPitr{} },
		func(d *DropPitr) { d.reset() },
		reuse.DefaultOptions[DropPitr](), //.
	) //WithEnableChecker()

	reuse.CreatePool[AlterPitr](
		func() *AlterPitr { return &AlterPitr{} },
		func(d *AlterPitr) { d.reset() },
		reuse.DefaultOptions[AlterPitr](), //.
	) //WithEnableChecker()
}

type PitrLevel int

const (
	PITRLEVELCLUSTER PitrLevel = iota
	PITRLEVELACCOUNT
	PITRLEVELDATABASE
	PITRLEVELTABLE
)

func (s PitrLevel) String() string {
	switch s {
	case PITRLEVELCLUSTER:
		return "cluster"
	case PITRLEVELACCOUNT:
		return "account"
	case PITRLEVELDATABASE:
		return "database"
	case PITRLEVELTABLE:
		return "table"
	}
	return "unknown"
}

type ShowPitr struct {
	statementImpl
	Where *Where
}

func (node *ShowPitr) Format(ctx *FmtCtx) {
	ctx.WriteString("show pitr")
	if node.Where != nil {
		ctx.WriteString(" ")
		node.Where.Format(ctx)
	}
}

func (node *ShowPitr) GetStatementType() string { return "Show PITR" }
func (node *ShowPitr) GetQueryType() string     { return QueryTypeDQL }

type DropPitr struct {
	statementImpl

	IfExists bool
	Name     Identifier // pitr name
}

func (node *DropPitr) Free() { reuse.Free[DropPitr](node, nil) }
func (node *DropPitr) Format(ctx *FmtCtx) {
	ctx.WriteString("drop pitr ")
	if node.IfExists {
		ctx.WriteString("if exists ")
	}
	node.Name.Format(ctx)
}

func (node *DropPitr) reset() { *node = DropPitr{} }

func (node DropPitr) TypeName() string { return "tree.DropPitr" }

func NewDropPitr(ifExists bool, Name Identifier) *DropPitr {
	drop := reuse.Alloc[DropPitr](nil)
	drop.IfExists = ifExists
	drop.Name = Name
	return drop
}

func (node *DropPitr) GetStatementType() string { return "Drop PITR" }
func (node *DropPitr) GetQueryType() string     { return QueryTypeOth }

type CreatePitr struct {
	statementImpl

	IfNotExists bool
	Name        Identifier // pitr name

	Level        PitrLevel  // pitr level
	AccountName  Identifier // account name
	DatabaseName Identifier // database name
	TableName    Identifier // table name

	PitrValue int64
	PitrUnit  string
}

func (node *CreatePitr) Format(ctx *FmtCtx) {
	ctx.WriteString("create pitr ")
	if node.IfNotExists {
		ctx.WriteString("if not exists ")
	}

	node.Name.Format(ctx)
	ctx.WriteString(" for ")
	switch node.Level {
	case PITRLEVELCLUSTER:
		ctx.WriteString("cluster")
	case PITRLEVELACCOUNT:
		if len(node.AccountName) != 0 {
			ctx.WriteString("account ")
			node.AccountName.Format(ctx)
		} else {
			ctx.WriteString("self account")

		}
	case PITRLEVELDATABASE:
		ctx.WriteString("database ")
		node.DatabaseName.Format(ctx)
	case PITRLEVELTABLE:
		ctx.WriteString("database ")
		node.DatabaseName.Format(ctx)
		ctx.WriteString(" table ")
		node.TableName.Format(ctx)
	}

	ctx.WriteString(" range ")
	ctx.WriteString(fmt.Sprintf("%v ", node.PitrValue))
	ctx.WriteString(" ")
	ctx.WriteString(node.PitrUnit)
}

func (node *CreatePitr) GetStatementType() string { return "Create PITR" }
func (node *CreatePitr) GetQueryType() string     { return QueryTypeDDL }

type AlterPitr struct {
	statementImpl

	IfExists bool
	Name     Identifier // pitr name

	PitrValue int64
	PitrUnit  string
}

func (node *AlterPitr) Format(ctx *FmtCtx) {
	ctx.WriteString("alter pitr ")
	if node.IfExists {
		ctx.WriteString("if exists ")
	}
	node.Name.Format(ctx)
	ctx.WriteString(" range ")
	ctx.WriteString(fmt.Sprintf("%v ", node.PitrValue))
	ctx.WriteString(" ")
	ctx.WriteString(node.PitrUnit)
}

func (node *AlterPitr) Free() { reuse.Free[AlterPitr](node, nil) }

func (node *AlterPitr) reset() { *node = AlterPitr{} }

func (node AlterPitr) TypeName() string { return "tree.AlterPitr" }

func NewAlterPitr(ifExists bool, Name Identifier, PitrValue int64, PitrUnit string) *AlterPitr {
	alter := reuse.Alloc[AlterPitr](nil)
	alter.IfExists = ifExists
	alter.Name = Name
	alter.PitrValue = PitrValue
	alter.PitrUnit = PitrUnit
	return alter
}

func (node *AlterPitr) GetStatementType() string { return "Alter PITR" }
func (node *AlterPitr) GetQueryType() string     { return QueryTypeOth }

type RestorePitr struct {
	statementImpl

	Level RestoreLevel // restore level

	Name Identifier // pitr name

	AccountName  Identifier // account name
	DatabaseName Identifier // database name
	TableName    Identifier // table name

	SrcAccountName Identifier // source account name

	TimeStamp string
}

func (node *RestorePitr) Format(ctx *FmtCtx) {
	ctx.WriteString("restore ")

	switch node.Level {
	case RESTORELEVELCLUSTER:
		ctx.WriteString("cluster")
	case RESTORELEVELACCOUNT:
		if len(node.AccountName) != 0 {
			ctx.WriteString("account ")
			node.AccountName.Format(ctx)
		} else {
			ctx.WriteString("self account")
		}
	case RESTORELEVELDATABASE:
		ctx.WriteString("database ")
		node.DatabaseName.Format(ctx)
	case RESTORELEVELTABLE:
		ctx.WriteString("database ")
		node.DatabaseName.Format(ctx)
		ctx.WriteString(" table ")
		node.TableName.Format(ctx)
	}

	ctx.WriteString(" from pitr ")
	node.Name.Format(ctx)
	ctx.WriteString(" timestamp = ")
	ctx.WriteString(node.TimeStamp)
	if len(node.SrcAccountName) != 0 {
		ctx.WriteString(" from account ")
		node.SrcAccountName.Format(ctx)
	}
}

func (node *RestorePitr) GetStatementType() string { return "Restore PITR" }
func (node *RestorePitr) GetQueryType() string     { return QueryTypeOth }
