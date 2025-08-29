// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[CloneTable](
		func() *CloneTable {
			return &CloneTable{
				CreateTable: CreateTable{},
			}
		},
		func(c *CloneTable) { c.reset() },
		reuse.DefaultOptions[CloneTable](), //.
	) //WithEnableChecker()

	reuse.CreatePool[CloneDatabase](
		func() *CloneDatabase {
			return &CloneDatabase{}
		},
		func(c *CloneDatabase) { c.reset() },
		reuse.DefaultOptions[CloneDatabase](), //.
	) //WithEnableChecker()
}

type CloneLevelCtxKey struct{}
type CloneLevelType uint64
type CloneStmtType int

const (
	NormalCloneLevelTable CloneLevelType = 1 << iota
	NormalCloneLevelDatabase
	NormalCloneLevelAccount
	NormalCloneLevelCluster

	RestoreCloneLevelTable
	RestoreCloneLevelDatabase
	RestoreCloneLevelAccount
	RestoreCloneLevelCluster
)

const (
	NoClone CloneStmtType = iota
	CloneCluster
	CloneAccount
	WithinAccCloneDB
	BetweenAccCloneDB
	WithinDBCloneTable
	WithinAccBetweenDBCloneTable
	BetweenAccCloneTable
)

type ToAccountOpt struct {
	AccountName Identifier
}

type CloneTable struct {
	statementImpl

	SrcTable    TableName
	CreateTable CreateTable

	IsRestore     bool
	IsRestoreByTS bool
	FromAccount   uint32

	ToAccountOpt *ToAccountOpt
	ToAccountId  uint32

	Sql      string
	StmtType CloneStmtType

	stmtKind StmtKind
}

func (node *CloneTable) StmtKind() StmtKind {
	return node.stmtKind
}

func (node *CloneTable) FlipStmtKind() {
	node.stmtKind = defaultStatusTyp
}

func (node *CloneTable) Format(ctx *FmtCtx) {
	ctx.WriteString("clone table: ")
	node.CreateTable.Format(ctx)
}

func (node *CloneTable) GetStatementType() string { return "CREATE TABLE CLONE" }
func (node *CloneTable) GetQueryType() string     { return QueryTypeOth }

func NewCloneTable() *CloneTable {
	clone := reuse.Alloc[CloneTable](nil)
	clone.stmtKind = frontendStatusTyp
	return clone
}

func (node *CloneTable) TypeName() string { return "tree.CloneTable" }

func (node *CloneTable) Free() {
	reuse.Free[CloneTable](node, nil)
}

func (node *CloneTable) reset() {
	node.CreateTable.reset()
	*node = CloneTable{}
}

//////////////////////////// clone database /////////////////////

type CloneDatabase struct {
	statementImpl
	SrcDatabase  Identifier
	DstDatabase  Identifier
	AtTsExpr     *AtTimeStamp
	ToAccountOpt *ToAccountOpt
}

func (node *CloneDatabase) Free() {
	reuse.Free[CloneDatabase](node, nil)
}

func (node *CloneDatabase) reset() {
	*node = CloneDatabase{}
}

func (node *CloneDatabase) TypeName() string { return "tree.CloneDatabase" }

func (node *CloneDatabase) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (node *CloneDatabase) Format(ctx *FmtCtx) {
	ctx.WriteString(fmt.Sprintf(
		"create database %s clone %s",
		node.SrcDatabase.String(), node.DstDatabase.String()))
}

func NewCloneDatabase() *CloneDatabase {
	return reuse.Alloc[CloneDatabase](nil)
}

func (node *CloneDatabase) GetStatementType() string { return "CREATE DATABASE CLONE" }

func (node *CloneDatabase) GetQueryType() string { return QueryTypeOth }

func DecideCloneStmtType(
	ctx context.Context,
	stmt *CloneTable,
	srcDbName string,
	dstDbName string,
	toAccount uint32,
	srcAccount uint32,
) (cloneType CloneStmtType) {

	if stmt.StmtType != NoClone {
		return stmt.StmtType
	}

	var (
		level = NormalCloneLevelTable
	)

	if val := ctx.Value(CloneLevelCtxKey{}); val != nil {
		level = val.(CloneLevelType)
	}

	switch level {
	case NormalCloneLevelCluster, RestoreCloneLevelCluster:
		return CloneCluster
	case NormalCloneLevelAccount, RestoreCloneLevelAccount:
		return CloneAccount
	case NormalCloneLevelDatabase, RestoreCloneLevelDatabase:
		if srcAccount == toAccount {
			return WithinAccCloneDB
		}
		return BetweenAccCloneDB

	case NormalCloneLevelTable, RestoreCloneLevelTable:
		if srcAccount == toAccount {
			if srcDbName == dstDbName {
				return WithinDBCloneTable
			}
			return WithinAccBetweenDBCloneTable
		}
		return BetweenAccCloneTable

	default:
		return NoClone
	}
}
