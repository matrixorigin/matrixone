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

import "github.com/matrixorigin/matrixone/pkg/common/reuse"

func init() {
	reuse.CreatePool[DataBranchCreateTable](
		func() *DataBranchCreateTable {
			return &DataBranchCreateTable{
				CreateTable: CreateTable{},
			}
		},
		func(c *DataBranchCreateTable) { c.reset() },
		reuse.DefaultOptions[DataBranchCreateTable](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DataBranchCreateDatabase](
		func() *DataBranchCreateDatabase {
			return &DataBranchCreateDatabase{}
		},
		func(c *DataBranchCreateDatabase) { c.reset() },
		reuse.DefaultOptions[DataBranchCreateDatabase](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DataBranchDeleteTable](
		func() *DataBranchDeleteTable {
			return &DataBranchDeleteTable{}
		},
		func(c *DataBranchDeleteTable) { c.reset() },
		reuse.DefaultOptions[DataBranchDeleteTable](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DataBranchDeleteDatabase](
		func() *DataBranchDeleteDatabase {
			return &DataBranchDeleteDatabase{}
		},
		func(c *DataBranchDeleteDatabase) { c.reset() },
		reuse.DefaultOptions[DataBranchDeleteDatabase](), //.
	) //WithEnableChecker()

	reuse.CreatePool[DataBranchDiff](
		func() *DataBranchDiff {
			return &DataBranchDiff{}
		},
		func(c *DataBranchDiff) {
			c.reset()
		},
		reuse.DefaultOptions[DataBranchDiff](),
	)

	reuse.CreatePool[DataBranchMerge](
		func() *DataBranchMerge {
			return &DataBranchMerge{}
		},
		func(c *DataBranchMerge) {
			c.reset()
		},
		reuse.DefaultOptions[DataBranchMerge](),
	)

	reuse.CreatePool[DataBranchPick](
		func() *DataBranchPick {
			return &DataBranchPick{}
		},
		func(c *DataBranchPick) {
			c.reset()
		},
		reuse.DefaultOptions[DataBranchPick](),
	)

}

type DataBranchType int

const (
	DataBranch_CreateTable DataBranchType = iota
	DataBranch_CreateDatabase
	DataBranch_DeleteTable
	DataBranch_DeleteDatabase
)

//type DataBranch interface {
//	String() string
//	StmtKind() StmtKind
//	Format(ctx *FmtCtx)
//	GetStatementType() string
//	GetQueryType() string
//	TypeName() string
//	Free()
//
//	DataBranchType() DataBranchType
//}

type DataBranchCreateTable struct {
	statementImpl
	ToAccountOpt *ToAccountOpt
	SrcTable     TableName
	CreateTable  CreateTable
}

func NewDataBranchCreateTable() *DataBranchCreateTable {
	return reuse.Alloc[DataBranchCreateTable](nil)
}

func (d *DataBranchCreateTable) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (d *DataBranchCreateTable) Format(ctx *FmtCtx) {
	//TODO implement me
	panic("implement me")
}

func (d *DataBranchCreateTable) String() string {
	return d.GetStatementType()
}

func (d *DataBranchCreateTable) GetStatementType() string {
	return "Data Branch Create Table"
}

func (d *DataBranchCreateTable) GetQueryType() string {
	return QueryTypeOth
}

func (d *DataBranchCreateTable) TypeName() string {
	//TODO implement me
	panic("implement me")
}

func (d *DataBranchCreateTable) Free() {
	reuse.Free[DataBranchCreateTable](d, nil)
}

func (d *DataBranchCreateTable) DataBranchType() DataBranchType {
	return DataBranch_CreateTable
}

func (d *DataBranchCreateTable) reset() {
	d.CreateTable.reset()
	*d = DataBranchCreateTable{}
}

type DataBranchDeleteTable struct {
	statementImpl
	TableName TableName
}

func NewDataBranchDeleteTable() *DataBranchDeleteTable {
	return reuse.Alloc[DataBranchDeleteTable](nil)
}

func (d *DataBranchDeleteTable) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (d *DataBranchDeleteTable) Format(ctx *FmtCtx) {
	//TODO implement me
	panic("implement me")
}

func (d *DataBranchDeleteTable) String() string {
	return d.GetStatementType()
}

func (d *DataBranchDeleteTable) GetStatementType() string {
	return "Data Branch Delete Table"
}

func (d *DataBranchDeleteTable) GetQueryType() string {
	return QueryTypeOth
}

func (d *DataBranchDeleteTable) TypeName() string {
	//TODO implement me
	panic("implement me")
}

func (d *DataBranchDeleteTable) Free() {
	reuse.Free[DataBranchDeleteTable](d, nil)
}

func (d *DataBranchDeleteTable) reset() {
	*d = DataBranchDeleteTable{}
}

func (d *DataBranchDeleteTable) DataBranchType() DataBranchType {
	return DataBranch_DeleteTable
}

type DataBranchCreateDatabase struct {
	CloneDatabase
}

func NewDataBranchCreateDatabase() *DataBranchCreateDatabase {
	return reuse.Alloc[DataBranchCreateDatabase](nil)
}

func (d *DataBranchCreateDatabase) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (d *DataBranchCreateDatabase) Format(ctx *FmtCtx) {
	//TODO implement me
	panic("implement me")
}

func (d *DataBranchCreateDatabase) String() string {
	return d.GetStatementType()
}

func (d *DataBranchCreateDatabase) GetStatementType() string {
	return "Data Branch Create Database"
}

func (d *DataBranchCreateDatabase) GetQueryType() string {
	return QueryTypeOth
}

func (d *DataBranchCreateDatabase) TypeName() string {
	//TODO implement me
	panic("implement me")
}

func (d *DataBranchCreateDatabase) Free() {
	reuse.Free[DataBranchCreateDatabase](d, nil)
}

func (d *DataBranchCreateDatabase) reset() {
	*d = DataBranchCreateDatabase{}
}

func (d *DataBranchCreateDatabase) DataBranchType() DataBranchType {
	return DataBranch_CreateDatabase
}

type DataBranchDeleteDatabase struct {
	statementImpl
	DatabaseName Identifier
}

func NewDataBranchDeleteDatabase() *DataBranchDeleteDatabase {
	return reuse.Alloc[DataBranchDeleteDatabase](nil)
}

func (d *DataBranchDeleteDatabase) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (d *DataBranchDeleteDatabase) Format(ctx *FmtCtx) {
	//TODO implement me
	panic("implement me")
}

func (d *DataBranchDeleteDatabase) String() string {
	return d.GetStatementType()
}

func (d *DataBranchDeleteDatabase) GetStatementType() string {
	return "Data Branch Delete Database"
}

func (d *DataBranchDeleteDatabase) GetQueryType() string {
	return QueryTypeOth
}

func (d *DataBranchDeleteDatabase) TypeName() string {
	//TODO implement me
	panic("implement me")
}

func (d *DataBranchDeleteDatabase) Free() {
	reuse.Free[DataBranchDeleteDatabase](d, nil)
}

func (d *DataBranchDeleteDatabase) reset() {
	*d = DataBranchDeleteDatabase{}
}

func (d *DataBranchDeleteDatabase) DataBranchType() DataBranchType {
	return DataBranch_DeleteDatabase
}

/////////////////////////////////////////////////////////////////

const (
	CONFLICT_FAIL = iota
	CONFLICT_SKIP
	CONFLICT_ACCEPT
)

type DiffOutputOpt struct {
	// store the result to another table called `As`
	As      TableName
	Limit   *int64
	Count   bool
	Summary bool
	DirPath string
}

type DataBranchDiff struct {
	statementImpl

	TargetTable TableName
	BaseTable   TableName
	OutputOpt   *DiffOutputOpt
}

func (s *DataBranchDiff) TypeName() string {
	//TODO implement me
	panic("implement me")
}

func (s *DataBranchDiff) reset() {
	*s = DataBranchDiff{}
}

func NewDataBranchDiff() *DataBranchDiff {
	return reuse.Alloc[DataBranchDiff](nil)
}

func (s *DataBranchDiff) StmtKind() StmtKind {
	return compositeResRowType
}

func (s *DataBranchDiff) Format(ctx *FmtCtx) {
	//TODO implement me
	panic("implement me")
}

func (s *DataBranchDiff) String() string {
	return s.GetStatementType()
}

func (s *DataBranchDiff) GetStatementType() string {
	return "branch diff"
}

func (s *DataBranchDiff) GetQueryType() string {
	return QueryTypeOth
}

func (s *DataBranchDiff) Free() {
	reuse.Free[DataBranchDiff](s, nil)
}

type ConflictOpt struct {
	Opt int
}

type DataBranchMerge struct {
	statementImpl
	SrcTable    TableName
	DstTable    TableName
	ConflictOpt *ConflictOpt
}

func (s *DataBranchMerge) TypeName() string {
	//TODO implement me
	panic("implement me")
}

func (s *DataBranchMerge) reset() {
	*s = DataBranchMerge{}
}

func NewDataBranchMerge() *DataBranchMerge {
	return reuse.Alloc[DataBranchMerge](nil)
}

func (s *DataBranchMerge) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (s *DataBranchMerge) Format(ctx *FmtCtx) {
	//TODO implement me
	panic("implement me")
}

func (s *DataBranchMerge) String() string {
	return s.GetStatementType()
}

func (s *DataBranchMerge) GetStatementType() string {
	return "branch merge"
}

func (s *DataBranchMerge) GetQueryType() string {
	return QueryTypeOth
}

func (s *DataBranchMerge) Free() {
	reuse.Free[DataBranchMerge](s, nil)
}

// PickKeysType distinguishes how the user specified the key set.
type PickKeysType int

const (
	PickKeysValues   PickKeysType = iota // KEYS (1, 2, 3) or KEYS ((1,'a'), (2,'b'))
	PickKeysSubquery                     // KEYS (SELECT pk FROM ...)
	PickKeysBetween                      // BETWEEN SNAPSHOT sp1 AND sp2
)

// PickKeys represents the KEYS clause of a PICK statement.
type PickKeys struct {
	Type     PickKeysType
	KeyExprs []Expr         // for PickKeysValues: literal expressions
	Select   *Select        // for PickKeysSubquery
}

// DataBranchPick represents:
//
//	DATA BRANCH PICK <src_table> INTO <dst_table>
//	  [ KEYS ( <value_list> | <subquery> ) ]
//	  [ BETWEEN SNAPSHOT sp1 AND sp2 ]
//	  [WHEN CONFLICT FAIL|SKIP|ACCEPT]
type DataBranchPick struct {
	statementImpl
	SrcTable    TableName
	DstTable    TableName
	Keys        *PickKeys
	BetweenFrom string // snapshot name for BETWEEN SNAPSHOT sp1 AND sp2
	BetweenTo   string // snapshot name for BETWEEN SNAPSHOT sp1 AND sp2
	ConflictOpt *ConflictOpt
}

func (s *DataBranchPick) TypeName() string {
	return "DataBranchPick"
}

func (s *DataBranchPick) reset() {
	*s = DataBranchPick{}
}

func NewDataBranchPick() *DataBranchPick {
	return reuse.Alloc[DataBranchPick](nil)
}

func (s *DataBranchPick) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (s *DataBranchPick) Format(ctx *FmtCtx) {
	ctx.WriteString("data branch pick ")
	s.SrcTable.Format(ctx)
	ctx.WriteString(" into ")
	s.DstTable.Format(ctx)
	if s.BetweenFrom != "" && s.BetweenTo != "" {
		ctx.WriteString(" between snapshot ")
		ctx.WriteString(s.BetweenFrom)
		ctx.WriteString(" and ")
		ctx.WriteString(s.BetweenTo)
	}
	if s.Keys != nil {
		ctx.WriteString(" keys (")
		if s.Keys.Type == PickKeysSubquery && s.Keys.Select != nil {
			s.Keys.Select.Format(ctx)
		} else {
			for i, e := range s.Keys.KeyExprs {
				if i > 0 {
					ctx.WriteString(", ")
				}
				e.Format(ctx)
			}
		}
		ctx.WriteByte(')')
	}
	if s.ConflictOpt != nil {
		ctx.WriteString(" when conflict ")
		switch s.ConflictOpt.Opt {
		case CONFLICT_FAIL:
			ctx.WriteString("fail")
		case CONFLICT_SKIP:
			ctx.WriteString("skip")
		case CONFLICT_ACCEPT:
			ctx.WriteString("accept")
		}
	}
}

func (s *DataBranchPick) String() string {
	return s.GetStatementType()
}

func (s *DataBranchPick) GetStatementType() string {
	return "branch pick"
}

func (s *DataBranchPick) GetQueryType() string {
	return QueryTypeOth
}

func (s *DataBranchPick) Free() {
	reuse.Free[DataBranchPick](s, nil)
}
