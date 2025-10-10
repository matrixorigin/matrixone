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

	reuse.CreatePool[SnapshotDiff](
		func() *SnapshotDiff {
			return &SnapshotDiff{}
		},
		func(c *SnapshotDiff) {
			c.reset()
		},
		reuse.DefaultOptions[SnapshotDiff](),
	)

	reuse.CreatePool[SnapshotMerge](
		func() *SnapshotMerge {
			return &SnapshotMerge{}
		},
		func(c *SnapshotMerge) {
			c.reset()
		},
		reuse.DefaultOptions[SnapshotMerge](),
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

type SnapshotDiff struct {
	statementImpl

	TargetTable TableName
	BaseTable   TableName
}

func (s *SnapshotDiff) TypeName() string {
	//TODO implement me
	panic("implement me")
}

func (s *SnapshotDiff) reset() {
	*s = SnapshotDiff{}
}

func NewSnapshotDiff() *SnapshotDiff {
	return reuse.Alloc[SnapshotDiff](nil)
}

func (s *SnapshotDiff) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (s *SnapshotDiff) Format(ctx *FmtCtx) {
	//TODO implement me
	panic("implement me")
}

func (s *SnapshotDiff) String() string {
	return s.GetStatementType()
}

func (s *SnapshotDiff) GetStatementType() string {
	return "snapshot diff"
}

func (s *SnapshotDiff) GetQueryType() string {
	return QueryTypeOth
}

func (s *SnapshotDiff) Free() {
	reuse.Free[SnapshotDiff](s, nil)
}

type ConflictOpt struct {
	Opt int
}
type SnapshotMerge struct {
	statementImpl
	SrcTable    TableName
	DstTable    TableName
	ConflictOpt *ConflictOpt
}

func (s *SnapshotMerge) TypeName() string {
	//TODO implement me
	panic("implement me")
}

func (s *SnapshotMerge) reset() {
	*s = SnapshotMerge{}
}

func NewSnapshotMerge() *SnapshotMerge {
	return reuse.Alloc[SnapshotMerge](nil)
}

func (s *SnapshotMerge) StmtKind() StmtKind {
	return frontendStatusTyp
}

func (s *SnapshotMerge) Format(ctx *FmtCtx) {
	//TODO implement me
	panic("implement me")
}

func (s *SnapshotMerge) String() string {
	return s.GetStatementType()
}

func (s *SnapshotMerge) GetStatementType() string {
	return "snapshot diff"
}

func (s *SnapshotMerge) GetQueryType() string {
	return QueryTypeOth
}

func (s *SnapshotMerge) Free() {
	reuse.Free[SnapshotMerge](s, nil)
}
