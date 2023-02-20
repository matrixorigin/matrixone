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

// the read and write mode for a transaction.
type ReadWriteMode int

const (
	READ_WRITE_MODE_NONE ReadWriteMode = iota
	READ_WRITE_MODE_READ_ONLY
	READ_WRITE_MODE_READ_WRITE
)

// modes for a transaction
type TransactionModes struct {
	RwMode ReadWriteMode
}

func (node *TransactionModes) Format(ctx *FmtCtx) {
	switch node.RwMode {
	case READ_WRITE_MODE_READ_ONLY:
		ctx.WriteString("read only")
	case READ_WRITE_MODE_READ_WRITE:
		ctx.WriteString("read write")
	}
}

func MakeTransactionModes(rwm ReadWriteMode) TransactionModes {
	return TransactionModes{RwMode: rwm}
}

// Begin statement
type BeginTransaction struct {
	statementImpl
	Modes TransactionModes
}

func (node *BeginTransaction) Format(ctx *FmtCtx) {
	ctx.WriteString("start transaction")
	if node.Modes.RwMode != READ_WRITE_MODE_NONE {
		ctx.WriteByte(' ')
		node.Modes.Format(ctx)
	}
}

func (node *BeginTransaction) GetStatementType() string { return "Start Transaction" }
func (node *BeginTransaction) GetQueryType() string     { return QueryTypeTCL }

func NewBeginTransaction(m TransactionModes) *BeginTransaction {
	return &BeginTransaction{Modes: m}
}

type CompletionType int

const (
	COMPLETION_TYPE_NO_CHAIN CompletionType = iota
	COMPLETION_TYPE_CHAIN
	COMPLETION_TYPE_RELEASE
)

// Commit statement
type CommitTransaction struct {
	statementImpl
	Type CompletionType
}

func (node *CommitTransaction) Format(ctx *FmtCtx) {
	ctx.WriteString("commit")
}

func (node *CommitTransaction) GetStatementType() string { return "Commit" }
func (node *CommitTransaction) GetQueryType() string     { return QueryTypeTCL }

func NewCommitTransaction(t CompletionType) *CommitTransaction {
	return &CommitTransaction{Type: t}
}

// Rollback statement
type RollbackTransaction struct {
	statementImpl
	Type CompletionType
}

func (node *RollbackTransaction) Format(ctx *FmtCtx) {
	ctx.WriteString("rollback")
}

func (node *RollbackTransaction) GetStatementType() string { return "Rollback" }
func (node *RollbackTransaction) GetQueryType() string     { return QueryTypeTCL }

func NewRollbackTransaction(t CompletionType) *RollbackTransaction {
	return &RollbackTransaction{Type: t}
}
