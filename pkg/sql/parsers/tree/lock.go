// Copyright 2022 Matrix Origin
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
	reuse.CreatePool[TableLock](
		func() *TableLock { return &TableLock{} },
		func(l *TableLock) { l.reset() },
		reuse.DefaultOptions[TableLock](),
	)

	reuse.CreatePool[LockTableStmt](
		func() *LockTableStmt { return &LockTableStmt{} },
		func(l *LockTableStmt) { l.reset() },
		reuse.DefaultOptions[LockTableStmt](),
	)

	reuse.CreatePool[UnLockTableStmt](
		func() *UnLockTableStmt { return &UnLockTableStmt{} },
		func(u *UnLockTableStmt) { u.reset() },
		reuse.DefaultOptions[UnLockTableStmt](),
	)
}

// TableLockType is the type of the table lock.
type TableLockType int32

const (
	// TableLockNone means this table lock is absent.
	TableLockNone TableLockType = iota
	// TableLockRead means the session with this lock can read the table (but not write it).
	// Multiple sessions can acquire a READ lock for the table at the same time.
	// Other sessions can read the table without explicitly acquiring a READ lock.
	TableLockRead
	// The locks are the same as READ except that they allow INSERT commands to be executed
	TableLockReadLocal
	// TableLockWrite means only the session with this lock has write/read permission.
	// Only the session that holds the lock can access the table. No other session can access it until the lock is released.
	TableLockWrite
	// Low priority write locks
	TableLockLowPriorityWrite
)

func (t TableLockType) String() string {
	switch t {
	case TableLockNone:
		return "NONE"
	case TableLockRead:
		return "READ"
	case TableLockReadLocal:
		return "READ LOCAL"
	case TableLockWrite:
		return "WRITE"
	case TableLockLowPriorityWrite:
		return "LOW_PRIORITY WRITE"
	}
	return ""
}

type TableLock struct {
	Table    TableName
	LockType TableLockType
}

func NewTableLock(table TableName, lockType TableLockType) *TableLock {
	l := reuse.Alloc[TableLock](nil)
	l.Table = table
	l.LockType = lockType
	return l
}

func (node *TableLock) reset() {
	// node.Table.Free()
	*node = TableLock{}
}

func (node *TableLock) Free() {
	reuse.Free[TableLock](node, nil)
}

func (node TableLock) TypeName() string { return "tree.TableLock" }

type LockTableStmt struct {
	statementImpl
	TableLocks []TableLock
}

func NewLockTableStmt(tableLocks []TableLock) *LockTableStmt {
	l := reuse.Alloc[LockTableStmt](nil)
	l.TableLocks = tableLocks
	return l
}

func (node *LockTableStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("Lock Tables")
	if node.TableLocks != nil {
		prefix := " "
		for _, a := range node.TableLocks {
			ctx.WriteString(prefix)
			a.Table.Format(ctx)
			ctx.WriteString(" ")
			ctx.WriteString(a.LockType.String())
			prefix = ", "
		}
	}
}

func (node *LockTableStmt) reset() {
	if node.TableLocks != nil {
		for _, item := range node.TableLocks {
			item.Free()
		}
	}
	*node = LockTableStmt{}
}

func (node *LockTableStmt) Free() {
	reuse.Free[LockTableStmt](node, nil)
}

func (node *LockTableStmt) GetStatementType() string { return "Lock Tables" }

func (node *LockTableStmt) GetQueryType() string { return QueryTypeOth }

func (node LockTableStmt) TypeName() string { return "tree.LockTableStmt" }

type UnLockTableStmt struct {
	statementImpl
}

func NewUnLockTableStmt() *UnLockTableStmt {
	u := reuse.Alloc[UnLockTableStmt](nil)
	return u
}

func (node *UnLockTableStmt) reset() {
	*node = UnLockTableStmt{}
}

func (node *UnLockTableStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("UnLock Tables")
}

func (node *UnLockTableStmt) GetStatementType() string { return "UnLock Tables" }

func (node *UnLockTableStmt) GetQueryType() string { return QueryTypeOth }

func (node UnLockTableStmt) TypeName() string { return "tree.UnLockTableStmt" }

func (node *UnLockTableStmt) Free() {
	reuse.Free[UnLockTableStmt](node, nil)
}
