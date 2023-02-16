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

// TableLockType is the type of the table lock.
type TableLockType int

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
	Table    TableExpr
	LockType TableLockType
}

type LockTableStmt struct {
	statementImpl
	TableLocks []TableLock
}

func (node *LockTableStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("Lock Table")
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

func (node *LockTableStmt) GetStatementType() string { return "Lock Table" }
func (node *LockTableStmt) GetQueryType() string     { return QueryTypeOth }

type UnLockTableStmt struct {
	statementImpl
}

func (node *UnLockTableStmt) Format(ctx *FmtCtx) {
	ctx.WriteString("UnLock Table")
}

func (node *UnLockTableStmt) GetStatementType() string { return "UnLock Table" }
func (node *UnLockTableStmt) GetQueryType() string     { return QueryTypeOth }
