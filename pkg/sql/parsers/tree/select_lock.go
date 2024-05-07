// Copyright 2023 Matrix Origin
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

// SelectLockType is the lock type for SelectStmt.
type SelectLockType int

// Select lock types.
const (
	SelectLockNone SelectLockType = iota
	SelectLockForUpdate
	SelectLockForShare
	SelectLockForUpdateNoWait
	SelectLockForUpdateWaitN
	SelectLockForShareNoWait
	SelectLockForUpdateSkipLocked
	SelectLockForShareSkipLocked
)

// String implements fmt.Stringer.
func (n SelectLockType) String() string {
	switch n {
	case SelectLockNone:
		return "none"
	case SelectLockForUpdate:
		return "for update"
	case SelectLockForShare:
		return "for share"
	case SelectLockForUpdateNoWait:
		return "for update nowait"
	case SelectLockForUpdateWaitN:
		return "for update wait"
	case SelectLockForShareNoWait:
		return "for share nowait"
	case SelectLockForUpdateSkipLocked:
		return "for update skip locked"
	case SelectLockForShareSkipLocked:
		return "for share skip locked"
	}
	return "unsupported select lock type"
}

type SelectLockInfo struct {
	LockType SelectLockType
	WaitSec  uint64
	Tables   []*TableName
}

func (node *SelectLockInfo) Format(ctx *FmtCtx) {
	ctx.WriteString(node.LockType.String())
}
