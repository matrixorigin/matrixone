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

import "github.com/matrixorigin/matrixone/pkg/common/reuse"

// SelectLockType is the lock type for SelectStmt.
type SelectLockType int

func init() {
	reuse.CreatePool[SelectLockInfo](
		func() *SelectLockInfo { return &SelectLockInfo{} },
		func(s *SelectLockInfo) { s.reset() },
		reuse.DefaultOptions[SelectLockInfo](),
	)
}

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

func NewSelectLockInfo(lt SelectLockType) *SelectLockInfo {
	s := reuse.Alloc[SelectLockInfo](nil)
	s.LockType = lt
	return s
}

func (node *SelectLockInfo) Format(ctx *FmtCtx) {
	ctx.WriteString(node.LockType.String())
}

func (node *SelectLockInfo) Free() {
	reuse.Free[SelectLockInfo](node, nil)
}

func (node SelectLockInfo) TypeName() string { return "tree.SelectLockInfo" }

func (node *SelectLockInfo) reset() {
	// if node.Tables != nil {
	// for _, item := range node.Tables {
	// 	item.Free()
	// }
	// }
	*node = SelectLockInfo{}
}
