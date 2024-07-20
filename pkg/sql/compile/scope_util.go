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

package compile

import (
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergedelete"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func (c *Compile) compileDelete(n *plan.Node, ss []*Scope) ([]*Scope, error) {
	var arg *deletion.Deletion
	arg, err := constructDeletion(n, c.e)
	if err != nil {
		return nil, err
	}

	currentFirstFlag := c.anal.isFirst
	arg.SetIdx(c.anal.curNodeIdx)
	arg.SetIsFirst(currentFirstFlag)
	c.anal.isFirst = false

	if n.Stats.Cost*float64(SingleLineSizeEstimate) > float64(DistributedThreshold) && !arg.DeleteCtx.CanTruncate {
		rs := c.newDeleteMergeScope(arg, ss)
		rs.Magic = MergeDelete

		mergeDeleteArg := mergedelete.NewArgument().
			WithObjectRef(arg.DeleteCtx.Ref).
			WithParitionNames(arg.DeleteCtx.PartitionTableNames).
			WithEngine(c.e).
			WithAddAffectedRows(arg.DeleteCtx.AddAffectedRows)

		currentFirstFlag = c.anal.isFirst
		mergeDeleteArg.SetIdx(c.anal.curNodeIdx)
		mergeDeleteArg.SetIsFirst(currentFirstFlag)
		rs.setRootOperator(mergeDeleteArg)
		c.anal.isFirst = false

		ss = []*Scope{rs}
		arg.Release()
		return ss, nil
	} else {
		var rs *Scope
		if c.IsSingleScope(ss) {
			rs = ss[0]
		} else {
			rs = c.newMergeScope(ss)
			rs.Magic = Merge
		}

		rs.setRootOperator(arg)
		ss = []*Scope{rs}
		return ss, nil
	}
}

func (c *Compile) compileOnduplicateKey(n *plan.Node, ss []*Scope) ([]*Scope, error) {
	rs := c.newMergeScope(ss)
	arg := constructOnduplicateKey(n, c.e)

	currentFirstFlag := c.anal.isFirst
	arg.SetIdx(c.anal.curNodeIdx)
	arg.SetIsFirst(currentFirstFlag)
	rs.ReplaceLeafOp(arg)
	c.anal.isFirst = false

	ss = []*Scope{rs}
	return ss, nil
}

func (c *Compile) compileLock(n *plan.Node, ss []*Scope) ([]*Scope, error) {
	lockRows := make([]*plan2.LockTarget, 0, len(n.LockTargets))
	for _, tbl := range n.LockTargets {
		if tbl.LockTable {
			c.lockTables[tbl.TableId] = tbl
		} else {
			if _, ok := c.lockTables[tbl.TableId]; !ok {
				lockRows = append(lockRows, tbl)
			}
		}
	}
	n.LockTargets = lockRows
	if len(n.LockTargets) == 0 {
		return ss, nil
	}

	block := false
	// only pessimistic txn needs to block downstream operators.
	if c.proc.GetTxnOperator().Txn().IsPessimistic() {
		block = n.LockTargets[0].Block
		if block {
			ss = []*Scope{c.newMergeScope(ss)}
		}
	}

	currentFirstFlag := c.anal.isFirst
	for i := range ss {
		var err error
		var lockOpArg *lockop.LockOp
		lockOpArg, err = constructLockOp(n, c.e)
		if err != nil {
			return nil, err
		}
		lockOpArg.SetBlock(block)
		lockOpArg.Idx = c.anal.curNodeIdx
		lockOpArg.IsFirst = currentFirstFlag
		if block {
			// 此处的逻辑是，替换root算子，建议实现一个ReplaceRootOp()函数
			lockOpArg.SetChildren(ss[i].RootOp.GetOperatorBase().Children)
			ss[i].RootOp.Release()
			ss[i].RootOp = lockOpArg
		} else {
			ss[i].doSetRootOperator(lockOpArg)
		}
	}
	c.anal.isFirst = false
	return ss, nil
}

// ----------------------------------------------------------------------------------------------------------------------
// Node expand mode when compiling plan scope
type NodeExpandType int

// type of scope
const (
	Merge1 NodeExpandType = 0
	Merge2 NodeExpandType = 1
)

// 1.Serial multi opertor linear, such as:
/*
-------@operatorA----->operatorB#---->
*/
const SerialMulOp NodeExpandType = 0

// 2.Serial single node linear, such as:
/*
--------@operatorA#------>
*/
const SerialSingleOp NodeExpandType = 1

// 3.ParallelMergeSingleComplete: NODE: MINUS, INTERSECT, INTERSECT_ALL
// NODE: MINUS, INTERSECT, INTERSECT_ALL
/*
---------\
           ----> @operator# ---->
---------/
*/
const ParallelMergeSingleComplete NodeExpandType = 2

// 4.Parallel left start merge end, such as:
// Join（Inner Join） ,LoopJoin ,Left ,LoopLeft ,Single ,LoopSingle ,Semi
// RightSemi ,LoopSemi ,Anti ,RightAnti ,LoopAnti ,Mark ,LoopMark ,Right
/*
---@leftOperator---\
	                ---> operator#
-------------------/
*/
const ParallelLeftStartMergeEnd NodeExpandType = 3
