package compile

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

func (c *Compile) compileRestrict2(n *plan.Node, ss []*Scope) []*Scope {
	if len(n.FilterList) == 0 && len(n.RuntimeFilterProbeList) == 0 {
		return ss
	}
	currentFirstFlag := c.anal.isFirst
	// for dynamic parameter, substitute param ref and const fold cast expression here to improve performance
	newFilters, err := plan2.ConstandFoldList(n.FilterList, c.proc, true)
	if err != nil {
		newFilters = n.FilterList
	}
	filterExpr := colexec.RewriteFilterExprList(newFilters)
	for i := range ss {
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.Filter,
			Idx:     c.anal.curr,
			IsFirst: currentFirstFlag,
			Arg:     constructRestrict(n, filterExpr),
		})
	}
	c.anal.isFirst = false
	return ss
}

func (c *Compile) compilePreInsertUk(n *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	defer func() {
		c.anal.isFirst = false
	}()

	for i := range ss {
		preInsertUkArg := constructPreInsertUk(n, c.proc)
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.PreInsertUnique,
			Idx:     c.anal.curr,
			IsFirst: currentFirstFlag,
			Arg:     preInsertUkArg,
		})
	}
	return ss
}

func (c *Compile) compilePreInsertSK(n *plan.Node, ss []*Scope) []*Scope {
	currentFirstFlag := c.anal.isFirst
	defer func() {
		c.anal.isFirst = false
	}()

	for i := range ss {
		preInsertSkArg := constructPreInsertSk(n, c.proc)
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.PreInsertSecondaryIndex,
			Idx:     c.anal.curr,
			IsFirst: currentFirstFlag,
			Arg:     preInsertSkArg,
		})
	}
	return ss
}

func (c *Compile) compilePreInsert(ns []*plan.Node, n *plan.Node, ss []*Scope) ([]*Scope, error) {
	currentFirstFlag := c.anal.isFirst
	defer func() {
		c.anal.isFirst = false
	}()

	for i := range ss {
		preInsertArg, err := constructPreInsert(ns, n, c.e, c.proc)
		if err != nil {
			return nil, err
		}
		ss[i].appendInstruction(vm.Instruction{
			Op:      vm.PreInsert,
			Idx:     c.anal.curr,
			IsFirst: currentFirstFlag,
			Arg:     preInsertArg,
		})
	}
	return ss, nil
}

func (c *Compile) compileLockOp(n *plan.Node, ss []*Scope) ([]*Scope, error) {
	lockRows := make([]*plan.LockTarget, 0, len(n.LockTargets))
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
	if c.proc.TxnOperator.Txn().IsPessimistic() {
		block = n.LockTargets[0].Block
		if block {
			ss = []*Scope{c.newMergeScope(ss)}
		}
	}

	currentFirstFlag := c.anal.FetchAndResetFirst()
	for i := range ss {
		//var lockOpArg *lockop.Argument
		lockOpArg, err := constructLockOp(n, c.e)
		if err != nil {
			return nil, err
		}
		lockOpArg.SetBlock(block)
		if block {
			ss[i].Instructions[len(ss[i].Instructions)-1].Arg.Release()
			ss[i].Instructions[len(ss[i].Instructions)-1] = vm.Instruction{
				Op:      vm.LockOp,
				Idx:     c.anal.curr,
				IsFirst: currentFirstFlag,
				Arg:     lockOpArg,
			}
		} else {
			ss[i].appendInstruction(vm.Instruction{
				Op:      vm.LockOp,
				Idx:     c.anal.curr,
				IsFirst: currentFirstFlag,
				Arg:     lockOpArg,
			})
		}
	}
	return ss, nil
}
