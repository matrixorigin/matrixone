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

package vm

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type OpType int

const (
	Top OpType = iota
	Limit
	Order
	Group
	Window
	TimeWin
	Fill
	Output
	Offset
	Product
	Filter
	Dispatch
	Connector
	Projection

	Join
	LoopJoin
	Left
	LoopLeft
	Single
	LoopSingle
	Semi
	RightSemi
	LoopSemi
	Anti
	RightAnti
	LoopAnti
	Mark
	LoopMark
	IndexJoin
	IndexBuild

	Merge
	MergeTop
	MergeLimit
	MergeOrder
	MergeGroup
	MergeOffset
	MergeRecursive
	MergeCTE
	Partition

	Deletion
	Insert
	External
	Source

	Minus
	Intersect
	IntersectAll
	UnionAll

	HashBuild
	ShuffleBuild

	TableFunction
	TableScan
	ValueScan
	// MergeBlock is used to recieve S3 block metLoc Info, and write
	// them to S3
	MergeBlock
	// MergeDelete is used to recieve S3 Blcok Delete Info from remote Cn
	MergeDelete
	Right
	OnDuplicateKey
	FuzzyFilter
	PreInsert
	PreInsertUnique
	PreInsertSecondaryIndex
	// LastInstructionOp is not a true operator and must set at last.
	// It was used by unit testing to ensure that
	// all functions related to instructions can reach 100% coverage.
	LastInstructionOp

	// LockOp is used to add locks to lockservice for pessimistic transactions.
	// Operator that encounters a write conflict will block until the previous
	// transaction has released the lock
	LockOp

	Shuffle

	Sample
	ProductL2
	Mock
)

type Operator interface {
	// Free release all the memory allocated from mPool in an operator.
	// pipelineFailed marks the process status of the pipeline when the method is called.
	Free(proc *process.Process, pipelineFailed bool, err error)

	// Reset clean all the memory that can be reused.
	Reset(proc *process.Process, pipelineFailed bool, err error)

	// String returns the string representation of an operator.
	String(buf *bytes.Buffer)

	// OpType returns the OpType of an operator.
	OpType() OpType

	//Prepare prepares an operator for execution.
	Prepare(proc *process.Process) error

	//Call calls an operator.
	Call(proc *process.Process) (CallResult, error)

	//Release an operator
	Release()

	// OperatorBase methods
	SetInfo(info *OperatorInfo)
	AppendChild(child Operator)

	GetOperatorBase() *OperatorBase
}

type OperatorBase struct {
	OperatorInfo
	Children []Operator
}

func (o *OperatorBase) SetInfo(info *OperatorInfo) {
	o.OperatorInfo = *info
}

func (o *OperatorBase) NumChildren() int {
	return len(o.Children)
}

func (o *OperatorBase) AppendChild(child Operator) {
	o.Children = append(o.Children, child)
}

func (o *OperatorBase) SetChild(child Operator, idx int) {
	o.Children[idx] = child
}

func (o *OperatorBase) SetChildren(children []Operator) {
	o.Children = children
}

func (o *OperatorBase) GetChildren(idx int) Operator {
	return o.Children[idx]
}

func (o *OperatorBase) GetCnAddr() string {
	return o.CnAddr
}

func (o *OperatorBase) SetCnAddr(cnAddr string) {
	o.CnAddr = cnAddr
}

func (o *OperatorBase) GetOperatorID() int32 {
	return o.OperatorID
}

func (o *OperatorBase) SetOperatorID(operatorID int32) {
	o.OperatorID = operatorID
}

func (o *OperatorBase) GetParalleID() int32 {
	return o.ParallelID
}

func (o *OperatorBase) SetParalleID(paralledID int32) {
	o.ParallelID = paralledID
}

func (o *OperatorBase) GetMaxParallel() int32 {
	return o.MaxParallel
}

func (o *OperatorBase) SetMaxParallel(maxParallel int32) {
	o.MaxParallel = maxParallel
}

func (o *OperatorBase) GetIdx() int {
	return o.Idx
}

func (o *OperatorBase) SetIdx(idx int) {
	o.Idx = idx
}

func (o *OperatorBase) GetParallelIdx() int {
	return o.ParallelIdx
}

func (o *OperatorBase) GetParallelMajor() bool {
	return o.ParallelMajor
}

func (o *OperatorBase) GetIsFirst() bool {
	return o.IsFirst
}

func (o *OperatorBase) SetIsFirst(isFirst bool) {
	o.IsFirst = isFirst
}

func (o *OperatorBase) GetIsLast() bool {
	return o.IsLast
}

func (o *OperatorBase) SetIsLast(isLast bool) {
	o.IsLast = isLast
}

func (o *OperatorBase) SetAnalyzeControl(nodeIdx int, isFirst bool) {
	o.Idx = nodeIdx
	o.IsFirst = isFirst
}

var CancelResult = CallResult{
	Status: ExecStop,
}

func CancelCheck(proc *process.Process) (error, bool) {
	select {
	case <-proc.Ctx.Done():
		return proc.Ctx.Err(), true
	default:
		return nil, false
	}
}

func ChildrenCall(o Operator, proc *process.Process, anal process.Analyze) (CallResult, error) {
	beforeChildrenCall := time.Now()
	result, err := o.Call(proc)
	anal.ChildrenCallStop(beforeChildrenCall)
	return result, err
}

type ExecStatus int

const (
	ExecStop ExecStatus = iota
	ExecNext
	ExecHasMore
)

type CtrState int

const (
	Build CtrState = iota
	Eval
	End
)

type CallResult struct {
	Status ExecStatus
	Batch  *batch.Batch
}

func NewCallResult() CallResult {
	return CallResult{
		Status: ExecNext,
	}
}

type OperatorInfo struct {
	Idx           int // plan node index to which the pipeline operator belongs
	ParallelIdx   int
	ParallelMajor bool
	IsFirst       bool
	IsLast        bool

	CnAddr      string
	OperatorID  int32
	ParallelID  int32
	MaxParallel int32
}

func (info OperatorInfo) GetAddress() message.MessageAddress {
	return message.MessageAddress{
		CnAddr:     info.CnAddr,
		OperatorID: info.OperatorID,
		ParallelID: info.ParallelID,
	}
}

func CannotRemote(op Operator) bool {
	// todo: I think we should add more operators here.
	return op.OpType() == LockOp
}

type ModificationArgument interface {
	AffectedRows() uint64
}

// doHandleAllOp function uses post traversal to recursively process nodes in the operand tree.
// In post traversal, all child nodes are recursively processed first, and then the current node is processed.
func doHandleAllOp(parentOp Operator, op Operator, opHandle func(parentOp Operator, op Operator) error) (err error) {
	if op == nil {
		return nil
	}
	numChildren := op.GetOperatorBase().NumChildren()

	for i := 0; i < numChildren; i++ {
		if err = doHandleAllOp(op, op.GetOperatorBase().GetChildren(i), opHandle); err != nil {
			return err
		}
	}
	return opHandle(parentOp, op)
}

func HandleAllOp(rootOp Operator, opHandle func(parentOp Operator, op Operator) error) (err error) {
	return doHandleAllOp(nil, rootOp, opHandle)
}

func HandleLeafOp(parentOp Operator, op Operator, opHandle func(leafOpParent Operator, leafOp Operator) error) (err error) {
	if op == nil {
		return nil
	}
	numChildren := op.GetOperatorBase().NumChildren()
	if numChildren == 0 {
		return opHandle(parentOp, op)
	}
	for i := 0; i < numChildren; i++ {
		if err := HandleLeafOp(op, op.GetOperatorBase().GetChildren(i), opHandle); err != nil {
			return err
		}
	}
	return nil
}

// suppose that the op tree is like a list, only one leaf child
func GetLeafOp(op Operator) Operator {
	if op == nil {
		return nil
	}
	if op.GetOperatorBase().NumChildren() == 0 {
		return op
	}
	return GetLeafOp(op.GetOperatorBase().GetChildren(0))
}

// suppose that the op tree is like a list, only one leaf child
func GetLeafOpParent(parentOp Operator, op Operator) Operator {
	if op == nil {
		return nil
	}
	if op.GetOperatorBase().NumChildren() == 0 {
		return parentOp
	}
	return GetLeafOpParent(op, op.GetOperatorBase().GetChildren(0))
}
