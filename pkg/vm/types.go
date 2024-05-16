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
	Restrict
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
)

// Instruction contains relational algebra
type Instruction struct {
	// Op specified the operator code of an instruction.
	Op OpType
	// Idx specified the analysis information index.
	Idx int
	// Arg contains the operand of this instruction.
	Arg Operator

	// flag for analyzeInfo record the row information
	IsFirst bool
	IsLast  bool

	CnAddr      string
	OperatorID  int32
	ParallelID  int32
	MaxParallel int32
}

type Operator interface {
	// Free release all the memory allocated from mPool in an operator.
	// pipelineFailed marks the process status of the pipeline when the method is called.
	Free(proc *process.Process, pipelineFailed bool, err error)

	// Reset clean all the memory that can be reused.
	Reset(proc *process.Process, pipelineFailed bool, err error)

	// String returns the string representation of an operator.
	String(buf *bytes.Buffer)

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

func (o *OperatorBase) SetChildren(children []Operator) {
	o.Children = children
}

func (o *OperatorBase) GetChildren(idx int) Operator {
	return o.Children[idx]
}

func (o *OperatorBase) GetCnAddr() string {
	return o.CnAddr
}

func (o *OperatorBase) GetOperatorID() int32 {
	return o.OperatorID
}

func (o *OperatorBase) GetParalleID() int32 {
	return o.ParallelID
}

func (o *OperatorBase) GetMaxParallel() int32 {
	return o.MaxParallel
}

func (o *OperatorBase) GetIdx() int {
	return o.Idx
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

func (o *OperatorBase) GetIsLast() bool {
	return o.IsLast
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
	Idx           int
	ParallelIdx   int
	ParallelMajor bool
	IsFirst       bool
	IsLast        bool

	CnAddr      string
	OperatorID  int32
	ParallelID  int32
	MaxParallel int32
}

func (info OperatorInfo) GetAddress() process.MessageAddress {
	return process.MessageAddress{
		CnAddr:     info.CnAddr,
		OperatorID: info.OperatorID,
		ParallelID: info.ParallelID,
	}
}

type Instructions []Instruction

func (ins *Instruction) IsBrokenNode() bool {
	switch ins.Op {
	case Order, MergeOrder, Partition:
		return true
	case Limit, MergeLimit:
		return true
	case Offset, MergeOffset:
		return true
	case Group, MergeGroup:
		return true
	case Sample:
		return true
	case Top, MergeTop:
		return true
	case Window:
		return true
	case TimeWin, Fill:
		return true
	case MergeRecursive:
		return true
	}
	return false
}

func (ins *Instruction) CannotRemote() bool {
	// todo: I think we should add more operators here.
	return ins.Op == LockOp
}

type ModificationArgument interface {
	AffectedRows() uint64
}
