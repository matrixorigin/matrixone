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
	"github.com/matrixorigin/matrixone/pkg/vm/message"
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
	Single
	Semi
	RightSemi
	Anti
	RightAnti
	Mark
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
	MultiUpdate

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
	Apply
)

var OperatorToStrMap map[OpType]string
var StrToOperatorMap map[string]OpType
var MinorOpMap map[string]struct{}
var MajorOpMap map[string]struct{}

func init() {
	// Initialize OperatorToStrMap
	OperatorToStrMap = map[OpType]string{
		Top:                     "Top",
		Limit:                   "Limit",
		Order:                   "Order",
		Group:                   "Group",
		Window:                  "Window",
		TimeWin:                 "TimeWin",
		Fill:                    "Fill",
		Output:                  "Output",
		Offset:                  "Offset",
		Product:                 "Product",
		Filter:                  "Filter",
		Dispatch:                "Dispatch",
		Connector:               "Connector",
		Projection:              "Projection",
		Join:                    "Join",
		LoopJoin:                "LoopJoin",
		Left:                    "Left",
		Single:                  "Single",
		Semi:                    "Semi",
		RightSemi:               "RightSemi",
		Anti:                    "Anti",
		RightAnti:               "RightAnti",
		Mark:                    "Mark",
		IndexJoin:               "IndexJoin",
		IndexBuild:              "IndexBuild",
		Merge:                   "Merge",
		MergeTop:                "MergeTop",
		MergeLimit:              "MergeLimit",
		MergeOrder:              "MergeOrder",
		MergeGroup:              "MergeGroup",
		MergeOffset:             "MergeOffset",
		MergeRecursive:          "MergeRecursive",
		MergeCTE:                "MergeCTE",
		Partition:               "Partition",
		Deletion:                "Deletion",
		Insert:                  "Insert",
		External:                "External",
		Source:                  "Source",
		Minus:                   "Minus",
		Intersect:               "Intersect",
		IntersectAll:            "IntersectAll",
		UnionAll:                "UnionAll",
		HashBuild:               "HashBuild",
		ShuffleBuild:            "ShuffleBuild",
		TableFunction:           "TableFunction",
		TableScan:               "TableScan",
		ValueScan:               "ValueScan",
		MergeBlock:              "MergeBlock",
		MergeDelete:             "MergeDelete",
		Right:                   "Right",
		OnDuplicateKey:          "OnDuplicateKey",
		FuzzyFilter:             "FuzzyFilter",
		PreInsert:               "PreInsert",
		PreInsertUnique:         "PreInsertUnique",
		PreInsertSecondaryIndex: "PreInsertSecondaryIndex",
		LastInstructionOp:       "LastInstructionOp",
		LockOp:                  "LockOp",
		Shuffle:                 "Shuffle",
		Sample:                  "Sample",
		ProductL2:               "ProductL2",
		Mock:                    "Mock",
		Apply:                   "Apply",
	}

	// Initialize StrToOperatorMap
	StrToOperatorMap = make(map[string]OpType)
	for op, str := range OperatorToStrMap {
		StrToOperatorMap[str] = op
	}

	// Initialize MinorOpMap (small impact on time consumption)
	MinorOpMap = map[string]struct{}{
		OperatorToStrMap[HashBuild]:    {},
		OperatorToStrMap[ShuffleBuild]: {},
		OperatorToStrMap[IndexBuild]:   {},
		OperatorToStrMap[Filter]:       {},
		OperatorToStrMap[MergeGroup]:   {},
		OperatorToStrMap[MergeOrder]:   {},
	}

	// Initialize MajorOpMap (large impact on time consumption)
	MajorOpMap = map[string]struct{}{
		OperatorToStrMap[TableScan]: {},
		OperatorToStrMap[External]:  {},
		OperatorToStrMap[Order]:     {},
		OperatorToStrMap[Window]:    {},
		OperatorToStrMap[Group]:     {},
		OperatorToStrMap[Join]:      {},
		OperatorToStrMap[LoopJoin]:  {},
		OperatorToStrMap[Left]:      {},
		OperatorToStrMap[Single]:    {},
		OperatorToStrMap[Semi]:      {},
		OperatorToStrMap[RightSemi]: {},
		OperatorToStrMap[Anti]:      {},
		OperatorToStrMap[RightAnti]: {},
		OperatorToStrMap[Mark]:      {},
		OperatorToStrMap[Product]:   {},
		OperatorToStrMap[ProductL2]: {},
	}
}

func (op OpType) String() string {
	switch op {
	case Top:
		return "Top"
	case Limit:
		return "Limit"
	case Order:
		return "Order"
	case Group:
		return "Group"
	case Window:
		return "Window"
	case TimeWin:
		return "TimeWin"
	case Fill:
		return "Fill"
	case Output:
		return "Output"
	case Offset:
		return "Offset"
	case Product:
		return "Product"
	case Filter:
		return "Filter"
	case Dispatch:
		return "Dispatch"
	case Connector:
		return "Connector"
	case Projection:
		return "Projection"
	case Join:
		return "Join"
	case LoopJoin:
		return "LoopJoin"
	case Left:
		return "Left"
	case Single:
		return "Single"
	case Semi:
		return "Semi"
	case RightSemi:
		return "RightSemi"
	case Anti:
		return "Anti"
	case RightAnti:
		return "RightAnti"
	case Mark:
		return "Mark"
	case IndexJoin:
		return "IndexJoin"
	case IndexBuild:
		return "IndexBuild"
	case Merge:
		return "Merge"
	case MergeTop:
		return "MergeTop"
	case MergeLimit:
		return "MergeLimit"
	case MergeOrder:
		return "MergeOrder"
	case MergeGroup:
		return "MergeGroup"
	case MergeOffset:
		return "MergeOffset"
	case MergeRecursive:
		return "MergeRecursive"
	case MergeCTE:
		return "MergeCTE"
	case Partition:
		return "Partition"
	case Deletion:
		return "Deletion"
	case Insert:
		return "Insert"
	case MultiUpdate:
		return "MultiUpdate"
	case External:
		return "External"
	case Source:
		return "Source"
	case Minus:
		return "Minus"
	case Intersect:
		return "Intersect"
	case IntersectAll:
		return "IntersectAll"
	case UnionAll:
		return "UnionAll"
	case HashBuild:
		return "HashBuild"
	case ShuffleBuild:
		return "ShuffleBuild"
	case TableFunction:
		return "TableFunction"
	case TableScan:
		return "TableScan"
	case ValueScan:
		return "ValueScan"
	case MergeBlock:
		return "MergeBlock"
	case MergeDelete:
		return "MergeDelete"
	case Right:
		return "Right"
	case OnDuplicateKey:
		return "OnDuplicateKey"
	case FuzzyFilter:
		return "FuzzyFilter"
	case PreInsert:
		return "PreInsert"
	case PreInsertUnique:
		return "PreInsertUnique"
	case PreInsertSecondaryIndex:
		return "PreInsertSecondaryIndex"
	case LastInstructionOp:
		return "LastInstructionOp"
	case LockOp:
		return "LockOp"
	case Shuffle:
		return "Shuffle"
	case Sample:
		return "Sample"
	case ProductL2:
		return "ProductL2"
	case Mock:
		return "Mock"
	case Apply:
		return "Apply"
	default:
		return "Unknown"
	}
}

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
	OpAnalyzer process.Analyzer
	Children   []Operator
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

func (o *OperatorBase) ResetChildren() {
	o.Children = o.Children[:0]
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

func ChildrenCall(op Operator, proc *process.Process, anal process.Analyzer) (CallResult, error) {
	beforeChildrenCall := time.Now()
	result, err := op.Call(proc)
	anal.ChildrenCallStop(beforeChildrenCall)
	if err == nil {
		anal.Input(result.Batch)
	}
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
	Idx     int // plan node index to which the pipeline operator belongs
	IsFirst bool
	IsLast  bool

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
	return op.OpType() == LockOp || op.OpType() == MergeRecursive || op.OpType() == MergeCTE
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
