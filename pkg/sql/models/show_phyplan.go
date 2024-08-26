// Copyright 2024 Matrix Origin
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

package models

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/vm"
)

var debugInstructionNames = map[vm.OpType]string{
	vm.Top:                     "top",
	vm.Join:                    "join",
	vm.Semi:                    "semi",
	vm.RightSemi:               "right semi",
	vm.RightAnti:               "right anti",
	vm.Left:                    "left",
	vm.Right:                   "right",
	vm.Limit:                   "limit",
	vm.Merge:                   "merge",
	vm.Order:                   "order",
	vm.Group:                   "group",
	vm.Output:                  "output",
	vm.Offset:                  "offset",
	vm.Product:                 "product",
	vm.ProductL2:               "product l2",
	vm.Filter:                  "filter",
	vm.Dispatch:                "dispatch",
	vm.Shuffle:                 "shuffle",
	vm.Connector:               "connect",
	vm.Projection:              "projection",
	vm.Anti:                    "anti",
	vm.Single:                  "single",
	vm.Mark:                    "mark",
	vm.IndexJoin:               "index join",
	vm.LoopJoin:                "loop join",
	vm.LoopLeft:                "loop left",
	vm.LoopSemi:                "loop semi",
	vm.LoopAnti:                "loop anti",
	vm.LoopSingle:              "loop single",
	vm.LoopMark:                "loop mark",
	vm.MergeTop:                "merge top",
	vm.MergeLimit:              "merge limit",
	vm.MergeOrder:              "merge order",
	vm.MergeGroup:              "merge group",
	vm.MergeOffset:             "merge offset",
	vm.MergeRecursive:          "merge recursive",
	vm.MergeCTE:                "merge cte",
	vm.Partition:               "partition",
	vm.Deletion:                "delete",
	vm.Insert:                  "insert",
	vm.PreInsert:               "pre insert",
	vm.PreInsertUnique:         "pre insert uk",
	vm.PreInsertSecondaryIndex: "pre insert 2nd",
	vm.External:                "external",
	vm.Source:                  "source",
	vm.Minus:                   "minus",
	vm.Intersect:               "intersect",
	vm.IntersectAll:            "intersect all",
	vm.UnionAll:                "union all",
	vm.HashBuild:               "hash build",
	vm.ShuffleBuild:            "shuffle build",
	vm.IndexBuild:              "index build",
	vm.MergeDelete:             "merge delete",
	vm.LockOp:                  "lockop",
	vm.MergeBlock:              "merge block",
	vm.FuzzyFilter:             "fuzzy filter",
	vm.Sample:                  "sample",
	vm.Window:                  "window",
	vm.TimeWin:                 "timewin",
	vm.Fill:                    "fill",
	vm.TableScan:               "tablescan",
	vm.ValueScan:               "valuescan",
	vm.TableFunction:           "tablefunction",
	vm.OnDuplicateKey:          "on duplicate key",
}

type magicType int

// type of scope
const (
	Merge magicType = iota
	Normal
	Remote
	Parallel
	CreateDatabase
	CreateTable
	CreateView
	CreateIndex
	DropDatabase
	DropTable
	DropIndex
	TruncateTable
	AlterView
	AlterTable
	MergeInsert
	MergeDelete
	CreateSequence
	DropSequence
	AlterSequence
	Replace
)

var debugMagicNames = map[magicType]string{
	Merge:          "Merge",
	Normal:         "Normal",
	Remote:         "Remote",
	Parallel:       "Parallel",
	CreateDatabase: "CreateDatabase",
	CreateTable:    "CreateTable",
	CreateIndex:    "CreateIndex",
	DropDatabase:   "DropDatabase",
	DropTable:      "DropTable",
	DropIndex:      "DropIndex",
	MergeDelete:    "MergeDelete",
	MergeInsert:    "MergeInsert",
}

func ExplainPhyPlan(plan *PhyPlan) string {
	buffer := bytes.NewBuffer(make([]byte, 0, 300))
	fmt.Fprintf(buffer, "Version: %s, S3IOInputCount: %d, S3IOOutputCount: %d\n", plan.Version, plan.S3IOInputCount, plan.S3IOOutputCount)
	//------------------------------------------------------------------------------------------------------------------
	buffer.WriteString("LOCAL SCOPES:")

	for i := range plan.LocalScope {
		explainPhyScope(plan.LocalScope[i], i, 0, buffer)
	}

	//------------------------------------------------------------------------------------------------------------------
	if len(plan.RemoteScope) > 0 {
		buffer.WriteString("\n")
		buffer.WriteString("REMOTE SCOPES:")
	}

	for i := range plan.RemoteScope {
		explainPhyScope(plan.RemoteScope[i], i, 0, buffer)
	}
	//------------------------------------------------------------------------------------------------------------------
	return buffer.String()
}

func explainPhyScope(scope PhyScope, index int, gap int, buffer *bytes.Buffer) {
	gapNextLine(gap, buffer)

	// Scope Header
	receiverStr := getReceiverStr(scope.Receiver)
	buffer.WriteString(fmt.Sprintf("Scope %d (Magic: %s, mcpu: x, Receiver: %s)", index+1, scope.Magic, receiverStr))

	// Scope DataSource
	if scope.DataSource != nil {
		gapNextLine(gap, buffer)
		buffer.WriteString(fmt.Sprintf("  DataSource: %s", showDataSource(scope.DataSource)))
	}

	if scope.RootOperator != nil {
		gapNextLine(gap, buffer)
		prefixStr := addGap(gap) + "         "
		PrintPipelineTreeV1(scope.RootOperator, prefixStr, true, true, buffer)
	}

	if len(scope.PreScopes) > 0 {
		gapNextLine(gap, buffer)
		buffer.WriteString("  PreScopes: {")
		for i := range scope.PreScopes {
			explainPhyScope(scope.PreScopes[i], i, gap+4, buffer)
		}
		gapNextLine(gap, buffer)
		buffer.WriteString("  }")
	}
}

func PrintPipelineTreeV1(node *PhyOperator, prefix string, isRoot, isTail bool, buffer *bytes.Buffer) {
	if node == nil {
		return
	}

	name := node.OpName
	//------------------------------------------------------------------------
	var analyzeStr = ""
	if true {
		analyzeStr = fmt.Sprintf("(idx:%v, isFirst:%v, isLast:%v)", node.NodeIdx, node.IsFirst, node.IsLast)

		if node.OpStats != nil {
			analyzeStr += node.OpStats.ReducedString()
		}
	}

	// Write to the current node
	if isRoot {
		headPrefix := "  Pipeline: └── "
		buffer.WriteString(fmt.Sprintf("%s%s%s", headPrefix, name, analyzeStr))
		handleTailNodeReceiver(node, buffer)
		buffer.WriteString("\n")
		// Ensure that child nodes are properly indented
		prefix += "   "
	} else {
		if isTail {
			buffer.WriteString(fmt.Sprintf("%s└── %s%s", prefix, name, analyzeStr))
			handleTailNodeReceiver(node, buffer)
			buffer.WriteString("\n")
		} else {
			buffer.WriteString(fmt.Sprintf("%s├── %s%s\n", prefix, name, analyzeStr))
		}
	}

	// Calculate new prefix
	newPrefix := prefix
	if isTail {
		newPrefix += "    "
	} else {
		newPrefix += "│   "
	}

	// Write to child node
	for i := 0; i < len(node.Children); i++ {
		isLast := i == len(node.Children)-1
		PrintPipelineTreeV1(node.Children[i], newPrefix, false, isLast, buffer)
	}

	if isRoot {
		trimLastNewline(buffer)
	}
}

// convert magic to its string name
func magicShow(magic magicType) string {
	name, ok := debugMagicNames[magic]
	if ok {
		return name
	}
	return "unknown"
}

// get receiver id string
func getReceiverStr(rsr []PhyReceiver) string {
	str := "["
	for i := range rsr {
		if i != 0 {
			str += ", "
		}
		str += fmt.Sprintf("%d%s", rsr[i].Idx, rsr[i].RemoteUuid)
	}
	str += "]"
	return str
}

// explain the datasource
func showDataSource(source *PhySource) string {
	s := fmt.Sprintf("%s.%s%s", source.SchemaName, source.RelationName, source.Attributes)
	return strings.TrimLeft(s, ".")
}

func gapNextLine(gap int, buffer *bytes.Buffer) {
	buffer.WriteString("\n")
	for i := 0; i < gap; i++ {
		buffer.WriteString(" ")
	}
}

// return n space
func addGap(gap int) string {
	str := ""
	for i := 0; i < gap; i++ {
		str += " "
	}
	return str
}

func handleTailNodeReceiver(node *PhyOperator, buffer *bytes.Buffer) {
	if node == nil {
		return
	}

	name := node.OpName
	if name == vm.Connector.String() {
		if len(node.DestReceiver) != 1 {
			panic(fmt.Sprintf("The number of DestReceivers of the Connector operator is not 1"))
		}
		buffer.WriteString(fmt.Sprintf(" to MergeReceiver %v", node.DestReceiver[0].Idx))
	}

	if name == vm.Dispatch.String() {
		buffer.WriteString(" to Receiver [")
		for i := range node.DestReceiver {
			if i != 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(fmt.Sprintf("%v", node.DestReceiver[i].Idx))
		}
		buffer.WriteString("]")
	}
}

func trimLastNewline(buf *bytes.Buffer) {
	data := buf.Bytes()
	if len(data) > 0 && data[len(data)-1] == '\n' {
		buf.Truncate(len(data) - 1)
	}
}
