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

package compile

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"strconv"
	"strings"
)

func explainPhyPlan(plan *PhyPlan) string {
	buffer := bytes.NewBuffer(make([]byte, 0, 300))

	buffer.WriteString(fmt.Sprintf("VERSION: %s", plan.Version))
	buffer.WriteString("\n")

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
	buffer.WriteString(fmt.Sprintf("Scope %d (Magic: %s, mcpu: x, Receiver: %s)", index+1, magicShow(scope.Magic), receiverStr))

	// Scope DataSource
	if scope.DataSource != nil {
		gapNextLine(gap, buffer)
		showDataSource(scope.DataSource)
		buffer.WriteString(fmt.Sprintf("  DataSource: %s", showDataSource(scope.DataSource)))
	}

	if scope.RootOperator != nil {
		gapNextLine(gap, buffer)
		prefixStr := addGap(gap) + "         "
		PrintPipelineTreeV1(scope.RootOperator, prefixStr, true, true, buffer)
	}

	if len(scope.PreScopes) > 0 {
		gapNextLine(gap, buffer)
		buffer.WriteString(fmt.Sprintf("  PreScopes: {"))
		for i := range scope.PreScopes {
			explainPhyScope(scope.PreScopes[i], i, gap+4, buffer)
		}
		gapNextLine(gap, buffer)
		buffer.WriteString(fmt.Sprintf("  }"))
	}
}

func PrintPipelineTreeV1(node *PhyOperator, prefix string, isRoot, isTail bool, buffer *bytes.Buffer) {
	if node == nil {
		return
	}

	nameId := node.OpName
	id, err := strconv.Atoi(nameId)
	if err != nil {
		panic(fmt.Sprintf("转换错误:", err))
	}

	name, ok := debugInstructionNames[vm.OpType(id)]
	if !ok {
		name = "unknown"
	}

	//------------------------------------------------------------------------
	var analyzeStr = ""
	if true {
		analyzeStr = fmt.Sprintf("(idx:%v, isFirst:%v, isLast:%v)", node.NodeIdx, node.IsFirst, node.IsLast)

		//if node.OpStats != nil {
		//	analyzeStr += node.OpStats.String()
		//}
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
	id, err := strconv.Atoi(name)
	if err != nil {
		panic(fmt.Sprintf("转换错误:", err))
	}

	if vm.OpType(id) == vm.Connector {
		if len(node.DestReceiver) != 1 {
			panic(fmt.Sprintf("Connector节点的DestReceiver数量不为1"))
		}
		buffer.WriteString(fmt.Sprintf(" to MergeReceiver %v", node.DestReceiver[0].Idx))
	}

	if vm.OpType(id) == vm.Dispatch {
		buffer.WriteString(fmt.Sprintf(" to Receiver [%v]", node.DestReceiver[0]))
		buffer.WriteString("[")
		for i := range node.DestReceiver {
			if i != 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(fmt.Sprintf("%v", node.DestReceiver[i].Idx))
		}
		buffer.WriteString("]")
	}
}
