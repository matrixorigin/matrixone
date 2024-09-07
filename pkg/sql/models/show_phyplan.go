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

const (
	IsFirstMask = 1 << 0 // 0001
	IsLastMask  = 1 << 1 // 0010
)

type ExplainOption int

const (
	NormalOption ExplainOption = iota
	VerboseOption
	AnalyzeOption
)

func ExplainPhyPlan(phy *PhyPlan, option ExplainOption) string {
	buffer := bytes.NewBuffer(make([]byte, 0, 300))
	if len(phy.LocalScope) > 0 || len(phy.RemoteScope) > 0 {
		fmt.Fprintf(buffer, "RetryTime: %v, S3IOInputCount: %d, S3IOOutputCount: %d", phy.RetryTime, phy.S3IOInputCount, phy.S3IOOutputCount)
	}

	if len(phy.LocalScope) > 0 {
		buffer.WriteString("\n")
		buffer.WriteString("LOCAL SCOPES:")
		for i := range phy.LocalScope {
			explainPhyScope(phy.LocalScope[i], i, 0, option, buffer)
		}
	}

	if len(phy.RemoteScope) > 0 {
		buffer.WriteString("\n")
		buffer.WriteString("REMOTE SCOPES:")
		for i := range phy.RemoteScope {
			explainPhyScope(phy.RemoteScope[i], i, 0, option, buffer)
		}
	}
	return buffer.String()
}

func explainPhyScope(scope PhyScope, index int, gap int, option ExplainOption, buffer *bytes.Buffer) {
	gapNextLine(gap, buffer)

	// Scope Header
	receiverStr := getReceiverStr(scope.Receiver)
	buffer.WriteString(fmt.Sprintf("Scope %d (Magic: %s, mcpu: %v, Receiver: %s)", index+1, scope.Magic, scope.Mcpu, receiverStr))

	// Scope DataSource
	if scope.DataSource != nil {
		gapNextLine(gap, buffer)
		buffer.WriteString(fmt.Sprintf("  DataSource: %s", showDataSource(scope.DataSource)))
	}

	if scope.RootOperator != nil {
		gapNextLine(gap, buffer)
		prefixStr := addGap(gap) + "         "
		PrintPipelineTree(scope.RootOperator, prefixStr, true, true, option, buffer)
	}

	if len(scope.PreScopes) > 0 {
		gapNextLine(gap, buffer)
		buffer.WriteString("  PreScopes: {")
		for i := range scope.PreScopes {
			explainPhyScope(scope.PreScopes[i], i, gap+4, option, buffer)
		}
		gapNextLine(gap, buffer)
		buffer.WriteString("  }")
	}
}

func PrintPipelineTree(node *PhyOperator, prefix string, isRoot, isTail bool, option ExplainOption, buffer *bytes.Buffer) {
	if node == nil {
		return
	}

	name := node.OpName

	var analyzeStr = ""
	if option == VerboseOption || option == AnalyzeOption {
		// Extract the original bool values
		isFirst := (node.Status & IsFirstMask) != 0
		isLast := (node.Status & IsLastMask) != 0
		analyzeStr = fmt.Sprintf(" (idx:%v, isFirst:%v, isLast:%v)", node.NodeIdx, isFirst, isLast)
	}
	if option == AnalyzeOption && node.OpStats != nil {
		analyzeStr += node.OpStats.ReducedString()
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
		PrintPipelineTree(node.Children[i], newPrefix, false, isLast, option, buffer)
	}

	if isRoot {
		trimLastNewline(buffer)
	}
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
