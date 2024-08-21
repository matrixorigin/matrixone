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
	"bytes"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

var _ = DebugShowScopes

// DebugShowScopes generates and returns a string representation of debugging information for a set of scopes.
func DebugShowScopes(ss []*Scope) string {
	receiverMap := make(map[*process.WaitRegister]int)
	for i := range ss {
		genReceiverMap(ss[i], receiverMap)
	}

	return showScopes(ss, 0, receiverMap)
}

// genReceiverMap recursively traverses the Scope tree and generates unique identifiers (integers) for
// each WaitRegister in Scope.
func genReceiverMap(s *Scope, mp map[*process.WaitRegister]int) {
	for i := range s.PreScopes {
		genReceiverMap(s.PreScopes[i], mp)
	}
	if s.Proc == nil {
		return
	}
	for i := range s.Proc.Reg.MergeReceivers {
		mp[s.Proc.Reg.MergeReceivers[i]] = len(mp)
	}
}

// showScopes generates and returns a string representation of a set of Scopes. It recursively calls the
// showSingleScope function to construct strings for each Scope and concatenates all results together.
func showScopes(scopes []*Scope, gap int, rmp map[*process.WaitRegister]int) string {
	buffer := bytes.NewBuffer(make([]byte, 0, 300))
	for i := range scopes {
		showSingleScope(scopes[i], i, 0, rmp, buffer)
	}
	return buffer.String()
}

// showSingleScope generates and outputs a string representation of a single Scope.
// It includes header information of Scope, data source information, and pipeline tree information.
// In addition, it recursively displays information from any PreScopes.
func showSingleScope(scope *Scope, index int, gap int, rmp map[*process.WaitRegister]int, buffer *bytes.Buffer) {
	gapNextLine(gap, buffer)

	// Scope Header
	receiverStr := getReceiverStr(scope, scope.Proc.Reg.MergeReceivers, rmp)
	buffer.WriteString(fmt.Sprintf("Scope %d (Magic: %s, mcpu: %v, Receiver: %s)", index+1, magicShow(scope.Magic), scope.NodeInfo.Mcpu, receiverStr))

	// Scope DataSource
	if scope.DataSource != nil {
		gapNextLine(gap, buffer)
		showDataSource(scope.DataSource)
		buffer.WriteString(fmt.Sprintf("  DataSource: %s", showDataSource(scope.DataSource)))
	}

	if scope.RootOp != nil {
		gapNextLine(gap, buffer)
		prefixStr := addGap(gap) + "         "
		PrintPipelineTree(scope.RootOp, prefixStr, true, true, rmp, buffer)
	}

	if len(scope.PreScopes) > 0 {
		gapNextLine(gap, buffer)
		buffer.WriteString("  PreScopes: {")
		for i := range scope.PreScopes {
			showSingleScope(scope.PreScopes[i], i, gap+4, rmp, buffer)
		}
		gapNextLine(gap, buffer)
		buffer.WriteString("  }")
	}
}

func PrintPipelineTree(node vm.Operator, prefix string, isRoot bool, isTail bool, mp map[*process.WaitRegister]int, buffer *bytes.Buffer) {
	if node == nil {
		return
	}

	id := node.OpType()
	name, ok := debugInstructionNames[id]
	if !ok {
		name = "unknown"
	}

	// Write to the current node
	if isRoot {
		headPrefix := "  Pipeline: └── "
		buffer.WriteString(fmt.Sprintf("%s%s", headPrefix, name))
		hanldeTailNodeReceiver(node, mp, buffer)
		buffer.WriteString("\n")
		// Ensure that child nodes are properly indented
		prefix += "   "
	} else {
		if isTail {
			buffer.WriteString(fmt.Sprintf("%s└── %s", prefix, name))
			hanldeTailNodeReceiver(node, mp, buffer)
			buffer.WriteString("\n")
		} else {
			buffer.WriteString(fmt.Sprintf("%s├── %s\n", prefix, name))
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
	for i := 0; i < len(node.GetOperatorBase().Children); i++ {
		isLast := i == len(node.GetOperatorBase().Children)-1
		PrintPipelineTree(node.GetOperatorBase().GetChildren(i), newPrefix, false, isLast, mp, buffer)
	}

	if isRoot {
		trimLastNewline(buffer)
	}
}

func hanldeTailNodeReceiver(node vm.Operator, mp map[*process.WaitRegister]int, buffer *bytes.Buffer) {
	if node == nil {
		return
	}

	id := node.OpType()
	if id == vm.Connector {
		var receiver = "unknown"
		arg := node.(*connector.Connector)
		if receiverId, okk := mp[arg.Reg]; okk {
			receiver = fmt.Sprintf("%d", receiverId)
		}
		buffer.WriteString(fmt.Sprintf(" to MergeReceiver %s", receiver))
	}
	if id == vm.Dispatch {
		arg := node.(*dispatch.Dispatch)
		chs := ""
		for i := range arg.LocalRegs {
			if i != 0 {
				chs += ", "
			}
			if receiverId, okk := mp[arg.LocalRegs[i]]; okk {
				chs += fmt.Sprintf("%d", receiverId)
			} else {
				chs += "unknown"
			}
		}
		switch arg.FuncId {
		case dispatch.ShuffleToAllFunc:
			buffer.WriteString(fmt.Sprintf(" shuffle to all of MergeReceiver [%s]", chs))
		case dispatch.SendToAllFunc, dispatch.SendToAllLocalFunc:
			buffer.WriteString(fmt.Sprintf(" to all of MergeReceiver [%s]", chs))
		case dispatch.SendToAnyLocalFunc:
			buffer.WriteString(fmt.Sprintf(" to any of MergeReceiver [%s]", chs))
		default:
			buffer.WriteString(fmt.Sprintf(" unknow type dispatch [%s]", chs))
		}

		if len(arg.RemoteRegs) != 0 {
			remoteChs := ""
			for i, reg := range arg.RemoteRegs {
				if i != 0 {
					remoteChs += ", "
				}
				buffer.WriteString(fmt.Sprintf("[addr: %s, uuid %s]", reg.NodeAddr, reg.Uuid))
			}
			buffer.WriteString(fmt.Sprintf(" cross-cn receiver info: %s", remoteChs))
		}

		if len(arg.RemoteRegs) != 0 {
			remoteChs := ""
			for i, reg := range arg.RemoteRegs {
				if i != 0 {
					remoteChs += ", "
				}
				uuidStr := reg.Uuid.String()
				buffer.WriteString(fmt.Sprintf("[addr: %s(%s)]", reg.NodeAddr, uuidStr[len(uuidStr)-6:]))
			}
			buffer.WriteString(fmt.Sprintf(" cross-cn receiver info: %s", remoteChs))
		}
	}
}
func trimLastNewline(buf *bytes.Buffer) {
	data := buf.Bytes()
	if len(data) > 0 && data[len(data)-1] == '\n' {
		buf.Truncate(len(data) - 1)
	}
}

func gapNextLine(gap int, buffer *bytes.Buffer) {
	buffer.WriteString("\n")
	for i := 0; i < gap; i++ {
		buffer.WriteString(" ")
	}
}

// get receiver id string
func getReceiverStr(s *Scope, rs []*process.WaitRegister, rmp map[*process.WaitRegister]int) string {
	str := "["
	for i := range rs {
		remote := ""
		for _, u := range s.RemoteReceivRegInfos {
			if u.Idx == i {
				uuidStr := u.Uuid.String()
				remote = fmt.Sprintf("(%s)", uuidStr[len(uuidStr)-6:])
				break
			}
		}
		if i != 0 {
			str += ", "
		}
		if id, ok := rmp[rs[i]]; ok {
			str += fmt.Sprintf("%d%s", id, remote)
		} else {
			str += "unknown"
		}
	}
	str += "]"
	return str
}

// convert magic to its string name
func magicShow(magic magicType) string {
	name, ok := debugMagicNames[magic]
	if ok {
		return name
	}
	return "unknown"
}

func showDataSource(source *Source) string {
	if source == nil {
		return "nil"
	}
	s := fmt.Sprintf("%s.%s%s", source.SchemaName, source.RelationName, source.Attributes)
	return strings.TrimLeft(s, ".")
}

// return n space
func addGap(gap int) string {
	str := ""
	for i := 0; i < gap; i++ {
		str += " "
	}
	return str
}
