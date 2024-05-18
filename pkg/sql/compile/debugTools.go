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
	vm.Restrict:                "restrict",
	vm.Dispatch:                "dispatch",
	vm.Shuffle:                 "shuffle",
	vm.Connector:               "connect",
	vm.Projection:              "projection",
	vm.Anti:                    "anti",
	vm.Single:                  "single",
	vm.Mark:                    "mark",
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

// DebugShowScopes show information of a scope structure.
func DebugShowScopes(ss []*Scope) string {
	var generateReceiverMap func(*Scope, map[*process.WaitRegister]int)
	generateReceiverMap = func(s *Scope, mp map[*process.WaitRegister]int) {
		for i := range s.PreScopes {
			generateReceiverMap(s.PreScopes[i], mp)
		}
		if s.Proc == nil {
			return
		}
		for i := range s.Proc.Reg.MergeReceivers {
			mp[s.Proc.Reg.MergeReceivers[i]] = len(mp)
		}
	}

	receiverMap := make(map[*process.WaitRegister]int)
	for i := range ss {
		generateReceiverMap(ss[i], receiverMap)
	}

	return debugShowScopes(ss, 0, receiverMap)
}

func debugShowScopes(ss []*Scope, gap int, rmp map[*process.WaitRegister]int) string {
	// new line and start with n space
	gapNextLine := func() string {
		str := "\n"
		for i := 0; i < gap; i++ {
			str += " "
		}
		return str
	}

	// return n space
	addGap := func() string {
		str := ""
		for i := 0; i < gap; i++ {
			str += " "
		}
		return str
	}

	// get receiver id string
	getReceiverStr := func(s *Scope, rs []*process.WaitRegister) string {
		str := "["
		for i := range rs {
			remote := ""
			for _, u := range s.RemoteReceivRegInfos {
				if u.Idx == i {
					remote = fmt.Sprintf("(%s)", u.Uuid)
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
	magicShow := func(magic magicType) string {
		name, ok := debugMagicNames[magic]
		if ok {
			return name
		}
		return "unknown"
	}

	// explain the datasource
	showDataSource := func(source *Source) string {
		if source == nil {
			return "nil"
		}
		s := fmt.Sprintf("%s.%s%s", source.SchemaName, source.RelationName, source.Attributes)
		return strings.TrimLeft(s, ".")
	}

	// explain the operator information
	showInstruction := func(instruction vm.Instruction, mp map[*process.WaitRegister]int) string {
		id := instruction.Op
		name, ok := debugInstructionNames[id]
		if ok {
			str := name
			if id == vm.Connector {
				var receiver = "unknown"
				arg := instruction.Arg.(*connector.Argument)
				if receiverId, okk := mp[arg.Reg]; okk {
					receiver = fmt.Sprintf("%d", receiverId)
				}
				str += fmt.Sprintf(" to MergeReceiver %s", receiver)
			}
			if id == vm.Dispatch {
				arg := instruction.Arg.(*dispatch.Argument)
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
					str += fmt.Sprintf(" shuffle to all of MergeReceiver [%s].", chs)
				case dispatch.SendToAllFunc, dispatch.SendToAllLocalFunc:
					str += fmt.Sprintf(" to all of MergeReceiver [%s].", chs)
				case dispatch.SendToAnyLocalFunc:
					str += fmt.Sprintf(" to any of MergeReceiver [%s].", chs)
				default:
					str += fmt.Sprintf(" unknow type dispatch [%s].", chs)
				}

				if len(arg.RemoteRegs) != 0 {
					remoteChs := ""
					for i, reg := range arg.RemoteRegs {
						if i != 0 {
							remoteChs += ", "
						}
						remoteChs += fmt.Sprintf("[addr: %s, uuid %s]", reg.NodeAddr, reg.Uuid)
					}
					str += fmt.Sprintf(" cross-cn receiver info: %s", remoteChs)
				}
			}
			return str
		}
		return "unknown"
	}

	var result string
	for i := range ss {
		str := addGap()
		receiverStr := "nil"
		if ss[i].Proc != nil {
			receiverStr = getReceiverStr(ss[i], ss[i].Proc.Reg.MergeReceivers)
		}
		str += fmt.Sprintf("Scope %d (Magic: %s, Receiver: %s): [", i+1, magicShow(ss[i].Magic), receiverStr)
		for j, instruction := range ss[i].Instructions {
			if j != 0 {
				str += " -> "
			}
			str += showInstruction(instruction, rmp)
		}
		str += "]"
		if ss[i].DataSource != nil {
			str += gapNextLine()
			str += fmt.Sprintf("DataSource: %s,", showDataSource(ss[i].DataSource))
		}
		if len(ss[i].PreScopes) > 0 {
			str += gapNextLine()
			str += "  PreScopes: {"
			str += debugShowScopes(ss[i].PreScopes, gap+2, rmp)
			str += gapNextLine()
			str += "}"
		}
		result += "\n"
		result += str
	}
	return result
}
