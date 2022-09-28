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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
)

var debugInstructionNames = map[int]string{
	vm.Top:          "top",
	vm.Join:         "join",
	vm.Semi:         "semi",
	vm.Left:         "left",
	vm.Limit:        "limit",
	vm.Merge:        "merge",
	vm.Order:        "order",
	vm.Group:        "group",
	vm.Output:       "output",
	vm.Offset:       "offset",
	vm.Product:      "product",
	vm.Restrict:     "restrict",
	vm.Dispatch:     "dispatch",
	vm.Connector:    "connect",
	vm.Projection:   "projection",
	vm.Anti:         "anti",
	vm.Single:       "single",
	vm.Mark:         "mark",
	vm.LoopJoin:     "loop join",
	vm.LoopLeft:     "loop left",
	vm.LoopSemi:     "loop semi",
	vm.LoopAnti:     "loop anti",
	vm.LoopSingle:   "loop single",
	vm.MergeTop:     "merge top",
	vm.MergeLimit:   "merge limit",
	vm.MergeOrder:   "merge order",
	vm.MergeGroup:   "merge group",
	vm.MergeOffset:  "merge offset",
	vm.Deletion:     "delete",
	vm.Insert:       "insert",
	vm.Update:       "update",
	vm.External:     "external",
	vm.Minus:        "minus",
	vm.Intersect:    "intersect",
	vm.IntersectAll: "intersect all",
	vm.HashBuild:    "hash build",
}

var debugMagicNames = map[int]string{
	Merge:          "Merge",
	Normal:         "Normal",
	Remote:         "Remote",
	Parallel:       "Parallel",
	Pushdown:       "Pushdown",
	CreateDatabase: "CreateDatabase",
	CreateTable:    "CreateTable",
	CreateIndex:    "CreateIndex",
	DropDatabase:   "DropDatabase",
	DropTable:      "DropTable",
	DropIndex:      "DropIndex",
	Deletion:       "Deletion",
	Insert:         "Insert",
	Update:         "Update",
	InsertValues:   "InsertValues",
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
	getReceiverStr := func(rs []*process.WaitRegister) string {
		str := "["
		for i := range rs {
			if i != 0 {
				str += ", "
			}
			if id, ok := rmp[rs[i]]; ok {
				str += fmt.Sprintf("%d", id)
			} else {
				str += "unknown"
			}
		}
		str += "]"
		return str
	}

	// convert magic to its string name
	magicShow := func(magic int) string {
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
			return str
		}
		return "unknown"
	}

	var result string
	for i := range ss {
		str := addGap()
		receiverStr := "nil"
		if ss[i].Proc != nil {
			receiverStr = getReceiverStr(ss[i].Proc.Reg.MergeReceivers)
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
