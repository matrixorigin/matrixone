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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (ins Instruction) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (ins Instruction) UnmarshalBinary(_ []byte) error {
	return nil
}

// String range instructions and call each operator's string function to show a query plan
func String(ins Instructions, buf *bytes.Buffer) {
	for i, in := range ins {
		if i > 0 {
			buf.WriteString(" -> ")
		}
		stringFunc[in.Op](in.Arg, buf)
	}
}

// Prepare range instructions and do init work for each operator's argument by calling its prepare function
func Prepare(ins Instructions, proc *process.Process) error {
	for _, in := range ins {
		if err := prepareFunc[in.Op](proc, in.Arg); err != nil {
			return err
		}
	}
	return nil
}

var debugInstructionNames = map[int]string{
	Top:          "top",
	Join:         "join",
	Semi:         "semi",
	Left:         "left",
	Limit:        "limit",
	Merge:        "merge",
	Order:        "order",
	Group:        "group",
	Output:       "output",
	Offset:       "offset",
	Product:      "product",
	Restrict:     "restrict",
	Dispatch:     "dispatch",
	Connector:    "connect",
	Projection:   "projection",
	Anti:         "anti",
	Single:       "single",
	Mark:         "mark",
	LoopJoin:     "loop join",
	LoopLeft:     "loop left",
	LoopSemi:     "loop semi",
	LoopAnti:     "loop anti",
	LoopSingle:   "loop single",
	MergeTop:     "merge top",
	MergeLimit:   "merge limit",
	MergeOrder:   "merge order",
	MergeGroup:   "merge group",
	MergeOffset:  "merge offset",
	Deletion:     "delete",
	Insert:       "insert",
	Update:       "update",
	External:     "external",
	Minus:        "minus",
	Intersect:    "intersect",
	IntersectAll: "intersect all",
	HashBuild:    "hash build",
}

func Run(ins Instructions, proc *process.Process) (end bool, err error) {
	var ok bool

	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(e)
		}
	}()
	for _, in := range ins {
		//println("step in ", debugInstructionNames[in.Op])
		if ok, err = execFunc[in.Op](in.Idx, proc, in.Arg); err != nil {
			return ok || end, err
		}
		//println("step out ", debugInstructionNames[in.Op])
		if ok { // ok is true shows that at least one operator has done its work
			end = true
		}
	}
	return end, err
}
