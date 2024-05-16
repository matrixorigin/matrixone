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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (ins *Instruction) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (ins *Instruction) UnmarshalBinary(_ []byte) error {
	return nil
}

// String range instructions and call each operator's string function to show a query plan
func String(ins Instructions, buf *bytes.Buffer) {
	for i, in := range ins {
		if i > 0 {
			buf.WriteString(" -> ")
		}
		in.Arg.String(buf)
	}
}

// Prepare range instructions and do init work for each operator's argument by calling its prepare function
func Prepare(ins Instructions, proc *process.Process) error {
	for _, in := range ins {
		if err := in.Arg.Prepare(proc); err != nil {
			return err
		}
	}
	return nil
}

func setAnalyzeInfo(ins Instructions, proc *process.Process) {
	for i := 0; i < len(ins); i++ {
		switch ins[i].Op {
		case Output:
			ins[i].Idx = -1
		case TableScan:
			ins[i].Idx = ins[i+1].Idx
		}
	}

	idxMapMajor := make(map[int]int, 0)
	idxMapMinor := make(map[int]int, 0)
	for i := 0; i < len(ins); i++ {
		info := &OperatorInfo{
			Idx:     ins[i].Idx,
			IsFirst: ins[i].IsFirst,
			IsLast:  ins[i].IsLast,

			CnAddr:      ins[i].CnAddr,
			OperatorID:  ins[i].OperatorID,
			ParallelID:  ins[i].ParallelID,
			MaxParallel: ins[i].MaxParallel,
		}
		switch ins[i].Op {
		case HashBuild, ShuffleBuild, IndexBuild, Restrict, MergeGroup, MergeOrder:
			isMinor := true
			if ins[i].Op == Restrict {
				if ins[0].Op != TableScan && ins[0].Op != External {
					isMinor = false // restrict operator is minor only for scan
				}
			}

			if isMinor {
				if info.Idx >= 0 && info.Idx < len(proc.AnalInfos) {
					info.ParallelMajor = false
					if pidx, ok := idxMapMinor[info.Idx]; ok {
						info.ParallelIdx = pidx
					} else {
						pidx = proc.AnalInfos[info.Idx].AddNewParallel(false)
						idxMapMinor[info.Idx] = pidx
						info.ParallelIdx = pidx
					}
				}
			} else {
				info.ParallelIdx = -1
			}

		case TableScan, External, Order, Window, Group, Join, LoopJoin, Left, LoopLeft, Single, LoopSingle, Semi, RightSemi, LoopSemi, Anti, RightAnti, LoopAnti, Mark, LoopMark, Product:
			info.ParallelMajor = true
			if info.Idx >= 0 && info.Idx < len(proc.AnalInfos) {
				if pidx, ok := idxMapMajor[info.Idx]; ok {
					info.ParallelIdx = pidx
				} else {
					pidx = proc.AnalInfos[info.Idx].AddNewParallel(true)
					idxMapMajor[info.Idx] = pidx
					info.ParallelIdx = pidx
				}
			}
		default:
			info.ParallelIdx = -1 // do nothing for parallel analyze info
		}
		ins[i].Arg.SetInfo(info)
	}
}

func Run(ins Instructions, proc *process.Process) (end bool, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = moerr.ConvertPanicError(proc.Ctx, e)
			logutil.Errorf("panic in vm.Run: %v", err)
		}
	}()

	setAnalyzeInfo(ins, proc)

	for i := 1; i < len(ins); i++ {
		ins[i].Arg.AppendChild(ins[i-1].Arg)
	}

	root := ins[len(ins)-1].Arg
	end = false
	for !end {
		result, err := root.Call(proc)
		if err != nil {
			return true, err
		}
		end = result.Status == ExecStop || result.Batch == nil
	}
	return end, nil
}
