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

package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/simdcsv"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(attrs []string, ins vm.Instructions, reg *process.WaitRegister, cols []*plan.ColDef, name2ColIndex map[string]int32, sql string) *Pipeline {
	return &Pipeline{
		reg:           reg,
		instructions:  ins,
		attrs:         attrs,
		cols:          cols,
		Name2ColIndex: name2ColIndex,
		CreateSql:     sql,
	}
}

func NewMerge(ins vm.Instructions, reg *process.WaitRegister) *Pipeline {
	return &Pipeline{
		reg:          reg,
		instructions: ins,
	}
}

func (p *Pipeline) String() string {
	var buf bytes.Buffer

	vm.String(p.instructions, &buf)
	return buf.String()
}

func (p *Pipeline) Run(r engine.Reader, proc *process.Process) (bool, error) {
	var end bool // exist flag
	var err error
	var bat *batch.Batch


	defer cleanup(p, proc)
	if p.reg != nil { // used to handle some push-down request
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}
	if err = vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}

	for {
		// read data from storage engine
		if bat, err = r.Read(p.attrs, nil, proc.Mp); err != nil {
			return false, err
		}
		if bat != nil {
			bat.Cnt = 1
		}
		// processing the batch according to the instructions
		proc.Reg.InputBatch = bat
		if end, err = vm.Run(p.instructions, proc); err != nil || end { // end is true means pipeline successfully completed
			return end, err
		}
	}
}

func getLineOutChan(v atomic.Value) chan simdcsv.LineOut {
	return v.Load().(chan simdcsv.LineOut)
}

func judgeInterge(field string) bool {
	for i := 0; i < len(field); i++ {
		if field[i] > '9' || field[i] < '0' {
			return false
		}
	}
	return true
}

const NULL_FLAG = "\\N"

func (p *Pipeline) GetBatchData(Line []string, proc *process.Process) (*batch.Batch, error) {
	bat := batch.New(true, p.attrs)
	bat.Vecs = make([]*vector.Vector, len(p.attrs))
	/*if len(Line) != len(p.attrs) {
		return nil, errors.New("the table colnum is not equal to input data colnum")
	}*/
	rowIdx := 0
	for i := range p.attrs {
		field := strings.TrimSpace(Line[p.Name2ColIndex[p.attrs[i]]])
		typ := types.New(types.T(p.cols[i].Typ.Id), p.cols[i].Typ.Width, p.cols[i].Typ.Scale, p.cols[i].Typ.Precision)
		vec := vector.New(typ)
		vec.Typ = typ
		vec.Or = true
		isNullOrEmpty := len(field) == 0 || field == NULL_FLAG
		switch p.cols[i].Typ.Id {
		case plan.Type_BOOL:
			vec.Data = make([]byte, 1)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []bool{false}
			} else {
				if field == "true" || field == "1" {
					vec.Col = []bool{true}
				} else if field == "false" || field == "0" {
					vec.Col = []bool{false}
				} else {
					return nil, fmt.Errorf("the input value '%s' is not bool type for colnum %d", field, i)
				}
			}
		case plan.Type_INT8:
			vec.Data = make([]byte, 1)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []int8{0}
			} else {
				if judgeInterge(field) {
					d, err := strconv.ParseInt(field, 10, 8)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not int8 type for colnum %d", field, i)
					}
					vec.Col = []int8{int8(d)}
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < math.MinInt8 || d > math.MaxInt8 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not int8 type for colnum %d", field, i)
					}
					vec.Col = []int8{int8(d)}
				}
			}
		case plan.Type_INT16:
			vec.Data = make([]byte, 2)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []int16{0}
			} else {
				if judgeInterge(field) {
					d, err := strconv.ParseInt(field, 10, 16)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not int16 type for colnum %d", field, i)
					}
					vec.Col = []int16{int16(d)}
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < math.MinInt16 || d > math.MaxInt16 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not int16 type for colnum %d", field, i)
					}
					vec.Col = []int16{int16(d)}
				}
			}
		case plan.Type_INT32:
			vec.Data = make([]byte, 4)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []int32{0}
			} else {
				if judgeInterge(field) {
					d, err := strconv.ParseInt(field, 10, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not int32 type for colnum %d", field, i)
					}
					vec.Col = []int32{int32(d)}
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < math.MinInt32 || d > math.MaxInt32 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not int32 type for colnum %d", field, i)
					}
					vec.Col = []int32{int32(d)}
				}
			}
		case plan.Type_INT64:
			vec.Data = make([]byte, 8)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []int64{0}
			} else {
				if judgeInterge(field) {
					d, err := strconv.ParseInt(field, 10, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not int64 type for colnum %d", field, i)
					}
					vec.Col = []int64{d}
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < math.MinInt64 || d > math.MaxInt64 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not int64 type for colnum %d", field, i)
					}
					vec.Col = []int64{int64(d)}
				}
			}
		case plan.Type_UINT8:
			vec.Data = make([]byte, 1)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []uint8{0}
			} else {
				if judgeInterge(field) {
					d, err := strconv.ParseUint(field, 10, 8)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not uint8 type for colnum %d", field, i)
					}
					vec.Col = []uint8{uint8(d)}
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < 0 || d > math.MaxUint8 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not uint8 type for colnum %d", field, i)
					}
					vec.Col = []uint8{uint8(d)}
				}
			}
		case plan.Type_UINT16:
			vec.Data = make([]byte, 2)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []uint16{0}
			} else {
				if judgeInterge(field) {
					d, err := strconv.ParseUint(field, 10, 16)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not uint16 type for colnum %d", field, i)
					}
					vec.Col = []uint16{uint16(d)}
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < 0 || d > math.MaxUint16 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not uint16 type for colnum %d", field, i)
					}
					vec.Col = []uint16{uint16(d)}
				}
			}
		case plan.Type_UINT32:
			vec.Data = make([]byte, 4)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []uint32{0}
			} else {
				if judgeInterge(field) {
					d, err := strconv.ParseUint(field, 10, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not uint32 type for colnum %d", field, i)
					}
					vec.Col = []uint32{uint32(d)}
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < 0 || d > math.MaxUint32 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not uint32 type for colnum %d", field, i)
					}
					vec.Col = []uint32{uint32(d)}
				}
			}
		case plan.Type_UINT64:
			vec.Data = make([]byte, 8)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []uint64{0}
			} else {
				if judgeInterge(field) {
					d, err := strconv.ParseUint(field, 10, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not uint64 type for colnum %d", field, i)
					}
					vec.Col = []uint64{uint64(d)}
				} else {
					d, err := strconv.ParseFloat(field, 64)
					if err != nil || d < 0 || d > math.MaxUint64 {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not uint64 type for colnum %d", field, i)
					}
					vec.Col = []uint64{uint64(d)}
				}
			}
		case plan.Type_FLOAT32:
			vec.Data = make([]byte, 4)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []float32{0}
			} else {
				d, err := strconv.ParseFloat(field, 32)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return nil, fmt.Errorf("the input value '%v' is not float32 type for colnum %d", field, i)
				}
				vec.Col = []float32{float32(d)}
			}
		case plan.Type_FLOAT64:
			vec.Data = make([]byte, 8)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []float64{0}
			} else {
				d, err := strconv.ParseFloat(field, 32)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return nil, fmt.Errorf("the input value '%v' is not float64 type for colnum %d", field, i)
				}
				vec.Col = []float64{d}
			}
		case plan.Type_CHAR, plan.Type_VARCHAR, plan.Type_JSON:
			vBytes := &types.Bytes{Data: make([]byte, 0), Offsets: make([]uint32, 1), Lengths: make([]uint32, 1)}
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
				vBytes.Lengths[rowIdx] = uint32(0)
				vec.Data = make([]byte, 0)
			} else {
				vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
				vBytes.Data = append(vBytes.Data, field...)
				vBytes.Lengths[rowIdx] = uint32(len(field))
				vec.Data = make([]byte, len(field))
			}
			vec.Col = vBytes
		case plan.Type_DATE:
			vec.Data = make([]byte, 4)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []types.Date{0}
			} else {
				d, err := types.ParseDate(field)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return nil, fmt.Errorf("the input value '%v' is not Date type for colnum %d", field, i)
				}
				vec.Col = []types.Date{d}
			}
		case plan.Type_DATETIME:
			vec.Data = make([]byte, 8)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []types.Datetime{0}
			} else {
				d, err := types.ParseDatetime(field, vec.Typ.Precision)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return nil, fmt.Errorf("the input value '%v' is not Datetime type for colnum %d", field, i)
				}
				vec.Col = []types.Datetime{d}
			}
		case plan.Type_DECIMAL64:
			vec.Data = make([]byte, 8)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []types.Decimal64{}
			} else {
				d, err := types.Decimal64_FromString(field)
				if err != nil {
					// we tolerate loss of digits.
					if !moerr.IsMoErrCode(err, moerr.DATA_TRUNCATED) {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not Decimal64 type for colnum %d", field, i)
					}
				}
				vec.Col = []types.Decimal64{d}
			}
		case plan.Type_DECIMAL128:
			vec.Data = make([]byte, 16)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []types.Decimal128{}
			} else {
				d, err := types.Decimal128_FromString(field)
				if err != nil {
					// we tolerate loss of digits.
					if !moerr.IsMoErrCode(err, moerr.DATA_TRUNCATED) {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						return nil, fmt.Errorf("the input value '%v' is not Decimal128 type for colnum %d", field, i)
					}
				}
				vec.Col = []types.Decimal128{d}
			}
		case plan.Type_TIMESTAMP:
			vec.Data = make([]byte, 8)
			if isNullOrEmpty {
				nulls.Add(vec.Nsp, uint64(rowIdx))
				vec.Col = []types.Timestamp{}
			} else {
				d, err := types.ParseTimestamp(field, vec.Typ.Precision)
				if err != nil {
					logutil.Errorf("parse field[%v] err:%v", field, err)
					return nil, fmt.Errorf("the input value '%v' is not Timestamp type for colnum %d", field, i)

				}
				vec.Col = []types.Timestamp{d}
			}
		default:
			return nil, fmt.Errorf("the value type %d is not support now", p.cols[i].Typ.Id)
		}
		bat.Vecs[i] = vec
		n := vector.Length(bat.Vecs[0])
		sels := proc.Mp.GetSels()
		if n > cap(sels) {
			proc.Mp.PutSels(sels)
			sels = make([]int64, n)
		}
		bat.Zs = sels[:n]
		for k := 0; k < n; k++ {
			bat.Zs[k] = 1
		}
	}
	return bat, nil
}

func (p *Pipeline) Run2(proc *process.Process) (bool, error) {
	var end bool // exist flag
	var err error
	var bat *batch.Batch

	defer cleanup(p, proc)
	if p.reg != nil { // used to handle some push-down request
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}

	if err = vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}

	lo := &tree.LoadParam{}
	err = json.Unmarshal([]byte(p.CreateSql), lo)
	if err != nil {
		return false, err
	}

	plh := &ParseLineHandler{}
	
	dataFile, err := os.Open(lo.Filepath)
	if err != nil {
		logutil.Errorf("open file failed. err:%v", err)
		return false, err
	}
	defer func() {
		err := dataFile.Close()
		if err != nil {
			logutil.Errorf("close file failed. err:%v", err)
		}
	}()
	
	channelSize := 100
	plh.simdCsvGetParsedLinesChan = atomic.Value{}
	plh.simdCsvGetParsedLinesChan.Store(make(chan simdcsv.LineOut, channelSize))
	plh.simdCsvReader = simdcsv.NewReaderWithOptions(dataFile,
		rune(','),
		'#',
		false,
		false)
	
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := plh.simdCsvReader.ReadLoop(getLineOutChan(plh.simdCsvGetParsedLinesChan))
		if err != nil {
			close(getLineOutChan(plh.simdCsvGetParsedLinesChan))	
		}
	}()

	var lineOut simdcsv.LineOut
	for {
		// read data from storage engine
		/*if bat, err = r.Read(p.attrs, nil, proc.Mp); err != nil {
			return false, err
		}*/
		lineOut = <-getLineOutChan(plh.simdCsvGetParsedLinesChan)

		if lineOut.Line == nil && lineOut.Lines == nil {
			bat = nil
		} else {
			bat, err = p.GetBatchData(lineOut.Line, proc)
			if err != nil {
				return false, err
			}
			bat.Cnt = 1
		}

		// processing the batch according to the instructions
		proc.Reg.InputBatch = bat
		if end, err = vm.Run(p.instructions, proc); err != nil || end { // end is true means pipeline successfully completed
			break
		}
	}
	wg.Wait()
	return end, nil
}

func (p *Pipeline) ConstRun(bat *batch.Batch, proc *process.Process) (bool, error) {
	var end bool // exist flag
	var err error

	defer cleanup(p, proc)
	if p.reg != nil { // used to handle some push-down request
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}
	if err = vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}
	bat.Cnt = 1
	// processing the batch according to the instructions
	proc.Reg.InputBatch = bat
	end, err = vm.Run(p.instructions, proc)
	proc.Reg.InputBatch = nil
	_, _ = vm.Run(p.instructions, proc)
	return end, err
}

func (p *Pipeline) MergeRun(proc *process.Process) (bool, error) {
	var end bool
	var err error

	defer func() {
		cleanup(p, proc)
		for i := 0; i < len(proc.Reg.MergeReceivers); i++ { // simulating the end of a pipeline
			for len(proc.Reg.MergeReceivers[i].Ch) > 0 {
				bat := <-proc.Reg.MergeReceivers[i].Ch
				if bat != nil {
					bat.Clean(proc.Mp)
				}
			}
		}
		proc.Cancel()
	}()
	if p.reg != nil { // used to handle some push-down request
		select {
		case <-p.reg.Ctx.Done():
		case <-p.reg.Ch:
		}
	}
	if err := vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}
	for {
		proc.Reg.InputBatch = nil
		if end, err = vm.Run(p.instructions, proc); err != nil || end {
			return end, err
		}
	}
}

func cleanup(p *Pipeline, proc *process.Process) {
	proc.Reg.InputBatch = nil
	_, _ = vm.Run(p.instructions, proc)
	for i, in := range p.instructions {
		if in.Op == vm.Connector {
			arg := p.instructions[i].Arg.(*connector.Argument)
			select {
			case <-arg.Reg.Ctx.Done():
			case arg.Reg.Ch <- nil:
			}
			break
		}
		if in.Op == vm.Dispatch {
			arg := p.instructions[i].Arg.(*dispatch.Argument)
			for _, reg := range arg.Regs {
				select {
				case <-reg.Ctx.Done():
				case reg.Ch <- nil:
				}
			}
			break
		}
	}
}
