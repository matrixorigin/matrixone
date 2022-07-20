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

package frontend

import (
	"bytes"
	"fmt"
	"go/constant"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"

	mo_config "github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
)

type CloseFlag struct {
	//closed flag
	closed uint32
}

//1 for closed
//0 for others
func (cf *CloseFlag) setClosed(value uint32) {
	atomic.StoreUint32(&cf.closed, value)
}

func (cf *CloseFlag) Open() {
	cf.setClosed(0)
}

func (cf *CloseFlag) Close() {
	cf.setClosed(1)
}

func (cf *CloseFlag) IsClosed() bool {
	return atomic.LoadUint32(&cf.closed) != 0
}

func (cf *CloseFlag) IsOpened() bool {
	return atomic.LoadUint32(&cf.closed) == 0
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func MinInt64(a int64, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MinUint64(a uint64, b uint64) uint64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

func MaxInt64(a int64, b int64) int64 {
	if a < b {
		return b
	} else {
		return a
	}
}

func MaxUint64(a uint64, b uint64) uint64 {
	if a < b {
		return b
	} else {
		return a
	}
}

type Uint64List []uint64

func (ul Uint64List) Len() int {
	return len(ul)
}

func (ul Uint64List) Less(i, j int) bool {
	return ul[i] < ul[j]
}

func (ul Uint64List) Swap(i, j int) {
	ul[i], ul[j] = ul[j], ul[i]
}

// GetRoutineId gets the routine id
func GetRoutineId() uint64 {
	data := make([]byte, 64)
	data = data[:runtime.Stack(data, false)]
	data = bytes.TrimPrefix(data, []byte("goroutine "))
	data = data[:bytes.IndexByte(data, ' ')]
	id, _ := strconv.ParseUint(string(data), 10, 64)
	return id
}

type DebugCounter struct {
	length  int
	counter []uint64
	Cf      CloseFlag
}

func NewDebugCounter(l int) *DebugCounter {
	return &DebugCounter{
		length:  l,
		counter: make([]uint64, l),
	}
}

func (dc *DebugCounter) Add(i int, v uint64) {
	atomic.AddUint64(&dc.counter[i], v)
}

func (dc *DebugCounter) Set(i int, v uint64) {
	atomic.StoreUint64(&dc.counter[i], v)
}

func (dc *DebugCounter) Get(i int) uint64 {
	return atomic.LoadUint64(&dc.counter[i])
}

func (dc *DebugCounter) Len() int {
	return dc.length
}

func (dc *DebugCounter) DCRoutine() {
	dc.Cf.Open()

	for dc.Cf.IsOpened() {
		for i := 0; i < dc.length; i++ {
			if i != 0 && i%8 == 0 {
				fmt.Printf("\n")
			}
			v := dc.Get(i)
			fmt.Printf("[%4d %4d]", i, v)
			dc.Set(i, 0)
		}
		fmt.Printf("\n")
		time.Sleep(5 * time.Second)
	}
}

const (
	TIMEOUT_TYPE_SECOND int = iota
	TIMEOUT_TYPE_MILLISECOND
)

type Timeout struct {
	//last record of the time
	lastTime atomic.Value //time.Time

	//period
	timeGap time.Duration

	//auto update
	autoUpdate bool
}

func NewTimeout(tg time.Duration, autoUpdateWhenChecked bool) *Timeout {
	ret := &Timeout{
		timeGap:    tg,
		autoUpdate: autoUpdateWhenChecked,
	}
	ret.lastTime.Store(time.Now())
	return ret
}

func (t *Timeout) UpdateTime(tn time.Time) {
	t.lastTime.Store(tn)
}

/*
----------+---------+------------------+--------
      lastTime     Now         lastTime + timeGap

return true  :  is timeout. the lastTime has been updated.
return false :  is not timeout. the lastTime has not been updated.
*/
func (t *Timeout) isTimeout() bool {
	if time.Since(t.lastTime.Load().(time.Time)) <= t.timeGap {
		return false
	}

	if t.autoUpdate {
		t.lastTime.Store(time.Now())
	}
	return true
}

/*
length:
-1, complete string.
0, empty string
>0 , length of characters at the header of the string.
*/
func SubStringFromBegin(str string, length int) string {
	if length == 0 || length < -1 {
		return ""
	}

	if length == -1 {
		return str
	}

	l := Min(len(str), length)
	if l != len(str) {
		return str[:l] + "..."
	}
	return str[:l]
}

/*
path exists in the system
return:
true/false - exists or not.
true/false - file or directory
error
*/
var PathExists = func(path string) (bool, bool, error) {
	fi, err := os.Stat(path)
	if err == nil {
		return true, !fi.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, false, err
	}

	return false, false, err
}

/*
MakeDebugInfo prints bytes in multi-lines.
*/
func MakeDebugInfo(data []byte, bytesCount int, bytesPerLine int) string {
	if len(data) == 0 || bytesCount == 0 || bytesPerLine == 0 {
		return ""
	}
	pl := Min(bytesCount, len(data))
	ps := ""
	for i := 0; i < pl; i++ {
		if i > 0 && (i%bytesPerLine == 0) {
			ps += "\n"
		}
		if i%bytesPerLine == 0 {
			ps += fmt.Sprintf("%d", i/bytesPerLine) + " : "
		}
		ps += fmt.Sprintf("%02x ", data[i])
	}
	return ps
}

func getSystemVariables(configFile string) (*mo_config.SystemVariables, error) {
	sv := &mo_config.SystemVariables{}
	var err error
	//before anything using the configuration
	if err = sv.LoadInitialValues(); err != nil {
		logutil.Errorf("error:%v", err)
		return nil, err
	}

	if err = mo_config.LoadvarsConfigFromFile(configFile, sv); err != nil {
		logutil.Errorf("error:%v", err)
		return nil, err
	}
	return sv, err
}

func getParameterUnit(configFile string, eng engine.Engine) (*mo_config.ParameterUnit, error) {
	sv, err := getSystemVariables(configFile)
	if err != nil {
		return nil, err
	}

	hostMmu := host.New(sv.GetHostMmuLimitation())
	mempool := mempool.New( /*int(sv.GetMempoolMaxSize()), int(sv.GetMempoolFactor())*/ )

	fmt.Println("Using Dump Storage Engine and Cluster Nodes.")

	pu := mo_config.NewParameterUnit(sv, hostMmu, mempool, eng, engine.Nodes{})

	return pu, nil
}

func ConvertCatalogSchemaToEngineFormat(mcs *CatalogSchema) []*engine.AttributeDef {
	genAttr := func(attr *CatalogSchemaAttribute) *engine.AttributeDef {
		return &engine.AttributeDef{
			Attr: engine.Attribute{
				Name:    attr.AttributeName,
				Alg:     0,
				Type:    attr.AttributeType,
				Default: engine.DefaultExpr{},
			}}
	}

	attrs := make([]*engine.AttributeDef, 0, mcs.Length())
	for _, attr := range mcs.GetAttributes() {
		attrs = append(attrs, genAttr(attr))
	}
	return attrs
}

func toTypesType(t types.T) types.Type {
	return t.ToType()
}

func AllocateBatchBasedOnEngineAttributeDefinition(attributeDefs []*engine.AttributeDef, rowCount int) *batch.Batch {
	var attributeNames = make([]string, len(attributeDefs))
	for i, def := range attributeDefs {
		attributeNames[i] = def.Attr.Name
	}
	batchData := batch.New(true, attributeNames)

	batchData.Zs = make([]int64, rowCount)
	for i := 0; i < rowCount; i++ {
		batchData.Zs[i] = 1
	}

	//alloc space for vector
	for i, def := range attributeDefs {
		vec := vector.New(def.Attr.Type)
		vec.Or = true
		vec.Data = nil
		switch vec.Typ.Oid {
		case types.T_bool:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_bool).Size))
			vec.Col = encoding.DecodeBoolSlice(vec.Data)
		case types.T_int8:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_int8).Size))
			vec.Col = encoding.DecodeInt8Slice(vec.Data)
		case types.T_int16:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_int16).Size))
			vec.Col = encoding.DecodeInt16Slice(vec.Data)
		case types.T_int32:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_int32).Size))
			vec.Col = encoding.DecodeInt32Slice(vec.Data)
		case types.T_int64:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_int64).Size))
			vec.Col = encoding.DecodeInt64Slice(vec.Data)
		case types.T_uint8:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_uint8).Size))
			vec.Col = encoding.DecodeUint8Slice(vec.Data)
		case types.T_uint16:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_uint16).Size))
			vec.Col = encoding.DecodeUint16Slice(vec.Data)
		case types.T_uint32:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_uint32).Size))
			vec.Col = encoding.DecodeUint32Slice(vec.Data)
		case types.T_uint64:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_uint64).Size))
			vec.Col = encoding.DecodeUint64Slice(vec.Data)
		case types.T_float32:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_float32).Size))
			vec.Col = encoding.DecodeFloat32Slice(vec.Data)
		case types.T_float64:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_float64).Size))
			vec.Col = encoding.DecodeFloat64Slice(vec.Data)
		case types.T_char, types.T_varchar:
			vBytes := &types.Bytes{
				Offsets: make([]uint32, rowCount),
				Lengths: make([]uint32, rowCount),
				Data:    nil,
			}
			vec.Col = vBytes
			vec.Data = vBytes.Data
		case types.T_date:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_date).Size))
			vec.Col = encoding.DecodeDateSlice(vec.Data)
		case types.T_datetime:
			vec.Data = make([]byte, rowCount*int(toTypesType(types.T_datetime).Size))
			vec.Col = encoding.DecodeDatetimeSlice(vec.Data)
		default:
			panic("unsupported vector type")
		}
		batchData.Vecs[i] = vec
	}
	batchData.Attrs = attributeNames
	return batchData
}

func FillBatchWithData(data [][]string, batch *batch.Batch) {
	for i, line := range data {
		rowIdx := i
		for j, field := range line {
			colIdx := j

			isNullOrEmpty := len(field) == 0 || field == "\\N"
			vec := batch.Vecs[colIdx]

			switch vec.Typ.Oid {
			case types.T_bool:
				cols := vec.Col.([]bool)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := types.ParseBool(field)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
					}
					cols[rowIdx] = d
				}
			case types.T_int8:
				cols := vec.Col.([]int8)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseInt(field, 10, 8)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = int8(d)
				}
			case types.T_int16:
				cols := vec.Col.([]int16)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseInt(field, 10, 16)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = int16(d)
				}
			case types.T_int32:
				cols := vec.Col.([]int32)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseInt(field, 10, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = int32(d)
				}
			case types.T_int64:
				cols := vec.Col.([]int64)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseInt(field, 10, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = d
				}
			case types.T_uint8:
				cols := vec.Col.([]uint8)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseUint(field, 10, 8)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = uint8(d)
				}
			case types.T_uint16:
				cols := vec.Col.([]uint16)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseUint(field, 10, 16)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = uint16(d)
				}
			case types.T_uint32:
				cols := vec.Col.([]uint32)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseUint(field, 10, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = uint32(d)
				}
			case types.T_uint64:
				cols := vec.Col.([]uint64)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseUint(field, 10, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = uint64(d)
				}
			case types.T_float32:
				cols := vec.Col.([]float32)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseFloat(field, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = float32(d)
				}
			case types.T_float64:
				cols := vec.Col.([]float64)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					fs := field
					d, err := strconv.ParseFloat(fs, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = d
				}
			case types.T_char, types.T_varchar:
				vBytes := vec.Col.(*types.Bytes)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
					vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
					vBytes.Lengths[rowIdx] = uint32(0)
				} else {
					vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
					vBytes.Data = append(vBytes.Data, field...)
					vBytes.Lengths[rowIdx] = uint32(len(field))
				}
			case types.T_date:
				cols := vec.Col.([]types.Date)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					fs := field
					d, err := types.ParseDate(fs)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = d
				}
			case types.T_datetime:
				cols := vec.Col.([]types.Datetime)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					fs := field
					d, err := types.ParseDatetime(fs, vec.Typ.Precision)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = d
				}
			default:
				panic("unsupported oid")
			}
		}
	}
}

func FormatLineInBatch(bat *batch.Batch, rowIndex int) []string {
	row := make([]interface{}, len(bat.Vecs))
	var res []string
	for i := 0; i < len(bat.Vecs); i++ {
		vec := bat.Vecs[i]
		switch vec.Typ.Oid { //get col
		case types.T_bool:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]bool)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]bool)
					row[i] = vs[rowIndex]
				}
			}
		case types.T_int8:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]int8)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]int8)
					row[i] = vs[rowIndex]
				}
			}
		case types.T_uint8:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]uint8)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]uint8)
					row[i] = vs[rowIndex]
				}
			}
		case types.T_int16:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]int16)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]int16)
					row[i] = vs[rowIndex]
				}
			}
		case types.T_uint16:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]uint16)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]uint16)
					row[i] = vs[rowIndex]
				}
			}
		case types.T_int32:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]int32)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]int32)
					row[i] = vs[rowIndex]
				}
			}
		case types.T_uint32:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]uint32)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]uint32)
					row[i] = vs[rowIndex]
				}
			}
		case types.T_int64:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]int64)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]int64)
					row[i] = vs[rowIndex]
				}
			}
		case types.T_uint64:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]uint64)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]uint64)
					row[i] = vs[rowIndex]
				}
			}
		case types.T_float32:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]float32)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]float32)
					row[i] = vs[rowIndex]
				}
			}
		case types.T_float64:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]float64)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]float64)
					row[i] = vs[rowIndex]
				}
			}
		case types.T_char, types.T_varchar:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.(*types.Bytes)
				row[i] = string(vs.Get(int64(rowIndex)))
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.(*types.Bytes)
					row[i] = string(vs.Get(int64(rowIndex)))
				}
			}
		case types.T_date:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]types.Date)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]types.Date)
					row[i] = vs[rowIndex]
				}
			}
		case types.T_datetime:
			if !nulls.Any(vec.Nsp) { //all data in this column are not null
				vs := vec.Col.([]types.Datetime)
				row[i] = vs[rowIndex]
			} else {
				if nulls.Contains(vec.Nsp, uint64(rowIndex)) { //is null
					row[i] = nil
				} else {
					vs := vec.Col.([]types.Datetime)
					row[i] = vs[rowIndex]
				}
			}
		default:
			panic(fmt.Sprintf("reader.Read : unsupported type %d \n", vec.Typ.Oid))
		}
		res = append(res, fmt.Sprintf("%v", row[i]))
	}
	return res
}

// WildcardMatch implements wildcard pattern match algorithm.
// pattern and target are ascii characters
// TODO: add \_ and \%
func WildcardMatch(pattern, target string) bool {
	var p = 0
	var t = 0
	var positionOfPercentPlusOne int = -1
	var positionOfTargetEncounterPercent int = -1
	plen := len(pattern)
	tlen := len(target)
	for t < tlen {
		//%
		if p < plen && pattern[p] == '%' {
			p++
			positionOfPercentPlusOne = p
			if p >= plen {
				//pattern end with %
				return true
			}
			//means % matches empty
			positionOfTargetEncounterPercent = t
		} else if p < plen && (pattern[p] == '_' || pattern[p] == target[t]) { //match or _
			p++
			t++
		} else {
			if positionOfPercentPlusOne == -1 {
				//have not matched a %
				return false
			}
			if positionOfTargetEncounterPercent == -1 {
				return false
			}
			//backtrace to last % position + 1
			p = positionOfPercentPlusOne
			//means % matches multiple characters
			positionOfTargetEncounterPercent++
			t = positionOfTargetEncounterPercent
		}
	}
	//skip %
	for p < plen && pattern[p] == '%' {
		p++
	}
	return p >= plen
}

// only support single value and unary minus
func GetSimpleExprValue(e tree.Expr) (interface{}, error) {
	var value interface{}
	switch v := e.(type) {
	case *tree.NumVal:
		switch v.Value.Kind() {
		case constant.Unknown:
			value = nil
		case constant.Bool:
			value = constant.BoolVal(v.Value)
		case constant.String:
			value = constant.StringVal(v.Value)
		case constant.Int:
			value, _ = constant.Int64Val(v.Value)
		case constant.Float:
			value, _ = constant.Float64Val(v.Value)
		default:
			return nil, errorNumericTypeIsNotSupported
		}
	case *tree.UnaryExpr:
		ival, err := GetSimpleExprValue(v.Expr)
		if err != nil {
			return nil, err
		}
		if v.Op == tree.UNARY_MINUS {
			switch iival := ival.(type) {
			case float64:
				value = -1 * iival
			case int64:
				value = -1 * iival
			default:
				return nil, errorUnaryMinusForNonNumericTypeIsNotSupported
			}
		}
	case *tree.UnresolvedName:
		return v.Parts[0], nil
	default:
		return nil, errorComplicateExprIsNotSupported
	}
	return value, nil
}
