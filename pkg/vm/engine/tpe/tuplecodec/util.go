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

package tuplecodec

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/encoding"

	"github.com/lni/goutils/random"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
)

func MakeAttributes(ts ...types.T) ([]string, []*engine.AttributeDef) {
	var name string

	gen_attr := func(name string, t types.T) *engine.AttributeDef {
		return &engine.AttributeDef{Attr: engine.Attribute{
			Name: name,
			Alg:  0,
			Type: types.Type{
				Oid:   t,
				Size:  0,
				Width: 0,
				Scale: 0,
			},
			Default: engine.DefaultExpr{},
		}}
	}

	names := make([]string, 0, len(ts))
	attrs := make([]*engine.AttributeDef, 0, len(ts))
	for _, t := range ts {
		switch t {
		case types.T_int8:
			name = "T_int8"
		case types.T_int16:
			name = "T_int16"
		case types.T_int32:
			name = "T_int32"
		case types.T_int64:
			name = "T_int64"
		case types.T_uint8:
			name = "T_uint8"
		case types.T_uint16:
			name = "T_uint16"
		case types.T_uint32:
			name = "T_uint32"
		case types.T_uint64:
			name = "T_uint64"
		case types.T_float32:
			name = "T_float32"
		case types.T_float64:
			name = "T_float64"
		case types.T_char:
			name = "T_char"
		case types.T_varchar:
			name = "T_varchar"
		case types.T_date:
			name = "T_date"
		case types.T_datetime:
			name = "T_datetime"
		default:
			panic("unsupported vector type")
		}

		names = append(names, name)
		attrs = append(attrs, gen_attr(name, t))
	}
	return names, attrs
}

//MakeBatch allocates a batch for test
func MakeBatch(batchSize int, attrName []string, cols []*engine.AttributeDef) *batch.Batch {
	batchData := batch.New(true, attrName)

	batchData.Zs = make([]int64, batchSize)
	for i := 0; i < batchSize; i++ {
		batchData.Zs[i] = 1
	}
	//alloc space for vector
	for i := 0; i < len(attrName); i++ {
		vec := vector.New(cols[i].Attr.Type)
		vec.Or = true
		vec.Data = nil
		switch vec.Typ.Oid {
		case types.T_int8:
			vec.Data = make([]byte, batchSize*int(toTypesType(types.T_int8).Size))
			vec.Col = encoding.DecodeInt8Slice(vec.Data)
		case types.T_int16:
			vec.Data = make([]byte, batchSize*int(toTypesType(types.T_int16).Size))
			vec.Col = encoding.DecodeInt16Slice(vec.Data)
		case types.T_int32:
			vec.Data = make([]byte, batchSize*int(toTypesType(types.T_int32).Size))
			vec.Col = encoding.DecodeInt32Slice(vec.Data)
		case types.T_int64:
			vec.Data = make([]byte, batchSize*int(toTypesType(types.T_int64).Size))
			vec.Col = encoding.DecodeInt64Slice(vec.Data)
		case types.T_uint8:
			vec.Data = make([]byte, batchSize*int(toTypesType(types.T_uint8).Size))
			vec.Col = encoding.DecodeUint8Slice(vec.Data)
		case types.T_uint16:
			vec.Data = make([]byte, batchSize*int(toTypesType(types.T_uint16).Size))
			vec.Col = encoding.DecodeUint16Slice(vec.Data)
		case types.T_uint32:
			vec.Data = make([]byte, batchSize*int(toTypesType(types.T_uint32).Size))
			vec.Col = encoding.DecodeUint32Slice(vec.Data)
		case types.T_uint64:
			vec.Data = make([]byte, batchSize*int(toTypesType(types.T_uint64).Size))
			vec.Col = encoding.DecodeUint64Slice(vec.Data)
		case types.T_float32:
			vec.Data = make([]byte, batchSize*int(toTypesType(types.T_float32).Size))
			vec.Col = encoding.DecodeFloat32Slice(vec.Data)
		case types.T_float64:
			vec.Data = make([]byte, batchSize*int(toTypesType(types.T_float64).Size))
			vec.Col = encoding.DecodeFloat64Slice(vec.Data)
		case types.T_char, types.T_varchar:
			vBytes := &types.Bytes{
				Offsets: make([]uint32, batchSize),
				Lengths: make([]uint32, batchSize),
				Data:    nil,
			}
			vec.Col = vBytes
			vec.Data = vBytes.Data
		case types.T_date:
			vec.Data = make([]byte, batchSize*int(toTypesType(types.T_date).Size))
			vec.Col = encoding.DecodeDateSlice(vec.Data)
		case types.T_datetime:
			vec.Data = make([]byte, batchSize*int(toTypesType(types.T_datetime).Size))
			vec.Col = encoding.DecodeDatetimeSlice(vec.Data)
		default:
			panic("unsupported vector type")
		}
		batchData.Vecs[i] = vec
	}
	batchData.Attrs = attrName
	return batchData
}

func randomLines(rowCnt int, attrName []string, cols []*engine.AttributeDef) [][]string {
	var lines [][]string

	for i := 0; i < rowCnt; i++ {
		var line []string
		for j := 0; j < len(attrName); j++ {
			var field string
			var d interface{}
			switch cols[j].Attr.Type.Oid {
			case types.T_int8:
				d = rand.Int31n((1<<7 - 1))
			case types.T_int16:
				d = rand.Int31n((1<<15 - 1))
			case types.T_int32:
				d = rand.Int31()
			case types.T_int64:
				d = rand.Int63()
			case types.T_uint8:
				d = rand.Int31n((1<<8 - 1))
			case types.T_uint16:
				d = rand.Int31n((1<<16 - 1))
			case types.T_uint32:
				d = rand.Int31()
			case types.T_uint64:
				d = rand.Int63()
			case types.T_float32:
				d = rand.Float32()
			case types.T_float64:
				d = rand.Float64()
			case types.T_char, types.T_varchar:
				d = random.String(10)
			case types.T_date:
				d = "2022-02-23"
			case types.T_datetime:
				d = "2022-02-23 00:00:00"
			default:
				panic("unsupported type")
			}
			field = fmt.Sprintf("%v", d)
			line = append(line, field)
		}
		lines = append(lines, line)
	}

	return lines
}

func FillBatch(lines [][]string, batchData *batch.Batch) {
	for i, line := range lines {
		rowIdx := i
		for j, field := range line {
			colIdx := j

			isNullOrEmpty := len(field) == 0 || field == "\\N"

			//put it into batch
			vec := batchData.Vecs[colIdx]
			//vecAttr := batchData.Attrs[colIdx]

			switch vec.Typ.Oid {
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
					vBytes.Lengths[rowIdx] = uint32(len(field))
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
					d, err := types.ParseDatetime(fs)
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

func TruncateBatch(bat *batch.Batch, batchSize, needLen int) {
	if needLen == batchSize {
		return
	}
	if needLen > batchSize {
		panic("needLen > batchSize is impossible")
	}
	for _, vec := range bat.Vecs {
		//remove nulls.NUlls
		for j := uint64(needLen); j < uint64(batchSize); j++ {
			nulls.Del(vec.Nsp, j)
		}
		//remove row
		switch vec.Typ.Oid {
		case types.T_int8:
			needBytes := needLen * int(toTypesType(types.T_int8).Size)
			vec.Data = vec.Data[:needBytes]
			vec.Col = encoding.DecodeInt8Slice(vec.Data)
		case types.T_int16:
			needBytes := needLen * int(toTypesType(types.T_int16).Size)
			vec.Data = vec.Data[:needBytes]
			vec.Col = encoding.DecodeInt16Slice(vec.Data)
		case types.T_int32:
			needBytes := needLen * int(toTypesType(types.T_int32).Size)
			vec.Data = vec.Data[:needBytes]
			vec.Col = encoding.DecodeInt32Slice(vec.Data)
		case types.T_int64:
			needBytes := needLen * int(toTypesType(types.T_int64).Size)
			vec.Data = vec.Data[:needBytes]
			vec.Col = encoding.DecodeInt64Slice(vec.Data)
		case types.T_uint8:
			needBytes := needLen * int(toTypesType(types.T_uint8).Size)
			vec.Data = vec.Data[:needBytes]
			vec.Col = encoding.DecodeUint8Slice(vec.Data)
		case types.T_uint16:
			needBytes := needLen * int(toTypesType(types.T_uint16).Size)
			vec.Data = vec.Data[:needBytes]
			vec.Col = encoding.DecodeUint16Slice(vec.Data)
		case types.T_uint32:
			needBytes := needLen * int(toTypesType(types.T_uint32).Size)
			vec.Data = vec.Data[:needBytes]
			vec.Col = encoding.DecodeUint32Slice(vec.Data)
		case types.T_uint64:
			needBytes := needLen * int(toTypesType(types.T_uint64).Size)
			vec.Data = vec.Data[:needBytes]
			vec.Col = encoding.DecodeUint64Slice(vec.Data)
		case types.T_float32:
			needBytes := needLen * int(toTypesType(types.T_float32).Size)
			vec.Data = vec.Data[:needBytes]
			vec.Col = encoding.DecodeFloat32Slice(vec.Data)
		case types.T_float64:
			needBytes := needLen * int(toTypesType(types.T_float64).Size)
			vec.Data = vec.Data[:needBytes]
			vec.Col = encoding.DecodeFloat64Slice(vec.Data)
		case types.T_char, types.T_varchar: //bytes is different
			vBytes := vec.Col.(*types.Bytes)
			if len(vBytes.Offsets) > needLen {
				nextStart := vBytes.Offsets[needLen]
				vec.Col = vBytes.Window(0, needLen)
				vBytes.Data = vBytes.Data[:nextStart]
			}
			vec.Data = vBytes.Data
		case types.T_date:
			needBytes := needLen * int(toTypesType(types.T_date).Size)
			vec.Data = vec.Data[:needBytes]
			vec.Col = encoding.DecodeDateSlice(vec.Data)
		case types.T_datetime:
			needBytes := needLen * int(toTypesType(types.T_datetime).Size)
			vec.Data = vec.Data[:needBytes]
			vec.Col = encoding.DecodeDatetimeSlice(vec.Data)
		}
	}
	bat.Zs = bat.Zs[:needLen]
}

func SerializeVectorForBatch(bat *batch.Batch) error {
	//for test
	//for i := range bat.Vecs {
	//	bat.Vecs[i].Or = true
	//	show, err := bat.Vecs[i].Show()
	//	if err != nil {
	//		return err
	//	}
	//	bat.Vecs[i].Data = show
	//}
	return nil
}

func ConvertAttributeDescIntoTypesType(attrs []*descriptor.AttributeDesc) ([]string, []*engine.AttributeDef) {
	names := make([]string, 0, len(attrs))
	defs := make([]*engine.AttributeDef, 0, len(attrs))
	for _, attr := range attrs {
		names = append(names, attr.Name)
		//make type
		defs = append(defs, &engine.AttributeDef{Attr: engine.Attribute{
			Name:    attr.Name,
			Alg:     0,
			Type:    attr.TypesType,
			Default: engine.DefaultExpr{},
		}})
	}
	return names, defs
}

func TpeTypeToEngineType(Ttype orderedcodec.ValueType) types.Type {
	t := types.Type{}
	switch Ttype {
	case orderedcodec.VALUE_TYPE_UINT64:
		t.Oid = types.T_uint64
	case orderedcodec.VALUE_TYPE_BYTES:
		t.Oid = types.T_char
	case orderedcodec.VALUE_TYPE_STRING:
		t.Oid = types.T_char
	case orderedcodec.VALUE_TYPE_INT8:
		t.Oid = types.T_int8
	case orderedcodec.VALUE_TYPE_INT16:
		t.Oid = types.T_int16
	case orderedcodec.VALUE_TYPE_INT32:
		t.Oid = types.T_int32
	case orderedcodec.VALUE_TYPE_INT64:
		t.Oid = types.T_int64
	case orderedcodec.VALUE_TYPE_UINT8:
		t.Oid = types.T_uint8
	case orderedcodec.VALUE_TYPE_UINT16:
		t.Oid = types.T_uint16
	case orderedcodec.VALUE_TYPE_UINT32:
		t.Oid = types.T_uint32
	case orderedcodec.VALUE_TYPE_FLOAT32:
		t.Oid = types.T_float32
	case orderedcodec.VALUE_TYPE_FLOAT64:
		t.Oid = types.T_float64
	case orderedcodec.VALUE_TYPE_DATE:
		t.Oid = types.T_date
	case orderedcodec.VALUE_TYPE_DATETIME:
		t.Oid = types.T_datetime
	default:
		panic("unsupported tpe type")
	}
	t.Oid.ToType()
	return t
}

func EngineTypeToTpeType(t *types.Type) orderedcodec.ValueType {
	var vt orderedcodec.ValueType
	switch t.Oid {
	case types.T_uint64:
		vt = orderedcodec.VALUE_TYPE_UINT64
	case types.T_char:
		vt = orderedcodec.VALUE_TYPE_BYTES
	case types.T_varchar:
		vt = orderedcodec.VALUE_TYPE_STRING
	case types.T_int8:
		vt = orderedcodec.VALUE_TYPE_INT8
	case types.T_int16:
		vt = orderedcodec.VALUE_TYPE_INT16
	case types.T_int32:
		vt = orderedcodec.VALUE_TYPE_INT32
	case types.T_int64:
		vt = orderedcodec.VALUE_TYPE_INT64
	case types.T_uint8:
		vt = orderedcodec.VALUE_TYPE_UINT8
	case types.T_uint16:
		vt = orderedcodec.VALUE_TYPE_UINT16
	case types.T_uint32:
		vt = orderedcodec.VALUE_TYPE_UINT32
	case types.T_float32:
		vt = orderedcodec.VALUE_TYPE_FLOAT32
	case types.T_float64:
		vt = orderedcodec.VALUE_TYPE_FLOAT64
	case types.T_date:
		vt = orderedcodec.VALUE_TYPE_DATE
	case types.T_datetime:
		vt = orderedcodec.VALUE_TYPE_DATETIME
	default:
		panic("unsupported types.Type")
	}
	return vt
}

//for rowid
var startTime int64 = time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()

const interval = uint64(10 * time.Microsecond)

var rowID struct {
	sync.Mutex
	previous uint64
}

func GetRowID(nodeID uint64) uint64 {
	nano := time.Now().UnixNano()
	if nano < startTime {
		nano = startTime
	}

	ts := uint64(nano-startTime) / interval

	rowID.Lock()
	if ts <= rowID.previous {
		ts = rowID.previous + 1
	}
	rowID.previous = ts
	rowID.Unlock()

	return (ts << 15) ^ nodeID
}

func MaxUint64(a, b uint64) uint64 {
	if a < b {
		return b
	} else {
		return a
	}
}

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func BeginCpuProfile(cpu string) *os.File {
	cpuf, _ := os.Create(cpu)
	pprof.StartCPUProfile(cpuf)
	return cpuf
}

func EndCpuProfile(cpuf *os.File) {
	pprof.StopCPUProfile()
	_ = cpuf.Close()
}

func BeginTime() time.Time {
	return time.Now()
}

func EndTime(s time.Time, info string) {
	logutil.Infof("%s duration %v", info, time.Since(s))
}

type errorStorage struct {
	mu     sync.Mutex
	keys   []TupleKey
	values []TupleValue
}

func (es *errorStorage) append(k TupleKey, v TupleValue) {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.keys = append(es.keys, k)
	es.values = append(es.values, v)
}

func (es *errorStorage) getKey(k TupleKey) TupleValue {
	es.mu.Lock()
	defer es.mu.Unlock()
	for i, key := range es.keys {
		if bytes.Equal(key, k) {
			return es.values[i]
		}
	}
	return nil
}

var ES errorStorage

func Uint64ToString(v uint64) string {
	s := fmt.Sprintf("%d", v)
	logutil.Infof("all_nodes id %d string %v", v, s)
	return s
}
