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

package blockid

import (
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type Writer struct {
	writer objectio.Writer
	fs     *ObjectFS
	name   string
}

func NewWriter(fs *ObjectFS, id *common.ID) *Writer {
	var name string
	if id.BlockID > 0 {
		name = EncodeBlkName(id)
	} else {
		name = EncodeSegName(id)
	}
	writer, err := objectio.NewObjectWriter(name, fs.service)
	if err != nil {
		panic(any(err))
	}
	return &Writer{
		fs:     fs,
		writer: writer,
		name:   name,
	}
}

func VectorsToMO(vec containers.Vector) *vector.Vector {
	mov := vector.NewOriginal(vec.GetType())
	data := vec.Data()
	typ := vec.GetType()
	mov.Typ = typ
	if vec.HasNull() {
		mov.Nsp.Np = bitmap.New(vec.Length())
		mov.Nsp.Np.AddMany(vec.NullMask().ToArray())
		//mov.Nsp.Np = vec.NullMask()
	}

	if vec.GetType().IsVarlen() {
		bs := vec.Bytes()
		nbs := len(bs.Offset)
		bsv := make([][]byte, nbs)
		for i := 0; i < nbs; i++ {
			bsv[i] = bs.Data[bs.Offset[i] : bs.Offset[i]+bs.Length[i]]
		}
		vector.AppendBytes(mov, bsv, nil)
	} else if vec.GetType().IsTuple() {
		cnt := types.DecodeInt32(data)
		if cnt != 0 {
			if err := types.Decode(data, &mov.Col); err != nil {
				panic(any(err))
			}
		}
	} else {
		vector.AppendFixedRaw(mov, data)
	}

	return mov
}
func CopyToMoVector(vec containers.Vector) *vector.Vector {
	return VectorsToMO(vec)
}
func CopyToMoVectors(vecs []containers.Vector) []*vector.Vector {
	movecs := make([]*vector.Vector, len(vecs))
	for i := range movecs {
		movecs[i] = CopyToMoVector(vecs[i])
	}
	return movecs
}

func MOToVectorTmp(v *vector.Vector, nullable bool) containers.Vector {
	vec := containers.MakeVector(v.Typ, nullable)
	bs := containers.NewBytes()
	switch v.Typ.Oid {
	case types.T_bool:
		if v.Col == nil || len(v.Col.([]bool)) == 0 {
			bs.Data = make([]byte, v.Length())
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]bool), 1)
		}
	case types.T_int8:
		if v.Col == nil || len(v.Col.([]int8)) == 0 {
			bs.Data = make([]byte, v.Length())
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]int8), 1)
		}
	case types.T_int16:
		if v.Col == nil || len(v.Col.([]int16)) == 0 {
			bs.Data = make([]byte, v.Length()*2)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]int16), 2)
		}
	case types.T_int32:
		if v.Col == nil || len(v.Col.([]int32)) == 0 {
			bs.Data = make([]byte, v.Length()*4)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]int32), 4)
		}
	case types.T_int64:
		if v.Col == nil || len(v.Col.([]int64)) == 0 {
			bs.Data = make([]byte, v.Length()*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]int64), 8)
		}
	case types.T_uint8:
		if v.Col == nil || len(v.Col.([]uint8)) == 0 {
			bs.Data = make([]byte, v.Length())
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]uint8), 1)
		}
	case types.T_uint16:
		if v.Col == nil || len(v.Col.([]uint16)) == 0 {
			bs.Data = make([]byte, v.Length()*2)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]uint16), 2)
		}
	case types.T_uint32:
		if v.Col == nil || len(v.Col.([]uint32)) == 0 {
			bs.Data = make([]byte, v.Length()*4)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]uint32), 4)
		}
	case types.T_uint64:
		if v.Col == nil || len(v.Col.([]uint64)) == 0 {
			bs.Data = make([]byte, v.Length()*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]uint64), 8)
		}
	case types.T_float32:
		if v.Col == nil || len(v.Col.([]float32)) == 0 {
			bs.Data = make([]byte, v.Length()*4)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]float32), 4)
		}
	case types.T_float64:
		if v.Col == nil || len(v.Col.([]float64)) == 0 {
			bs.Data = make([]byte, v.Length()*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]float64), 8)
		}
	case types.T_date:
		if v.Col == nil || len(v.Col.([]types.Date)) == 0 {
			bs.Data = make([]byte, v.Length()*4)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Date), 4)
		}
	case types.T_datetime:
		if v.Col == nil || len(v.Col.([]types.Datetime)) == 0 {
			bs.Data = make([]byte, v.Length()*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Datetime), 8)
		}
	case types.T_timestamp:
		if v.Col == nil || len(v.Col.([]types.Timestamp)) == 0 {
			bs.Data = make([]byte, v.Length()*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Timestamp), 8)
		}
	case types.T_decimal64:
		if v.Col == nil || len(v.Col.([]types.Decimal64)) == 0 {
			bs.Data = make([]byte, v.Length()*8)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Decimal64), 8)
		}
	case types.T_decimal128:
		if v.Col == nil || len(v.Col.([]types.Decimal128)) == 0 {
			bs.Data = make([]byte, v.Length()*16)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Decimal128), 16)
		}
	case types.T_uuid:
		if v.Col == nil || len(v.Col.([]types.Uuid)) == 0 {
			bs.Data = make([]byte, v.Length()*16)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Uuid), 16)
		}
	case types.T_TS:
		if v.Col == nil || len(v.Col.([]types.TS)) == 0 {
			bs.Data = make([]byte, v.Length()*types.TxnTsSize)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.TS), types.TxnTsSize)
		}
	case types.T_Rowid:
		if v.Col == nil || len(v.Col.([]types.Rowid)) == 0 {
			bs.Data = make([]byte, v.Length()*types.RowidSize)
			logutil.Warn("[Moengine]", common.OperationField("MOToVector"),
				common.OperandField("Col length is 0"))
		} else {
			bs.Data = types.EncodeFixedSlice(v.Col.([]types.Rowid), types.RowidSize)
		}
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		if v.Col == nil {
			bs.Data = make([]byte, 0)
		} else {
			vbs := vector.GetBytesVectorValues(v)
			for _, v := range vbs {
				bs.Append(v)
			}
		}
	default:
		panic(any(fmt.Errorf("%s not supported", v.Typ.String())))
	}
	if v.Nsp.Np != nil {
		np := &roaring64.Bitmap{}
		np.AddMany(v.Nsp.Np.ToArray())
		logutil.Infof("sie : %d", np.GetCardinality())
		vec.ResetWithData(bs, np)
		return vec
	}
	vec.ResetWithData(bs, nil)
	return vec
}

func (w *Writer) WriteBlock(columns *containers.Batch) (block objectio.BlockObject, err error) {
	bat := batch.New(true, columns.Attrs)
	bat.Vecs = CopyToMoVectors(columns.Vecs)
	block, err = w.writer.Write(bat)
	if err != nil {
		return
	}
	return
}

func (w *Writer) Sync() ([]objectio.BlockObject, error) {
	blocks, err := w.writer.WriteEnd()
	return blocks, err
}

func (w *Writer) WriteIndex(
	block objectio.BlockObject,
	index objectio.IndexData) (err error) {
	return w.writer.WriteIndex(block, index)
}

func (w *Writer) GetName() string {
	return w.name
}

func (w *Writer) GetWriter() objectio.Writer {
	return w.writer
}
