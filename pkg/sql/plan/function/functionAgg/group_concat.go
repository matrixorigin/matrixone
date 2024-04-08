// Copyright 2021 - 2022 Matrix Origin
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

package functionAgg

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/util"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

// todo: reimplement this agg function after we support multi-column agg ?
// 	but isn't it should be implemented in the function layer?

const (
	defaultSeparator  = ","
	groupConcatMaxLen = 1024
)

var (
	// group_concat() supported input type and output type.
	AggGroupConcatReturnType = func(typs []types.Type) types.Type {
		for _, p := range typs {
			if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
				return types.T_blob.ToType()
			}
		}
		return types.T_text.ToType()
	}
)

func NewAggGroupConcat(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, config any) (agg.Agg[any], error) {
	aggPriv := &sAggGroupConcat{}

	bts, ok := config.([]byte)
	if ok && bts != nil {
		aggPriv.separator = string(bts)
	} else {
		aggPriv.separator = defaultSeparator
	}

	switch inputTypes[0].Oid {
	case types.T_varchar:
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}

	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for group_concat", inputTypes[0])
}

type sAggGroupConcat struct {
	result    []bytes.Buffer
	separator string
}

func (s *sAggGroupConcat) Dup() agg.AggStruct {
	val := &sAggGroupConcat{
		result:    make([]bytes.Buffer, len(s.result)),
		separator: s.separator,
	}
	for i, buf := range s.result {
		val.result[i] = *bytes.NewBuffer(buf.Bytes())
	}
	return val
}
func (s *sAggGroupConcat) Grows(cnt int) {
	oldLen := len(s.result)
	s.result = append(s.result, make([]bytes.Buffer, cnt)...)
	for i := oldLen; i < len(s.result); i++ {
		s.result[i].Grow(groupConcatMaxLen)
	}
}
func (s *sAggGroupConcat) Free(_ *mpool.MPool) {}
func (s *sAggGroupConcat) Fill(groupNumber int64, values []byte, lastResult []byte, count int64, isEmpty bool, isNull bool) ([]byte, bool, error) {
	if isNull || s.result[groupNumber].Len() > groupConcatMaxLen {
		return nil, isEmpty, nil
	}

	tuple, err := types.Unpack(values)
	if err != nil {
		return nil, isEmpty, err
	}
	tupleStr := tupleToString(tuple)
	if !isEmpty {
		s.result[groupNumber].WriteString(s.separator)
	}
	s.result[groupNumber].WriteString(tupleStr)

	return nil, false, nil
}
func (s *sAggGroupConcat) Merge(groupNumber1 int64, groupNumber2 int64, result1 []byte, result2 []byte, isEmpty1 bool, isEmpty2 bool, priv2 any) ([]byte, bool, error) {
	if isEmpty2 || s.result[groupNumber1].Len() > groupConcatMaxLen {
		return nil, isEmpty1 && isEmpty2, nil
	}

	if !isEmpty1 {
		s.result[groupNumber1].WriteString(s.separator)
	}
	s2 := priv2.(*sAggGroupConcat)
	s.result[groupNumber1].Write(s2.result[groupNumber2].Bytes())

	return nil, isEmpty1 && isEmpty2, nil
}
func (s *sAggGroupConcat) Eval(lastResult [][]byte) ([][]byte, error) {
	result := make([][]byte, 0, len(s.result))

	for i := 0; i < len(s.result); i++ {
		result = append(result, s.result[i].Bytes())
	}

	return result, nil
}
func (s *sAggGroupConcat) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer

	// 1. separator length
	separatorBytes := util.UnsafeStringToBytes(s.separator)
	var separatorLen = uint64(len(separatorBytes))
	buf.Write(types.EncodeUint64(&separatorLen)) // 8 bytes

	// 2. separator eg:- "," or "|" etc.
	buf.Write(separatorBytes)

	// 3. result
	strList := make([]string, 0, len(s.result))
	for i := range s.result {
		strList = append(strList, s.result[i].String())
	}
	buf.Write(types.EncodeStringSlice(strList))

	return buf.Bytes(), nil
}
func (s *sAggGroupConcat) UnmarshalBinary(data []byte) error {

	// 1. separator length
	separatorLen := types.DecodeUint64(data[:8])
	data = data[8:]

	// 2. separator
	s.separator = util.UnsafeBytesToString(data[:separatorLen])
	data = data[separatorLen:]

	// 3. result
	strList := types.DecodeStringSlice(data)
	s.result = make([]bytes.Buffer, len(strList))
	for i := range s.result {
		s.result[i].WriteString(strList[i])
	}
	return nil
}

func tupleToString(tp types.Tuple) string {
	res := ""
	for _, t := range tp {
		switch t := t.(type) {
		case bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64:
			res += fmt.Sprintf("%v", t)
		case []byte:
			res += *(*string)(unsafe.Pointer(&t))
		case types.Date:
			res += fmt.Sprintf("%v", t.String())
		case types.Time:
			res += fmt.Sprintf("%v", t.String())
		case types.Datetime:
			res += fmt.Sprintf("%v", t.String())
		case types.Timestamp:
			res += fmt.Sprintf("%v", t.String())
		case types.Decimal64:
			res += fmt.Sprintf("%v", t.Format(0))
		case types.Decimal128:
			res += fmt.Sprintf("%v", t.Format(0))
		default:
			res += fmt.Sprintf("%v", t)
		}
	}
	return res
}
