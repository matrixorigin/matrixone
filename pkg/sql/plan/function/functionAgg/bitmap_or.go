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
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

var (
	AggBitmapConstructSupportedParameters = []types.T{
		types.T_uint64,
	}
	AggBitmapOrSupportedParameters = []types.T{
		types.T_varbinary,
	}
	AggBitmapConstructReturnType = func(_ []types.Type) types.Type {
		return types.T_varbinary.ToType()
	}
	AggBitmapOrReturnType = AggBitmapConstructReturnType
)

func NewAggBitmapConstruct(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	aggPriv := &sAggBitmapConstruct{}
	if dist {
		return agg.NewUnaryDistAgg[uint64, []byte](overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg[uint64, []byte](overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

func NewAggBitmapOr(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, _ any) (agg.Agg[any], error) {
	aggPriv := &sAggBitmapOr{}
	if dist {
		return agg.NewUnaryDistAgg[[]byte, []byte](overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return agg.NewUnaryAgg[[]byte, []byte](overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
}

type sAggBitmapConstruct struct {
	bmps []*roaring.Bitmap
}

func (s *sAggBitmapConstruct) Dup() agg.AggStruct {
	return &sAggBitmapConstruct{}
}
func (s *sAggBitmapConstruct) Grows(count int) {
	for ; count > 0; count-- {
		s.bmps = append(s.bmps, roaring.New())
	}
}
func (s *sAggBitmapConstruct) Free(_ *mpool.MPool) {}
func (s *sAggBitmapConstruct) Fill(groupNumber int64, value uint64, lastResult []byte, count int64, isEmpty bool, isNull bool) ([]byte, bool, error) {
	if !isNull {
		s.bmps[groupNumber].Add(uint32(value))
		return lastResult, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggBitmapConstruct) Merge(groupNumber1, groupNumber2 int64, result1, result2 []byte, isEmpty1, isEmpty2 bool, priv2 any) ([]byte, bool, error) {
	s2 := priv2.(*sAggBitmapConstruct)
	s.bmps[groupNumber1].Or(s2.bmps[groupNumber2])
	return result1, false, nil
}
func (s *sAggBitmapConstruct) Eval(lastResult [][]byte) ([][]byte, error) {
	for groupNumber, bmp := range s.bmps {
		lastResult[groupNumber], _ = bmp.MarshalBinary()
	}
	return lastResult, nil
}
func (s *sAggBitmapConstruct) MarshalBinary() ([]byte, error) {
	return nil, nil
}
func (s *sAggBitmapConstruct) UnmarshalBinary(_ []byte) error {
	return nil
}

type sAggBitmapOr struct {
	bmps []*roaring.Bitmap
}

func (s *sAggBitmapOr) Dup() agg.AggStruct {
	return &sAggBitmapOr{}
}
func (s *sAggBitmapOr) Grows(count int) {
	for ; count > 0; count-- {
		s.bmps = append(s.bmps, roaring.New())
	}
}
func (s *sAggBitmapOr) Free(_ *mpool.MPool) {}
func (s *sAggBitmapOr) Fill(groupNumber int64, value []byte, lastResult []byte, count int64, isEmpty bool, isNull bool) ([]byte, bool, error) {
	if !isNull {
		valueBmp := roaring.New()
		if err := valueBmp.UnmarshalBinary(value); err != nil {
			return lastResult, isEmpty, nil
		}

		s.bmps[groupNumber].Or(valueBmp)
		return lastResult, false, nil
	}
	return lastResult, isEmpty, nil
}
func (s *sAggBitmapOr) Merge(groupNumber1, groupNumber2 int64, result1, result2 []byte, isEmpty1, isEmpty2 bool, priv2 any) ([]byte, bool, error) {
	s2 := priv2.(*sAggBitmapOr)
	s.bmps[groupNumber1].Or(s2.bmps[groupNumber2])
	return result1, false, nil
}
func (s *sAggBitmapOr) Eval(lastResult [][]byte) ([][]byte, error) {
	for groupNumber, bmp := range s.bmps {
		lastResult[groupNumber], _ = bmp.MarshalBinary()
	}
	return lastResult, nil
}
func (s *sAggBitmapOr) MarshalBinary() ([]byte, error) {
	return nil, nil
}
func (s *sAggBitmapOr) UnmarshalBinary(_ []byte) error {
	return nil
}
