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

package bsi

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
)

type ValueType uint

const (
	UnsignedInt ValueType = iota
	SignedInt
	Float
	FixedLengthString
)

type BitSlicedIndex interface {
	Clone() BitSlicedIndex
	NotNull(*roaring.Bitmap) *roaring.Bitmap

	Unmarshal([]byte) error
	Marshal() ([]byte, error)

	Del(uint64) error
	Set(uint64, interface{}) error
	Get(uint64) (interface{}, bool)

	Count(*roaring.Bitmap) uint64
	NullCount(*roaring.Bitmap) uint64
	Min(*roaring.Bitmap) (interface{}, uint64)
	Max(*roaring.Bitmap) (interface{}, uint64)
	Sum(*roaring.Bitmap) (interface{}, uint64)

	Eq(interface{}, *roaring.Bitmap) (*roaring.Bitmap, error)
	Ne(interface{}, *roaring.Bitmap) (*roaring.Bitmap, error)
	Lt(interface{}, *roaring.Bitmap) (*roaring.Bitmap, error)
	Le(interface{}, *roaring.Bitmap) (*roaring.Bitmap, error)
	Gt(interface{}, *roaring.Bitmap) (*roaring.Bitmap, error)
	Ge(interface{}, *roaring.Bitmap) (*roaring.Bitmap, error)

	Top(uint64, *roaring.Bitmap) *roaring.Bitmap
	Bottom(uint64, *roaring.Bitmap) *roaring.Bitmap
}

type NumericBSI struct {
	rowCount int
	valType  ValueType
	bitSize  int
	slices   []*roaring.Bitmap
}

type StringBSI struct {
	rowCount  int
	charWidth int
	charSize  int
	slices    []*roaring.Bitmap
}

const (
	bsiExistsBit = 0
	bsiOffsetBit = 1
)

func marshallRB(mp *roaring.Bitmap) ([]byte, error) {
	var buf bytes.Buffer

	if _, err := mp.WriteTo(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
