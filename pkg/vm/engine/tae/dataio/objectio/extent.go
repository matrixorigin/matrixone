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

package objectio

type ExtentType uint8

const (
	APPEND ExtentType = iota
	UPDATE
)

type entry struct {
	offset uint32
	length uint32
}

type Extent struct {
	typ    ExtentType
	offset uint32
	length uint32
	data   entry
}

func (ex *Extent) End() uint32 {
	return ex.offset + ex.length
}

func (ex *Extent) Offset() uint32 {
	return ex.offset
}

func (ex *Extent) Length() uint32 {
	return ex.length
}

func (ex *Extent) GetData() *entry {
	return &ex.data
}

func (en *entry) GetOffset() uint32 {
	return en.offset
}

func (en *entry) GetLength() uint32 {
	return en.length
}
