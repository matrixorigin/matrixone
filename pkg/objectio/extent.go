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

type Extent struct {
	id         uint64
	offset     uint32
	length     uint32
	originSize uint32
}

func (ex Extent) Id() uint64 { return ex.id }

func (ex Extent) End() uint32 { return ex.offset + ex.length }

func (ex Extent) Offset() uint32 { return ex.offset }

func (ex Extent) Length() uint32 { return ex.length }

func (ex Extent) OriginSize() uint32 { return ex.originSize }

func (ex Extent) SetOffset(offset uint32) { ex.offset = offset }

func (ex Extent) SetLength(size uint32) { ex.length = size }

func (ex Extent) SetOriginSize(size uint32) { ex.originSize = size }

func (ex Extent) SetId(id uint64) { ex.id = id }
